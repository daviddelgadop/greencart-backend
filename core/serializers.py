import json
from rest_framework import serializers
from datetime import datetime
import pprint
from .models import (
    CustomUser,
    Region,
    Department,
    City, 
    Address, 
    Company,
    Certification,
    ProductCatalog,
    ProductCategory,
    ProductImpact,
    Product, 
    ProductImage,
    ProductBundle,
    ProductBundleItem, 
    PaymentMethod,
    UserSetting,
    UserMeta,
    Order, 
    OrderItem, 
    Favorite,
    RewardTier, 
    UserRewardProgress,
    Reward, 
    CartItem,
    Cart,

    BlogCategory, 
    BlogPost,
    AboutSection, 
    CoreValue, 
    LegalInformation,
    SiteSetting 
)
from datetime import date
from decimal import Decimal
from django.utils import timezone
from django.shortcuts import get_object_or_404
from django.core.exceptions import ObjectDoesNotExist


class RateOnlySerializer(serializers.Serializer):
    rating = serializers.IntegerField(min_value=1, max_value=5, required=True)
    note = serializers.CharField(allow_blank=True, required=False, max_length=2000)


# === Sérialiseurs liés à l'utilisateur ===

class CustomUserSerializer(serializers.ModelSerializer):
    avatar = serializers.ImageField(required=False, allow_null=True)
    main_address = serializers.PrimaryKeyRelatedField(
        queryset=Address.objects.all(),
        required=False,
        allow_null=True
    )

    class Meta:
        model = CustomUser
        fields = [
            'id',
            'first_name',
            'last_name',
            'email',
            'password',
            'type',
            'phone',
            'date_of_birth',
            'avatar',
            'public_display_name',
            'main_address',
            'description_utilisateur',
            'years_of_experience',
            'is_staff',
        ]

        extra_kwargs = {
            'password': {'write_only': True},
            'main_address': {'required': False, 'allow_null': True}
        }

    def get_avatar(self, obj):
        request = self.context.get('request')
        if obj.avatar and hasattr(obj.avatar, 'url'):
            return request.build_absolute_uri(obj.avatar.url) if request else obj.avatar.url
        return None

    def validate_date_of_birth(self, value):
        today = date.today()
        age = today.year - value.year - ((today.month, today.day) < (value.month, value.day))
        if age < 18:
            raise serializers.ValidationError("Vous devez avoir au moins 18 ans pour vous inscrire.")
        return value

    def validate_years_of_experience(self, value):
        user_type = self.initial_data.get('type') or getattr(self.instance, 'type', None)
        if user_type != 'producer' and value > 0:
            raise serializers.ValidationError("Seuls les producteurs peuvent renseigner leurs années d'expérience.")
        return value 
        
    def create(self, validated_data):
        user = CustomUser.objects.create_user(
            email=validated_data['email'],
            first_name=validated_data['first_name'],
            last_name=validated_data['last_name'],
            password=validated_data['password'],
            type=validated_data['type'],
            date_of_birth=validated_data['date_of_birth'],
        )
        user.phone = validated_data.get('phone', '')
        user.public_display_name = validated_data.get('public_display_name', '')
        user.avatar = validated_data.get('avatar')
        user.main_address = validated_data.get('main_address')
        user.description_utilisateur = validated_data.get('description_utilisateur')
        user.years_of_experience = validated_data.get('years_of_experience', 0)
        user.save()
        return user

class UserSerializer(serializers.ModelSerializer):
    avatar = serializers.SerializerMethodField()

    class Meta:
        model = CustomUser
        fields = [
            'id',
            'first_name',
            'last_name',
            'email',
            'type',
            'avatar',
            'public_display_name'
        ]

class ProducerSerializer(serializers.ModelSerializer):
    avatar = serializers.SerializerMethodField()

    class Meta:
        model = CustomUser
        fields = ['id', 'first_name', 'last_name', 'public_display_name', 'description_utilisateur',
                  'years_of_experience', 'avg_rating', 'ratings_count', 'avatar']

    def get_avatar(self, obj):
        if not obj.avatar:
            return None
        url = obj.avatar.url
        request = self.context.get("request")
        return request.build_absolute_uri(url) if request else url
    


# === Sérialiseur pour les adresses ===

class CitySerializer(serializers.ModelSerializer):
    class Meta:
        model = City
        fields = '__all__'


class AddressSerializer(serializers.ModelSerializer):
    city = CitySerializer(read_only=True)  
    city_id = serializers.PrimaryKeyRelatedField(
        queryset=City.objects.all(),
        write_only=True,
        source='city'
    )
    department = serializers.SerializerMethodField()
    region = serializers.SerializerMethodField()

    class Meta:
        model = Address
        fields = '__all__'
        read_only_fields = ['user']

    def validate(self, data):
        if self.instance and self.partial:
            return data

        required_fields = ['title', 
                           'street_number', 
                           'street_name', 
                           #'postal_code', 
                           'city', 
                           ]
        errors = {}
        for field in required_fields:
            if not data.get(field):
                errors[field] = "Ce champ est obligatoire."
        if errors:
            raise serializers.ValidationError(errors)
        return data

    def get_city_name(self, obj):
        return getattr(obj.city, 'ville', '')

    def get_postal_code(self, obj):
        return getattr(obj.city, 'code_postal', '')
    
    def get_department(self, obj):
        try:
            dep = obj.city.department
            return {"code": dep.code, "name": dep.name}
        except Exception:
            return None

    def get_region(self, obj):
        try:
            reg = obj.city.department.region
            return {"code": reg.code, "name": reg.name}
        except Exception:
            return None

# === Sérialiseurs pour Certification ===

class CertificationSerializer(serializers.ModelSerializer):
    company = serializers.PrimaryKeyRelatedField(
        queryset=Company.objects.all(),
        write_only=True
    )
    company_name = serializers.CharField(source='company.name', read_only=True)

    class Meta:
        model = Certification
        fields = [
            'id',
            'company', 
            'company_name', 
            'code',
            'certification_number',
            'valid_until',
            'file',
            'verified',
            'uploaded_at',
            'is_active'
        ]
        read_only_fields = ['verified', 'uploaded_at']



# === Sérialiseurs pour entreprise ===

class CompanySerializer(serializers.ModelSerializer):
    owner = serializers.StringRelatedField(read_only=True)
    address = AddressSerializer(read_only=True)
    address_id = serializers.PrimaryKeyRelatedField(
        queryset=Address.objects.all(), source='address', write_only=True
    )
    certifications = CertificationSerializer(many=True, read_only=True)

    avg_rating = serializers.DecimalField(max_digits=3, decimal_places=2, read_only=True)
    ratings_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = Company
        fields = [
            'id', 'owner', 'name', 'siret_number', 'description', 'logo',
            'address', 'address_id', 'is_active', 'certifications',
            'avg_rating', 'ratings_count', 
        ]
        read_only_fields = ['owner']

    def validate(self, data):
        if self.instance and self.partial:
            return data
        required_fields = ['name', 'siret_number', 'description', 'address']
        errors = {}
        for field in required_fields:
            if not data.get(field):
                errors[field] = "Ce champ est obligatoire."
        if errors:
            raise serializers.ValidationError(errors)
        return data
    


# === Sérialiseurs pour les produits ===

class ProductImpactSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProductImpact
        fields = ['id', 'unit', 'quantity', 'weight_equivalent_kg', 'avoided_waste_kg', 'avoided_co2_kg']


class ProductCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = ProductCategory
        fields = ['id', 'code', 'label']


class ProductCatalogSerializer(serializers.ModelSerializer):
    category = ProductCategorySerializer(read_only=True)
    category_id = serializers.PrimaryKeyRelatedField(
        queryset=ProductCategory.objects.filter(is_active=True),
        source='category',
        write_only=True
    )

    class Meta:
        model = ProductCatalog
        fields = [
            'id',
            'name',
            'category',
            'category_id',
            'eco_score',
            'storage_instructions',
            'created_at',
            'updated_at',
            'is_active'
        ]
        read_only_fields = ['created_at', 'updated_at', 'is_active']


class ProductCatalogNoEcoScoreSerializer(serializers.ModelSerializer):
    category = ProductCategorySerializer(read_only=True)

    class Meta:
        model = ProductCatalog
        exclude = ('eco_score',)

class ProductImageSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProductImage
        fields = ['id', 'image']


class ProductSerializer(serializers.ModelSerializer):
    storage_instructions_display = serializers.SerializerMethodField()
    company_name = serializers.CharField(source="company.name", read_only=True)
    company = serializers.PrimaryKeyRelatedField(
        queryset=Company.objects.all(),
        write_only=True
    )
    company_data = CompanySerializer(source="company", read_only=True)

    images = ProductImageSerializer(many=True, read_only=True)

    certifications = CertificationSerializer(many=True, read_only=True)
    certification_ids = serializers.PrimaryKeyRelatedField(
        many=True,
        queryset=Certification.objects.all(),
        write_only=True,
        source="certifications"
    )

    catalog_entry = serializers.PrimaryKeyRelatedField(
        queryset=ProductCatalog.objects.all(),
        write_only=True,
        required=False,
        allow_null=True
    )
    catalog_entry_data = ProductCatalogNoEcoScoreSerializer(source='catalog_entry', read_only=True)

    avg_rating = serializers.DecimalField(max_digits=3, decimal_places=2, read_only=True)
    ratings_count = serializers.IntegerField(read_only=True)


    def get_storage_instructions_display(self, obj):
        return obj.get_storage_instructions_display()
    

    class Meta:
        model = Product
        fields = [
            'id','title','variety','description',
            'catalog_entry','catalog_entry_data',
            'certifications','certification_ids',
            'original_price','stock','unit',
            'storage_instructions','eco_score','storage_instructions_display',
            'company','company_name','company_data',
            'images','is_active','sold_units',
            'avg_rating','ratings_count'
        ]

    def validate_certification_ids(self, certifs):
        company_id = self.initial_data.get('company') or (self.instance.company_id if self.instance else None)
        if not company_id:
            raise serializers.ValidationError("Impossible de vérifier les certifications sans entreprise associée.")
        for certif in certifs:
            if certif.company_id != int(company_id):
                raise serializers.ValidationError(
                    f"La certification {certif.code} n'appartient pas à l’entreprise spécifiée."
                )
        return certifs

    def validate(self, data):
        user = self.context['request'].user
        company = data.get('company') or (self.instance.company if self.instance else None)

        if company and company.owner != user:
            raise serializers.ValidationError("Cette entreprise ne vous appartient pas.")

        if (stock := data.get('stock')) is not None and stock < 0:
            raise serializers.ValidationError("Le stock doit être positif.")
        if (price := data.get('original_price')) is not None and price < 0:
            raise serializers.ValidationError("Le prix doit être positif.")

        catalog = data.get('catalog_entry')
        if catalog:
            if not data.get('title'):
                data['title'] = catalog.name
            if not data.get('eco_score'):
                data['eco_score'] = catalog.eco_score
            if not data.get('storage_instructions'):
                data['storage_instructions'] = catalog.storage_instructions
        elif not self.partial:
            raise serializers.ValidationError("Le champ 'catalog_entry' est requis.")

        return data

    def create(self, validated_data):
        print("Donneés:", validated_data)
        return super().create(validated_data)


class ProductBundleItemSerializer(serializers.ModelSerializer):
    #product = serializers.PrimaryKeyRelatedField(queryset=Product.objects.all())
    product = ProductSerializer(read_only=True) 
    product_id = serializers.PrimaryKeyRelatedField(
        queryset=Product.objects.all(),
        write_only=True,
        source='product'
    )

    class Meta:
        model = ProductBundleItem
        fields = ['product', 'product_id', 'quantity', 'best_before_date',
            'avoided_waste_kg', 'avoided_co2_kg' ]


class ProductBundleSerializer(serializers.ModelSerializer):
    items = ProductBundleItemSerializer(many=True)
    company_id = serializers.SerializerMethodField(method_name='get_company_id')
    producer_data = serializers.SerializerMethodField()
    region_data = serializers.SerializerMethodField()
    department_data = serializers.SerializerMethodField()

    total_avoided_waste_kg = serializers.DecimalField(max_digits=7, decimal_places=2, read_only=True)
    total_avoided_co2_kg = serializers.DecimalField(max_digits=7, decimal_places=2, read_only=True)
    avg_rating = serializers.DecimalField(max_digits=3, decimal_places=2, read_only=True)
    ratings_count = serializers.IntegerField(read_only=True)
    
    evaluations = serializers.SerializerMethodField()

    class Meta:
        model = ProductBundle
        fields = [
            'id', 'title', 'items', 'stock', 'discounted_percentage', 'original_price',
            'discounted_price', 'status', 'is_active', 'created_at', 'company_id',
            'producer_data', 'region_data', 'department_data',
            'total_avoided_waste_kg', 'total_avoided_co2_kg', 'sold_bundles',
            'avg_rating', 'ratings_count',
            "evaluations", 
        ]
        read_only_fields = ['id', 'original_price', 'discounted_price', 
                            'sold_bundles', 'avg_rating', 'ratings_count', 'evaluations']

    def get_region_data(self, obj):
        try:
            first_item = obj.items.first()
            region = first_item.product.company.address.city.department.region
            return {
                'code': region.code,
                'name': region.name
            }
        except AttributeError:
            return None

    def get_department_data(self, obj):
        try:
            first_item = obj.items.first()
            department = first_item.product.company.address.city.department
            return {
                'code': department.code,
                'name': department.name
            }
        except AttributeError:
            return None

    def get_producer_data(self, obj):
        first_item = obj.items.first()
        if not first_item:
            return None
        company = first_item.product.company
        producer = company.owner if company else None
        if not producer:
            return None
        return ProducerSerializer(producer, context=self.context).data

    
    def calculate_prices(self, items_data, discounted_percentage):
        product_ids = [item['product'] if isinstance(item['product'], int) else item['product'].id for item in items_data]
        print("IDs de productos recibidos:", product_ids)
        
        products_map = {p.id: p for p in Product.objects.filter(id__in=product_ids)}
        print("Productos recuperados de DB:")
        for pid, prod in products_map.items():
            print(f" - ID {pid}: {prod.title}, precio: {prod.original_price}")

        total_original_price = Decimal('0.0')
        for item in items_data:
            product_id = item['product'] if isinstance(item['product'], int) else item['product'].id
            product = products_map[product_id]
            quantity = item['quantity']
            subtotal = Decimal(product.original_price) * quantity
            total_original_price += Decimal(product.original_price) * quantity
            print(f"> Producto: {product.title} (x{quantity}) = {subtotal} €")

        discount = Decimal(discounted_percentage) / Decimal(100)
        discounted_price = total_original_price * (Decimal('1.0') - discount)
        return total_original_price, discounted_price

    def get_company_id(self, obj):
        first_item = obj.items.first()
        if first_item and first_item.product and first_item.product.company:
            return first_item.product.company.id
        return None
    
    def validate_items(self, items):
        if not items:
            raise serializers.ValidationError("Le lot doit contenir au moins un produit.")

        companies = {item['product'].company_id for item in items}
        if len(companies) > 1:
            raise serializers.ValidationError("Tous les produits doivent appartenir à la même entreprise.")

        return items

    def validate_discounted_percentage(self, value):
        if value is not None and (value < 0 or value > 100):
            raise serializers.ValidationError("Le pourcentage de réduction doit être entre 0 et 100.")
        return value

    def validate(self, data):
        request = self.context.get('request')

        if isinstance(request.data.get('items'), str):
            try:
                raw_items = json.loads(request.data.get('items'))
                parsed_items = []
                for item in raw_items:
                    product = get_object_or_404(Product, id=item['product_id'])
                    quantity = item['quantity']
                    best_before = item.get('best_before_date')
                    if best_before:
                        try:
                            best_before = datetime.strptime(best_before, '%Y-%m-%d').date()
                        except ValueError:
                            best_before = None
                    parsed_items.append({
                        'product': product,
                        'quantity': quantity,
                        'best_before_date': best_before
                    })
                data['items'] = parsed_items
            except Exception as e:
                raise serializers.ValidationError({'items': f"Format invalide: {str(e)}"})

        items = data.get('items', [])
        stock = data.get('stock', 0)

        for item in items:
            product = item['product']
            required = stock * item['quantity']
            if product.stock < required:
                raise serializers.ValidationError(
                    f"Stock insuffisant pour le produit {product.title} : requis {required}, disponible {product.stock}"
                )

        return data


    def create(self, validated_data):

        print("Data (create):")
        pprint.pprint(validated_data)

        items_data = validated_data.pop('items')
        print(" validated_data :", validated_data)
        validated_data.pop('id', None)
        discounted_percentage = validated_data.get('discounted_percentage', 0)

        total_original_price, discounted_price = self.calculate_prices(items_data, discounted_percentage)
        validated_data['original_price'] = total_original_price
        validated_data['discounted_price'] = discounted_price

        bundle = ProductBundle.objects.create(**validated_data)

        for item_data in items_data:
            best_before_value = item_data.get("best_before_date")
            if isinstance(best_before_value, str):
                try:
                    item_data["best_before_date"] = datetime.strptime(best_before_value, "%Y-%m-%d").date()
                except ValueError:
                    item_data["best_before_date"] = None
            elif isinstance(best_before_value, datetime):
                item_data["best_before_date"] = best_before_value.date()
            elif isinstance(best_before_value, date):
                item_data["best_before_date"] = best_before_value
            else:
                item_data["best_before_date"] = None
            ProductBundleItem.objects.create(bundle=bundle, **item_data)

        bundle.calculate_bundle_impact()

        return bundle

    def update(self, instance, validated_data):

        print("📥 Data (update):")
        pprint.pprint(validated_data)

        items_data = validated_data.pop('items', None)
        discounted_percentage = validated_data.get('discounted_percentage', instance.discounted_percentage)

        if items_data is not None:
            instance.items.all().delete()
            for item_data in items_data:
                best_before_value = item_data.get("best_before_date")
                if isinstance(best_before_value, str):
                    try:
                        item_data["best_before_date"] = datetime.strptime(best_before_value, "%Y-%m-%d").date()
                    except ValueError:
                        item_data["best_before_date"] = None
                elif isinstance(best_before_value, datetime):
                    item_data["best_before_date"] = best_before_value.date()
                elif isinstance(best_before_value, date):
                    item_data["best_before_date"] = best_before_value
                else:
                    item_data["best_before_date"] = None
                ProductBundleItem.objects.create(bundle=instance, **item_data)

            total_original_price, discounted_price = self.calculate_prices(items_data, discounted_percentage)
            instance.original_price = total_original_price
            instance.discounted_price = discounted_price

        for attr, value in validated_data.items():
            setattr(instance, attr, value)

        instance.calculate_bundle_impact()
        instance.save()
        return instance

    def get_evaluations(self, obj):
        """
        1000 evaluations
        """
        qs = (
            OrderItem.objects
            .filter(bundle=obj, customer_rating__isnull=False)
            .select_related("order__user", "bundle") 
            .order_by("-rated_at")
        )[:1000]

        out = []
        for oi in qs:
            order = oi.order
            user = getattr(order, "user", None)

            display = None
            if user:
                display = getattr(user, "public_display_name", None)
                if not display:
                    first = (user.first_name or "").strip()
                    last_i = ((user.last_name or "").strip()[:1] + ".") if (user.last_name or "").strip() else ""
                    display = (first + " " + last_i).strip() or "Client"

            out.append({
                "order_id": order.id,
                "order_code": getattr(order, "order_code", None),
                "ordered_at": getattr(order, "created_at", None),
                "user_display_name": display,
                "rating": oi.customer_rating,
                "note": oi.customer_note or "",
                "rated_at": oi.rated_at,
                "quantity": oi.quantity,
                "order_status": getattr(order, "status", None),
                "line_total": str(oi.total_price) if getattr(oi, "total_price", None) is not None else None,
                "bundle_id": oi.bundle_id if hasattr(oi, "bundle_id") else obj.id,
                "bundle_title": getattr(oi.bundle, "title", None) or obj.title,
            })
        return out


# === Sérialiseur pour les méthodes de paiement ===

# serializers.py

class PaymentMethodSerializer(serializers.ModelSerializer):
    digits = serializers.CharField(write_only=True, required=True, allow_blank=True, allow_null=True)
    last4 = serializers.SerializerMethodField(read_only=True)

    class Meta:
        model = PaymentMethod
        fields = '__all__'  
        read_only_fields = ['user']

    def get_last4(self, obj):
        if obj.digits:
            return obj.digits[-4:]
        return None

    def _normalize_store_last4(self, validated_data):
        raw = validated_data.pop('digits', None)
        if raw:
            only_digits = ''.join(ch for ch in str(raw) if ch.isdigit())
            validated_data['digits'] = only_digits[-4:] if only_digits else None
        return validated_data

    def create(self, validated_data):
        validated_data = self._normalize_store_last4(validated_data)
        return super().create(validated_data)

    def update(self, instance, validated_data):
        validated_data = self._normalize_store_last4(validated_data)
        return super().update(instance, validated_data)


# === Sérialiseur pour les paramètres utilisateur ===

class UserSettingSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserSetting
        fields = [
            'notif_promotions',
            'notif_new_products',
            'notif_orders',
            'account_deletion_requested',
            'download_data_requested'
        ]
        read_only_fields = ['user']

class UserMetaSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserMeta
        fields = [
            'signup_ip',
            'last_login_ip',
            'browser_info',
            'device_info',
            'locale',
            'created_at',
            'updated_at',
        ]


# === Sérialiseurs pour les commandes et leurs items ===

class OrderItemSerializer(serializers.ModelSerializer):
    bundle = ProductBundleSerializer(read_only=True)

    class Meta:
        model = OrderItem
        fields = [
            'id',
            'bundle',
            'quantity',
            'total_price',
            'order_item_savings',
            'order_item_total_avoided_waste_kg',
            'order_item_total_avoided_co2_kg',
            'bundle_snapshot',
            'customer_rating',
            'customer_note',
            'rated_at',
        ]
        read_only_fields = [
            'customer_rating',
            'customer_note',
            'rated_at',
        ]

    
class OrderSerializer(serializers.ModelSerializer):
    items = OrderItemSerializer(many=True, read_only=True)
    shipping_address = AddressSerializer(read_only=True)
    billing_address = AddressSerializer(read_only=True)

    class Meta:
        model = Order
        fields = [
            'id',
            'order_code',
            'status',
            'total_price',
            'subtotal',
            'shipping_cost',
            'order_total_savings',
            'order_total_avoided_waste_kg',
            'order_total_avoided_co2_kg',
            'created_at',
            'shipping_address',
            'billing_address',
            'payment_method',
            'shipping_address_snapshot',
            'billing_address_snapshot',
            'payment_method_snapshot',
            'customer_rating',
            'customer_note',
            'rated_at',
            'items',
        ]
        read_only_fields = [
            'order_code',
            'shipping_address_snapshot',
            'billing_address_snapshot',
            'payment_method_snapshot',
            'customer_rating',
            'customer_note',
            'rated_at',
            'items',
        ]



# === Sérialiseur pour les favoris ===

class FavoriteSerializer(serializers.ModelSerializer):
    bundle = ProductBundleSerializer(read_only=True)
    bundle_id = serializers.PrimaryKeyRelatedField(
        queryset=ProductBundle.objects.all(),
        write_only=True,
        source='bundle'
    )

    class Meta:
        model = Favorite
        fields = ['id', 'bundle', 'bundle_id', 'added_at']

    def validate(self, attrs):
        user = self.context['request'].user
        bundle = attrs.get('bundle') or getattr(self.instance, 'bundle', None)
        if Favorite.objects.filter(user=user, bundle=bundle, is_active=True).exists():
            raise serializers.ValidationError("Ce lot est déjà dans vos favoris.")
        return attrs


class CartItemSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)

    bundle = serializers.IntegerField(source='bundle_id', read_only=True)
    title_snapshot = serializers.CharField(read_only=True)
    price_snapshot = serializers.DecimalField(max_digits=10, decimal_places=2, read_only=True)

    class Meta:
        model = CartItem
        fields = [
            'id',                
            'bundle',
            'quantity',
            'price_snapshot',
            'title_snapshot',
            'company_id_snapshot',
            'company_name_snapshot',
            'dluo_snapshot',
            'bundle_image',
            'avoided_waste_kg',
            'avoided_co2_kg',
        ]
      


class CartSerializer(serializers.ModelSerializer):
    items = serializers.SerializerMethodField()

    class Meta:
        model = Cart
        fields = ['id', 'items']

    def get_items(self, obj):
        qs = obj.items.order_by('-created_at')
        return CartItemSerializer(qs, many=True, context=self.context).data
    


# === Sérialiseurs pour l'impact écologique et les récompenses ===


class RewardTierSerializer(serializers.ModelSerializer):
    class Meta:
        model = RewardTier
        fields = ('code','title','description','icon',
                  'min_orders','min_waste_kg','min_co2_kg',
                  'min_producers_supported','min_savings_eur',
                  'benefit_kind','benefit_config','is_active')

class RewardSerializer(serializers.ModelSerializer):
    tier = RewardTierSerializer()
    class Meta:
        model = Reward
        fields = ('title','description','earned_on','tier',
                  'benefit_status','benefit_payload','fulfilled_at')


class UserRewardProgressSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserRewardProgress
        fields = '__all__'


# === Sérialiseurs pour les profil publiques de producteur ===
class CityPublicSerializer(serializers.ModelSerializer):
    department_data = serializers.SerializerMethodField()
    region_data = serializers.SerializerMethodField()

    class Meta:
        model = City
        # OJO: no incluimos "department" (el FK numérico) para no contaminar la respuesta
        fields = (
            'id', 'postal_code', 'name', 'commune_code',
            'latitude', 'longitude', 'country_name',
            'department_data', 'region_data',
        )

    def get_department_data(self, obj):
        d = getattr(obj, 'department', None)
        if not d:
            return None
        return {'code': d.code, 'name': d.name}

    def get_region_data(self, obj):
        d = getattr(obj, 'department', None)
        r = getattr(d, 'region', None) if d else None
        if not r:
            return None
        return {'code': r.code, 'name': r.name}


class AddressPublicSerializer(serializers.ModelSerializer):
    city = CityPublicSerializer(read_only=True)

    class Meta:
        model = Address
        fields = (
            'id', 'title', 'street_number', 'street_name', 'complement',
            'is_primary', 'is_active', 'created_at', 'updated_at',
            'city',
        )


# --- en serializers.py ---



class RegionMiniSerializer(serializers.ModelSerializer):
    class Meta:
        model = Region
        fields = ("code", "name")


class DepartmentMiniSerializer(serializers.ModelSerializer):
    region = RegionMiniSerializer(read_only=True)

    class Meta:
        model = Department
        fields = ("code", "name", "region")


class PublicCitySerializer(serializers.ModelSerializer):
    department = DepartmentMiniSerializer(read_only=True)

    class Meta:
        model = City
        fields = (
            "id",
            "postal_code",
            "name",
            "commune_code",
            "latitude",
            "longitude",
            "country_name",
            "department",
        )


class PublicAddressSerializer(serializers.ModelSerializer):
    city = PublicCitySerializer(read_only=True)

    class Meta:
        model = Address
        fields = (
            "id",
            "title",
            "street_number",
            "street_name",
            "complement",
            "city",
            "is_primary",
            "is_active",
            "created_at",
            "updated_at",
        )
        read_only_fields = fields


class PublicCompanySerializer(serializers.ModelSerializer):
    address = PublicAddressSerializer(read_only=True)
    certifications = serializers.SlugRelatedField(slug_field="code", many=True, read_only=True)
    region_data = serializers.SerializerMethodField()
    department_data = serializers.SerializerMethodField()

    avg_rating = serializers.DecimalField(max_digits=3, decimal_places=2, read_only=True)
    ratings_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = Company
        fields = [
            "id",
            "name",
            "logo",
            "siret_number",
            "description",
            "address",
            "certifications",
            "region_data",
            "department_data",
            "avg_rating",
            "ratings_count",
        ]

    def get_region_data(self, obj):
        try:
            r = obj.address.city.department.region
            return {"code": r.code, "name": r.name}
        except AttributeError:
            return None

    def get_department_data(self, obj):
        try:
            d = obj.address.city.department
            return {"code": d.code, "name": d.name}
        except AttributeError:
            return None


class PublicProducerSerializer(serializers.ModelSerializer):
    commerces = serializers.SerializerMethodField()
    joined_at = serializers.SerializerMethodField()

    main_address = serializers.SerializerMethodField()
    main_region_data = serializers.SerializerMethodField()
    main_department_data = serializers.SerializerMethodField()

    avg_rating = serializers.DecimalField(max_digits=3, decimal_places=2, read_only=True)
    ratings_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = CustomUser
        fields = [
            "id","public_display_name","first_name","last_name","avatar",
            "description_utilisateur","years_of_experience","joined_at",
            "main_address","main_region_data","main_department_data",
            "commerces",
            "avg_rating","ratings_count"
        ]


    def get_joined_at(self, obj):
        dt = getattr(obj, "created_at", None) or getattr(obj, "date_joined", None)
        return dt.isoformat() if dt else None

    def get_commerces(self, obj):
        qs = Company.objects.filter(owner=obj).distinct()
        return PublicCompanySerializer(qs, many=True, context=self.context).data

    def _get_primary_address(self, obj):
        addr = getattr(obj, "main_address", None)
        if addr:
            return addr
        return (
            Address.objects
            .filter(user=obj, is_primary=True, is_active=True)
            .select_related("city__department__region")
            .first()
        )

    def get_main_address(self, obj):
        addr = self._get_primary_address(obj)
        return AddressSerializer(addr, context=self.context).data if addr else None

    def get_main_region_data(self, obj):
        addr = self._get_primary_address(obj)
        try:
            region = addr.city.department.region
            return {"code": region.code, "name": region.name}
        except AttributeError:
            return None

    def get_main_department_data(self, obj):
        addr = self._get_primary_address(obj)
        try:
            dept = addr.city.department
            return {"code": dept.code, "name": dept.name}
        except AttributeError:
            return None



# === Sérialiseurs pour le blog ===


class BlogCategorySerializer(serializers.ModelSerializer):
    class Meta:
        model = BlogCategory
        fields = ['id', 'name', 'slug', 'description', 'color', 'icon', 'order', 'is_active']


class BlogPostReadSerializer(serializers.ModelSerializer):
    category = BlogCategorySerializer(read_only=True)
    author_name = serializers.CharField(source='author.public_display_name', read_only=True)

    class Meta:
        model = BlogPost
        fields = [
            'id','title','slug','excerpt','content',
            'image','image_alt',
            'status','is_active','pinned',
            'published_at','created_at','updated_at',
            'read_time_min','author_name','category'
        ]



TRUTHY = {"true","1","on","yes","oui"}
FALSY  = {"false","0","off","no","non",""}

class FlexibleBooleanField(serializers.Field):
    def to_internal_value(self, data):
        if isinstance(data, bool):
            return data
        if data is None:
            return None
        s = str(data).strip().lower()
        if s in TRUTHY: return True
        if s in FALSY:  return False
        raise serializers.ValidationError("must be a valid boolean")
    def to_representation(self, value):
        return bool(value)

class BlogPostWriteSerializer(serializers.ModelSerializer):
    category_id = serializers.PrimaryKeyRelatedField(
        queryset=BlogCategory.objects.filter(is_active=True),
        source='category',
        write_only=True,
        required=True,
        error_messages={'required': "La catégorie est obligatoire."}
    )
    pinned = FlexibleBooleanField(required=False)
    is_active = FlexibleBooleanField(required=False)

    class Meta:
        model = BlogPost
        fields = [
            'title','slug','excerpt','content',
            'image','image_alt',
            'status','pinned','published_at',
            'read_time_min','category_id','is_active'
        ]

    def validate(self, data):
        errors = {}
        creating = self.instance is None
        if creating:
            for f in ('title','slug','excerpt','content','status','category'):
                if not data.get(f):
                    errors[f] = ["Ce champ est obligatoire."]
            if not data.get('image'):
                errors['image'] = ["Image requise pour la création."]
        if errors:
            raise serializers.ValidationError(errors)

        valid = getattr(BlogPost,'STATUS', (('draft',''),('scheduled',''),('published',''),('archived','')))
        keys = {k for k,_ in valid}
        st = data.get('status') or (self.instance.status if self.instance else 'draft')
        if st not in keys:
            raise serializers.ValidationError({'status':[f"Valeur invalide. Utilisez: {', '.join(sorted(keys))}"]})

        if st == 'published' and not (data.get('published_at') or (self.instance and self.instance.published_at)):
            data.setdefault('published_at', timezone.now())

        if 'content' in data:
            words = len((data.get('content') or '').split())
            data['read_time_min'] = max(1, (words + 199) // 200)

        if creating and 'is_active' not in data:
            data['is_active'] = True
        if 'pinned' not in data:
            data['pinned'] = False

        return data

    def create(self, validated_data):
        return super().create(validated_data)

    def update(self, instance, validated_data):
        if 'is_active' not in validated_data:
            validated_data['is_active'] = instance.is_active
        return super().update(instance, validated_data)


class BlogCategoryPublicSerializer(serializers.ModelSerializer):
    class Meta:
        model = BlogCategory
        fields = ["id", "name", "slug", "order", "color"]


class BlogPostPublicSerializer(serializers.ModelSerializer):
    category = BlogCategoryPublicSerializer(read_only=True)

    class Meta:
        model = BlogPost
        fields = [
            "id",
            "title",
            "slug",
            "excerpt",
            "content",
            "author_name",
            "published_at",
            "image",
            "image_alt",
            "read_time_min",
            "category",
        ]


# === Autres ===

class AboutSectionSerializer(serializers.ModelSerializer):
    class Meta:
        model = AboutSection
        fields = '__all__'

class CoreValueSerializer(serializers.ModelSerializer):
    class Meta:
        model = CoreValue
        fields = '__all__'


class LegalInformationSerializer(serializers.ModelSerializer):
    class Meta:
        model = LegalInformation
        fields = '__all__'

class SiteSettingSerializer(serializers.ModelSerializer):
    class Meta:
        model = SiteSetting
        fields = ['id', 'key', 'value', 'description', 'last_updated']



