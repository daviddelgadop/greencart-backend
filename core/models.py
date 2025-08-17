from decimal import Decimal
from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin
from django.core.validators import RegexValidator
from django.core.exceptions import ValidationError
from django.db.models import Q
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
import os
import uuid


class ActiveManager(models.Manager):
    def get_queryset(self):
        return super().get_queryset().filter(is_active=True)
    
def validate_file_extension(value):
    valid_extensions = ['.pdf', '.jpg', '.jpeg', '.png']
    ext = os.path.splitext(value.name)[1].lower()
    if ext not in valid_extensions:
        raise ValidationError("Seuls les fichiers PDF ou images (.jpg, .jpeg, .png) sont autorisés.")
    

# === Modèles abstraits communs ===

class BaseModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    deactivated_at = models.DateTimeField(null=True, blank=True) 
    is_active = models.BooleanField(default=True)

    class Meta:
        abstract = True


# === Gestion des utilisateurs personnalisés ===

phone_regex = RegexValidator(
    regex=r'^0\d{9}$',
    message="Le numéro de téléphone doit comporter 10 chiffres et commencer par 0 (ex: 0612345678)."
)

class CustomUserManager(BaseUserManager):
    def create_user(self, email, password=None, **extra_fields):
        if not email:
            raise ValueError("L'adresse email est requise.")
        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, email, password=None, **extra_fields):
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        return self.create_user(email, password, **extra_fields)

class CustomUser(AbstractBaseUser, PermissionsMixin):
    email = models.EmailField(unique=True)
    first_name = models.CharField(max_length=150, default="Prénom")
    last_name = models.CharField(max_length=150, default="Nom")
    date_of_birth = models.DateField(null=True, blank=True)
    phone = models.CharField(validators=[phone_regex], max_length=10, blank=True)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    avatar = models.ImageField(upload_to='avatars/', blank=True, null=True)
    public_display_name = models.CharField(max_length=255, blank=True, null=True)
    main_address = models.ForeignKey('Address', on_delete=models.SET_NULL, null=True, blank=True, related_name='main_users')
    #dossier = models.ManyToManyField('Document', blank=True)
    type = models.CharField(max_length=20, choices=[('customer', 'Customer'), ('producer', 'Producer')])
    description_utilisateur = models.TextField(blank=True, null=True)
    years_of_experience = models.PositiveIntegerField(default=0, verbose_name="Années d'expérience du producteur")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    deletion_requested = models.BooleanField(default=False)
    deletion_requested_at = models.DateTimeField(null=True, blank=True)

    avg_rating = models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    ratings_count = models.PositiveIntegerField(default=0)

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['type', 'first_name', 'last_name']
    EMAIL_FIELD = 'email'

    objects = CustomUserManager()

    def __str__(self):
        return f"{self.first_name} {self.last_name} <{self.email}>"


# === Adresse utilisateur ===

class Region(models.Model):
    code = models.CharField(max_length=5, unique=True)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class Department(models.Model): 
    code = models.CharField(max_length=5, unique=True)
    name = models.CharField(max_length=100) 
    region = models.ForeignKey(Region, on_delete=models.CASCADE, related_name='departments')

    def __str__(self):
        return self.name


class City(models.Model):
    postal_code = models.CharField(max_length=10)
    name = models.CharField(max_length=100) 
    commune_code = models.CharField(max_length=10, unique=True)
    department = models.ForeignKey(Department, on_delete=models.CASCADE, related_name='cities')
    latitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    country_name = models.CharField(max_length=100, default="FRANCE") 

    def __str__(self):
        return f"{self.name} ({self.postal_code})"

    
class Address(BaseModel):
    user = models.ForeignKey(CustomUser, on_delete=models.CASCADE, related_name='addresses')
    title = models.CharField(max_length=100)
    street_number = models.CharField(max_length=10)
    street_name = models.CharField(max_length=255)
    complement = models.CharField(max_length=255, blank=True, null=True)
    city = models.ForeignKey(City, on_delete=models.SET_NULL, null=True, blank=True)
    is_primary = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)

    def save(self, *args, **kwargs):
        if self.is_primary:
            Address.objects.filter(user=self.user, is_primary=True).exclude(pk=self.pk).update(is_primary=False)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.title} - {self.city}"
    


# === Entreprise producteur ===

class Company(BaseModel):

    owner = models.ForeignKey(CustomUser, on_delete=models.CASCADE, limit_choices_to={'type': 'producer'}, related_name='companies')
    name = models.CharField(max_length=255)
    siret_number = models.CharField(max_length=14)
    address = models.ForeignKey(Address, on_delete=models.PROTECT)  
    description = models.TextField()
    logo = models.ImageField(upload_to='companies/', null=True, blank=True)
    avg_rating = models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    ratings_count = models.PositiveIntegerField(default=0)

    def __str__(self):
        return f"{self.name} ({self.owner.email})"


# === Certifications ===

class Certification(BaseModel):
    CERTIFICATION_CHOICES = [
        ("AB", "Agriculture Biologique"),
        ("Demeter", "Demeter"),
        ("Label Rouge", "Label Rouge"),
        ("HVE", "Haute Valeur Environnementale"),
        ("IGP", "Indication géographique protégée"),
        ("AOP", "Appellation d'origine protégée"),
        ("Sans OGM", "Sans OGM"),
        ("Commerce Équitable", "Commerce équitable"),
    ]

    company = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        related_name='certifications'
    )
    code = models.CharField(max_length=50, choices=CERTIFICATION_CHOICES)
    certification_number = models.CharField(max_length=100)
    valid_until = models.DateField(blank=True, null=True)
    file = models.FileField(upload_to='certifications/')
    verified = models.BooleanField(default=False)
    uploaded_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('company', 'code', 'certification_number') 

    def __str__(self):
        return f"{self.code} - {self.company.name} - {self.certification_number}"

    @property
    def is_expired(self):
        return self.valid_until and self.valid_until < timezone.now().date()


 
# === Produits ===
UNIT_CHOICES = [
    ("pièce", "Pièce"),
    ("kg", "Kilogramme"),
    ("g", "Gramme"),
    ("l", "Litre"),
    ("cl", "Centilitre"),
    ("boîte", "Boîte"),
    ("botte", "Botte"), 
    ("tête", "Tête"),
    ("gobelet", "Gobelet"),
    ("bouteille 75 cl", "Bouteille 75 cl"),
    ("bouteille 50 cl", "Bouteille 50 cl"),
    ("bouteille 25 cl", "Bouteille 25 cl"),
    ("bouteille 1 l", "Bouteille 1 L"),
]


ECO_SCORE_CHOICES = [
    ("A", "A (Très faible impact)"),
    ("B", "B (Faible impact)"),
    ("C", "C (Impact modéré)"),
    ("D", "D (Impact élevé)"),
    ("E", "E (Très fort impact)"),
]


STORAGE_CHOICES = [
    ("surgelé", "Surgelé (-18°C)"),
    ("frais", "Frais (0-4°C)"),
    ("réfrigéré", "Réfrigéré (4-8°C)"),
    ("temp_ambiante", "Température ambiante"),
    ("sec", "Sec"),
    ("cave", "Stockage en cave"),
    ("other", "Selon le produit'"),
]


class ProductCategory(BaseModel):
    code = models.SlugField(max_length=50, unique=True)
    label = models.CharField(max_length=100)

    def __str__(self):
        return self.label


class ProductCatalog(BaseModel):
    name = models.CharField(max_length=150)
    category = models.ForeignKey(ProductCategory, on_delete=models.CASCADE, related_name="catalog_products")
    eco_score = models.CharField(max_length=1, choices=ECO_SCORE_CHOICES, default="A")
    storage_instructions = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.name
    
    
class Product(BaseModel):
    company = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        related_name="products"
    )
    title = models.CharField(max_length=255)
    variety = models.CharField(max_length=255, blank=True)
    description = models.TextField(blank=True)
    catalog_entry = models.ForeignKey(
        ProductCatalog,
        on_delete=models.PROTECT,
        related_name="products"
    )
    certifications = models.ManyToManyField("Certification", related_name="certified_products", blank=True)
    original_price = models.DecimalField(max_digits=10, decimal_places=2)
    stock = models.PositiveIntegerField()
    unit = models.CharField(max_length=20, choices=UNIT_CHOICES)
    storage_instructions = models.CharField(max_length=50, choices=STORAGE_CHOICES, blank=True)
    eco_score = models.CharField(max_length=2, choices=ECO_SCORE_CHOICES, blank=True, null=True)
    sold_units = models.PositiveIntegerField(default=0)
    avg_rating = models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    ratings_count = models.PositiveIntegerField(default=0)

    def __str__(self):
        return self.title
    

class ProductImpact(models.Model):
    product = models.ForeignKey("ProductCatalog", on_delete=models.CASCADE, related_name="impacts")
    unit = models.CharField(max_length=20, choices=UNIT_CHOICES)
    quantity = models.DecimalField(max_digits=7, decimal_places=3, help_text="Nombre d’unités ou poids en kg/l")
    weight_equivalent_kg = models.DecimalField(max_digits=7, decimal_places=3, help_text="Poids réel en kg")
    avoided_waste_kg = models.DecimalField(max_digits=7, decimal_places=3, help_text="Kg de gaspillage évité")
    avoided_co2_kg = models.DecimalField(max_digits=7, decimal_places=3, help_text="Kg de CO₂ évité")

    class Meta:
        unique_together = ('product', 'unit', 'quantity')

    def __str__(self):
        return f"{self.product.name} - {self.quantity} {self.unit}"
    

class ProductImage(models.Model):
    product = models.ForeignKey(
        'Product',
        on_delete=models.CASCADE,
        related_name='images'
    )
    image = models.ImageField(upload_to='product_images/')
    alt_text = models.CharField(max_length=255, blank=True)

    def __str__(self):
        return f"Image for {self.product.title}"


class ProductBundleImage(models.Model):
    bundle = models.ForeignKey(
        'ProductBundle',
        on_delete=models.CASCADE,
        related_name='images'
    )
    image = models.ImageField(upload_to='bundle_images/')
    alt_text = models.CharField(max_length=255, blank=True)

    def __str__(self):
        return f"Image for {self.bundle.title}"


class ProductBundle(BaseModel):
    STATUS_CHOICES = [
        ('draft', 'Brouillon'),
        ('published', 'Publié'),
        ('archived', 'Archivé'),
        ('out_of_stock', 'Épuisé'),
    ]

    title = models.CharField(max_length=255)
    products = models.ManyToManyField("Product", through="ProductBundleItem")
    stock = models.PositiveIntegerField(default=1)
    discounted_percentage = models.PositiveIntegerField(default=0)
    discounted_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    original_price = models.DecimalField(max_digits=10, decimal_places=2)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft')
    total_avoided_waste_kg = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    total_avoided_co2_kg = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    sold_bundles = models.PositiveIntegerField(default=0)
    avg_rating = models.DecimalField(max_digits=3, decimal_places=2, null=True, blank=True)
    ratings_count = models.PositiveIntegerField(default=0)

    def clean(self):
        errors = {}

        items = self.items.all()
        if not items.exists():
            return 

        producers = {item.product.company_id for item in items}
        if len(producers) > 1:
            errors["products"] = _("Tous les produits doivent appartenir à la même entreprise.")

        for item in items:
            total_needed = item.quantity * self.stock
            if item.product.stock < total_needed:
                errors[f"stock_{item.product.title}"] = _(
                    f"Stock insuffisant pour {item.product.title}. Requis: {total_needed}, disponible: {item.product.stock}"
                )

        if self.discounted_percentage < 0 or self.discounted_percentage > 100:
            errors["discounted_percentage"] = _("Le pourcentage de réduction doit être entre 0 et 100.")

        if errors:
            raise ValidationError(errors)

    def calculate_original_price(self):
        return sum(item.product.original_price * item.quantity for item in self.items.all())

    def save(self, *args, **kwargs):
        is_new = self._state.adding
        if self.stock == 0 and self.status == 'published':
            self.status = 'out_of_stock'

        #super().save(update_fields=["original_price", "discounted_price", "status"])
        super().save(*args, **kwargs)

    def calculate_bundle_impact(self):
        total_waste = Decimal("0.0")
        total_co2 = Decimal("0.0")

        for item in self.items.all():
            produit = item.product
            catalogue = produit.catalog_entry
            unite = produit.unit
            quantite_totale = Decimal(item.quantity)

            impact_entry = ProductImpact.objects.filter(
                product=catalogue,
                unit=unite
            ).order_by('quantity').first()

            if impact_entry:
                multiplicateur = quantite_totale / impact_entry.quantity
                avoided_waste_kg = (multiplicateur * impact_entry.avoided_waste_kg).quantize(Decimal('0.001'))
                avoided_co2_kg = (multiplicateur * impact_entry.avoided_co2_kg).quantize(Decimal('0.001'))

                item.avoided_waste_kg = avoided_waste_kg
                item.avoided_co2_kg = avoided_co2_kg
                item.save(update_fields=['avoided_waste_kg', 'avoided_co2_kg'])

                total_waste += avoided_waste_kg
                total_co2 += avoided_co2_kg

            else:
                print(f"Aucun impact trouvé pour {produit.title} avec l’unité '{unite}'")

        self.total_avoided_waste_kg = total_waste.quantize(Decimal('0.01'))
        self.total_avoided_co2_kg = total_co2.quantize(Decimal('0.01'))
        self.save(update_fields=["total_avoided_waste_kg", "total_avoided_co2_kg"])

    
    @property
    def is_out_of_stock(self):
        return self.stock == 0
    
    def __str__(self):
        return self.title


class ProductBundleItem(BaseModel):
    bundle = models.ForeignKey(ProductBundle, on_delete=models.CASCADE, related_name="items")
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    quantity = models.PositiveIntegerField(default=1)
    best_before_date = models.DateField(
        null=True,
        blank=True,
        verbose_name="Best before date (DLUO)"
    )
    avoided_waste_kg = models.DecimalField(max_digits=7, decimal_places=3, default=0.0)
    avoided_co2_kg = models.DecimalField(max_digits=7, decimal_places=3, default=0.0)

    class Meta:
        unique_together = ("bundle", "product")

    def __str__(self):
        return f"{self.quantity} x {self.product.title}"
    

# === Paiement et paramètres ===

class PaymentMethod(BaseModel):
    METHOD_CHOICES = [('card', 'Carte bancaire'), ('paypal', 'PayPal'), ('rib', 'RIB')]
    user = models.ForeignKey(CustomUser, on_delete=models.CASCADE)
    type = models.CharField(max_length=20, choices=METHOD_CHOICES)
    provider_name = models.CharField(max_length=50)
    digits = models.CharField(max_length=34, blank=True, null=True)
    paypal_email = models.EmailField(blank=True, null=True)
    is_default = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)


class UserSetting(BaseModel):
    user = models.OneToOneField(CustomUser, on_delete=models.CASCADE)
    notif_promotions = models.BooleanField(default=False)
    notif_new_products = models.BooleanField(default=False)
    notif_orders = models.BooleanField(default=False)
    download_data_requested = models.DateTimeField(null=True, blank=True)
    account_deletion_requested = models.DateTimeField(null=True, blank=True)


class UserMeta(models.Model):
    """Données techniques ou annexes associées à un utilisateur."""
    user = models.OneToOneField(CustomUser, on_delete=models.CASCADE)
    signup_ip = models.GenericIPAddressField(blank=True, null=True)
    last_login_ip = models.GenericIPAddressField(blank=True, null=True)
    browser_info = models.TextField(blank=True, null=True)
    device_info = models.TextField(blank=True, null=True)
    locale = models.CharField(max_length=20, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Métadonnées pour {self.user.email}"
    

# === Commandes, Panier ===

User = get_user_model()
class Order(BaseModel):
    STATUS_CHOICES = [
        ('pending', 'En attente'),
        ('confirmed', 'Confirmée'),
        ('delivered', 'Livrée'),
        ('cancelled', 'Annulée'),
    ]

    user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    order_code = models.CharField(max_length=20, unique=True, editable=False)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='confirmed')

    total_price = models.DecimalField(max_digits=10, decimal_places=2)
    subtotal = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    shipping_cost = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    shipping_address = models.ForeignKey('Address', on_delete=models.SET_NULL, null=True, blank=True)
    billing_address = models.ForeignKey('Address', on_delete=models.SET_NULL, null=True, blank=True, related_name='billing_orders')
    payment_method = models.ForeignKey('PaymentMethod', on_delete=models.SET_NULL, null=True, blank=True, related_name='orders')

    order_total_avoided_waste_kg = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    order_total_avoided_co2_kg = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    order_total_savings = models.DecimalField(max_digits=10, decimal_places=2, default=0)

    shipping_address_snapshot = models.JSONField(null=True, blank=True)
    billing_address_snapshot = models.JSONField(null=True, blank=True)
    payment_method_snapshot = models.JSONField(null=True, blank=True)

    customer_rating = models.PositiveSmallIntegerField(null=True, blank=True, choices=[(i, str(i)) for i in range(1, 6)])
    customer_note = models.TextField(blank=True)
    rated_at = models.DateTimeField(null=True, blank=True)

    def update_totals(self):
        total_waste = Decimal("0.0")
        total_co2 = Decimal("0.0")
        total_savings = Decimal("0.0")
        for item in self.items.all():
            total_waste += item.order_item_total_avoided_waste_kg
            total_co2 += item.order_item_total_avoided_co2_kg
            total_savings += item.order_item_savings
        self.order_total_avoided_waste_kg = total_waste
        self.order_total_avoided_co2_kg = total_co2
        self.order_total_savings = total_savings

    def save(self, *args, **kwargs):
        if not self.order_code:
            self.order_code = uuid.uuid4().hex[:10].upper()
        super().save(*args, **kwargs)


class OrderItem(BaseModel):
    order = models.ForeignKey(Order, on_delete=models.CASCADE, related_name='items')
    bundle = models.ForeignKey('ProductBundle', on_delete=models.SET_NULL, null=True)
    quantity = models.PositiveIntegerField()
    total_price = models.DecimalField(max_digits=8, decimal_places=2, default=0)

    order_item_total_avoided_waste_kg = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    order_item_total_avoided_co2_kg = models.DecimalField(max_digits=7, decimal_places=2, default=0)
    order_item_savings = models.DecimalField(max_digits=8, decimal_places=2, default=0)

    bundle_snapshot = models.JSONField(null=True, blank=True)

    # NEW: rating fields por item
    customer_rating = models.PositiveSmallIntegerField(null=True, blank=True, choices=[(i, str(i)) for i in range(1, 6)])
    customer_note = models.TextField(blank=True)
    rated_at = models.DateTimeField(null=True, blank=True)

    objects = ActiveManager()
    all_objects = models.Manager()

    def soft_deactivate(self):
        self.is_active = False
        self.deactivated_at = timezone.now()
        self.save(update_fields=["is_active", "deactivated_at", "updated_at"])

    def calculate_savings(self):
        if self.bundle_snapshot:
            original_price = Decimal(self.bundle_snapshot.get("original_price", "0"))
            discounted_price = Decimal(self.bundle_snapshot.get("discounted_price", original_price))
            return (original_price - discounted_price) * self.quantity
        return Decimal("0.0")

    def save(self, *args, **kwargs):
        self.order_item_savings = self.calculate_savings()
        super().save(*args, **kwargs)
        if self.order_id:
            self.order.update_totals()
            self.order.save()




# === Favoris, écologie, récompenses ===

class Favorite(BaseModel):
    user = models.ForeignKey(CustomUser, on_delete=models.CASCADE)
    bundle = models.ForeignKey(ProductBundle, on_delete=models.CASCADE)
    added_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'bundle')

class RewardBenefit(models.TextChoices):
    NONE = 'none', 'None'
    COUPON = 'coupon', 'Coupon'
    FREESHIP = 'freeship', 'Free Shipping'

class RewardTier(models.Model):
    code = models.SlugField(unique=True)
    title = models.CharField(max_length=100)
    description = models.TextField(blank=True)
    icon = models.CharField(max_length=100, blank=True)
    min_orders = models.PositiveIntegerField(default=0)
    min_waste_kg = models.DecimalField(max_digits=9, decimal_places=2, default=0)
    min_co2_kg = models.DecimalField(max_digits=9, decimal_places=2, default=0)
    min_producers_supported = models.PositiveIntegerField(default=0)
    min_savings_eur = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    benefit_kind = models.CharField(max_length=20, choices=RewardBenefit.choices, default=RewardBenefit.NONE)
    benefit_config = models.JSONField(blank=True, default=dict)
    is_active = models.BooleanField(default=True)

class RewardStatus(models.TextChoices):
    NONE = 'none', 'None'
    PENDING = 'pending', 'Pending'
    BLOCKED = 'blocked', 'Blocked'
    FULFILLED = 'fulfilled', 'Fulfilled'

class Reward(models.Model):
    user = models.ForeignKey(CustomUser, on_delete=models.CASCADE)
    title = models.CharField(max_length=100)
    description = models.TextField()
    earned_on = models.DateTimeField(auto_now_add=True)
    tier = models.ForeignKey(RewardTier, on_delete=models.SET_NULL, null=True, blank=True)
    benefit_status = models.CharField(max_length=20, choices=RewardStatus.choices, default=RewardStatus.NONE)
    benefit_payload = models.JSONField(blank=True, default=dict)
    fulfilled_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ('user', 'tier')

class UserRewardProgress(models.Model):
    user = models.OneToOneField(CustomUser, on_delete=models.CASCADE, related_name='rewards_progress')
    total_orders = models.PositiveIntegerField(default=0)
    total_waste_kg = models.DecimalField(max_digits=9, decimal_places=2, default=0)
    total_co2_kg = models.DecimalField(max_digits=9, decimal_places=2, default=0)
    total_savings_eur = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    producers_supported = models.PositiveIntegerField(default=0)
    seen_producer_ids = models.JSONField(default=list, blank=True)  
    last_updated = models.DateTimeField(auto_now=True)



class Cart(BaseModel):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                             on_delete=models.CASCADE, related_name='carts')
    session_key = models.CharField(max_length=64, blank=True, null=True, db_index=True)

    class Meta:
        indexes = [models.Index(fields=['session_key'])]

    def __str__(self):
        owner = self.user.email if self.user_id else (self.session_key or "no-owner")
        return f"Cart<{owner}>"

class CartItem(BaseModel):
    cart = models.ForeignKey(Cart, on_delete=models.CASCADE, related_name='items')
    bundle = models.ForeignKey('ProductBundle', on_delete=models.CASCADE)
    quantity = models.PositiveIntegerField(default=1)
    price_snapshot = models.DecimalField(max_digits=10, decimal_places=2)
    title_snapshot = models.CharField(max_length=255, blank=True)
    bundle_image = models.URLField(max_length=500, null=True, blank=True)

    company_id_snapshot = models.IntegerField(null=True, blank=True)
    company_name_snapshot = models.CharField(max_length=255, null=True, blank=True)
    dluo_snapshot = models.CharField(max_length=120, null=True, blank=True)

    avoided_waste_kg = models.DecimalField(max_digits=7, decimal_places=3, default=0.0)
    avoided_co2_kg = models.DecimalField(max_digits=7, decimal_places=3, default=0.0)

    objects = ActiveManager()
    all_objects = models.Manager()

    def soft_deactivate(self):
        self.is_active = False
        self.deactivated_at = timezone.now()
        self.save(update_fields=["is_active", "deactivated_at", "updated_at"])

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['cart', 'bundle'],
                condition=Q(is_active=True),
                name='uniq_active_cart_bundle',
            ),
        ]




# === Blog ===

class BlogCategory(BaseModel):
    name = models.CharField(max_length=80, unique=True)
    slug = models.SlugField(max_length=80, unique=True)
    description = models.CharField(max_length=255, blank=True)
    color = models.CharField(max_length=7, blank=True)
    icon = models.CharField(max_length=80, blank=True)
    order = models.PositiveSmallIntegerField(default=0)

    def __str__(self):
        return self.name
    

class BlogPost(BaseModel):  
    STATUS = [
        ('draft', 'Draft'),
        ('scheduled', 'Scheduled'),
        ('published', 'Published'),
        ('archived', 'Archived'),
    ]
    title = models.CharField(max_length=200)
    slug = models.SlugField(unique=True)
    category = models.ForeignKey(BlogCategory, on_delete=models.SET_NULL, null=True, related_name='posts')
    excerpt = models.TextField(blank=True)
    content = models.TextField()
    image = models.ImageField(upload_to='blog/', blank=True, null=True)
    image_alt = models.CharField(max_length=255, blank=True)
    read_time_min = models.PositiveSmallIntegerField(default=0)
    author = models.ForeignKey(CustomUser, on_delete=models.SET_NULL, null=True, limit_choices_to={'is_staff': True})
    status = models.CharField(max_length=10, choices=STATUS, default='draft')
    pinned = models.BooleanField(default=False)
    published_at = models.DateTimeField(null=True, blank=True)



# === Messages ===


class ContactCategory(models.TextChoices):
    GENERAL = 'general', 'Question générale'
    TECHNICAL = 'technical', 'Support technique'
    PRODUCER = 'producer', 'Devenir producteur'
    PARTNERSHIP = 'partnership', 'Partenariat'
    COMPLAINT = 'complaint', 'Réclamation'

class ContactMessage(models.Model):
    full_name = models.CharField(max_length=255)
    email = models.EmailField()
    subject = models.CharField(max_length=255)
    message = models.TextField()
    category = models.CharField(max_length=50, choices=ContactCategory.choices)
    created_at = models.DateTimeField(auto_now_add=True)


# === Pages à propos et légales ===

class AboutSection(models.Model):
    slug = models.SlugField(unique=True)
    title = models.CharField(max_length=255)
    content = models.TextField()
    image = models.ImageField(upload_to='about/', blank=True, null=True)

    def __str__(self):
        return self.title

class CoreValue(models.Model):
    title = models.CharField(max_length=100)
    description = models.TextField()
    icon = models.ImageField(upload_to='values/', blank=True, null=True)

    def __str__(self):
        return self.title

class TeamMember(models.Model):
    name = models.CharField(max_length=100)
    position = models.CharField(max_length=100)
    bio = models.TextField(blank=True)
    photo = models.ImageField(upload_to='team/')

    def __str__(self):
        return f"{self.name} - {self.position}"

class LegalInformation(models.Model):
    title = models.CharField(max_length=255)
    content = models.TextField()
    last_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.title

class SiteSetting(models.Model):
    key = models.CharField(max_length=100, unique=True)
    value = models.TextField()
    description = models.TextField(blank=True, null=True)
    last_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.key} = {self.value}"

class PayPalTransaction(models.Model):
    user = models.ForeignKey(CustomUser, on_delete=models.CASCADE)
    order_id = models.CharField(max_length=100)
    status = models.CharField(max_length=50, default='CREATED')
    amount = models.DecimalField(max_digits=8, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)