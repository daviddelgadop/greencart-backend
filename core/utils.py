import os
import json
import tempfile
import zipfile
from decimal import Decimal
from django.db.models import Avg, Count, Q, QuerySet
from .models import (
    Cart, 
    OrderItem, 
    ProductBundle, 
    Product,
    CustomUser
)
from django.db import transaction
from django.contrib.auth import get_user_model
from typing import Dict, Any

User = get_user_model()

def export_user_data(user):
    from .serializers import (
        CustomUserSerializer, AddressSerializer, PaymentMethodSerializer,
        OrderSerializer, FavoriteSerializer, RewardSerializer,
        UserSettingSerializer, UserMetaSerializer,
        DocumentSerializer
    )

    data = {
        "user": CustomUserSerializer(user).data,
        "addresses": AddressSerializer(user.addresses.all(), many=True).data,
        "payment_methods": PaymentMethodSerializer(user.paymentmethod_set.all(), many=True).data,
        "orders": OrderSerializer(user.order_set.all(), many=True).data,
        "favorites": FavoriteSerializer(user.favorite_set.all(), many=True).data,
        "rewards": RewardSerializer(user.reward_set.all(), many=True).data,
        "settings": UserSettingSerializer(user.usersetting).data if hasattr(user, 'usersetting') else {},
        "meta": UserMetaSerializer(user.usermeta).data if hasattr(user, 'usermeta') else {},
        "documents": DocumentSerializer(user.documents.all(), many=True).data,
    }

    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
        with zipfile.ZipFile(tmp_zip.name, 'w') as zipf:
            # JSON
            json_path = os.path.join(tempfile.gettempdir(), f"user_data_{user.id}.json")
            with open(json_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
            zipf.write(json_path, arcname="mes_donnees_utilisateur.json")
            os.remove(json_path)

        return tmp_zip.name  # ZIP


def get_or_create_cart(request):
    """
    Returns the current cart for the request, creating it if necessary.
    Resolves by authenticated user first; else requires a valid session key.
    """
    user = request.user if request.user.is_authenticated else None
    session_key = request.headers.get('X-Session-Key') or request.COOKIES.get('sessionid')
    qs = Cart.objects.filter(is_active=True)

    if user:
        cart = qs.filter(user=user).order_by('-updated_at').first()
        if not cart:
            cart = Cart.objects.create(user=user, session_key=None)
        return cart

    if not session_key:
        raise ValueError("Missing X-Session-Key")

    cart = qs.filter(session_key=session_key, user__isnull=True).order_by('-updated_at').first()
    if not cart:
        cart = Cart.objects.create(user=None, session_key=session_key)
    return cart

def _decimal_or_none(val):
    if val is None:
        return None
    return Decimal(str(val)).quantize(Decimal('0.01'))

def recompute_bundle_rating(bundle_id: int):
    qs = OrderItem.objects.filter(
        bundle_id=bundle_id,
        customer_rating__isnull=False,
        is_active=True
    )
    agg = qs.aggregate(avg=Avg('customer_rating'), cnt=Count('id'))
    avg = _decimal_or_none(agg['avg'])
    cnt = int(agg['cnt'] or 0)

    ProductBundle.objects.filter(id=bundle_id).update(
        avg_rating=avg, ratings_count=cnt
    )
    return avg, cnt

def recompute_product_rating(product_id: int):
    # Average of avg_ratings of all bundles that include this product
    bundles = (
        ProductBundle.objects.filter(
            items__product_id=product_id,
            avg_rating__isnull=False,
            is_active=True
        )
        .values_list('avg_rating', flat=True)
        .distinct()
    )
    vals = [Decimal(str(x)) for x in bundles]
    if not vals:
        Product.objects.filter(id=product_id).update(avg_rating=None, ratings_count=0)
        return None, 0
    avg = (sum(vals) / Decimal(len(vals))).quantize(Decimal('0.01'))
    Product.objects.filter(id=product_id).update(avg_rating=avg, ratings_count=len(vals))
    return avg, len(vals)

def recompute_company_rating(company_id: int):
    # Average of bundles (with avg) that belong to this company
    bundles = ProductBundle.objects.filter(
        items__product__company_id=company_id,
        avg_rating__isnull=False,
        is_active=True
    ).values_list('avg_rating', flat=True).distinct()
    vals = [Decimal(str(x)) for x in bundles]
    from .models import Company
    if not vals:
        Company.objects.filter(id=company_id).update(avg_rating=None, ratings_count=0)
        return None, 0
    avg = (sum(vals) / Decimal(len(vals))).quantize(Decimal('0.01'))
    Company.objects.filter(id=company_id).update(avg_rating=avg, ratings_count=len(vals))
    return avg, len(vals)

def recompute_producer_rating(producer_id: int):
    # Average of bundles (with avg) across all companies of this producer
    bundles = ProductBundle.objects.filter(
        items__product__company__owner_id=producer_id,
        avg_rating__isnull=False,
        is_active=True
    ).values_list('avg_rating', flat=True).distinct()
    vals = [Decimal(str(x)) for x in bundles]
    if not vals:
        CustomUser.objects.filter(id=producer_id).update(avg_rating=None, ratings_count=0)
        return None, 0
    avg = (sum(vals) / Decimal(len(vals))).quantize(Decimal('0.01'))
    CustomUser.objects.filter(id=producer_id).update(avg_rating=avg, ratings_count=len(vals))
    return avg, len(vals)


def recompute_after_bundle(bundle_id: int):
    # Cascade: bundle -> products -> company -> producer
    from .models import ProductBundleItem
    # 1) bundle
    recompute_bundle_rating(bundle_id)

    # 2) products included in the bundle
    product_ids = list(
        ProductBundleItem.objects.filter(bundle_id=bundle_id).values_list('product_id', flat=True)
    )
    for pid in set(product_ids):
        recompute_product_rating(pid)

    # 3) company (owner of those products)
    from .models import Product
    company_ids = list(
        Product.objects.filter(id__in=product_ids).values_list('company_id', flat=True)
    )
    for cid in set(company_ids):
        recompute_company_rating(cid)

    # 4) producer (owner of those companies)
    from .models import Company
    producer_ids = list(
        Company.objects.filter(id__in=company_ids).values_list('owner_id', flat=True)
    )
    for uid in set(producer_ids):
        if uid:
            recompute_producer_rating(uid)



def hard_delete_user_and_related(user_id: int) -> Dict[str, Any]:
    """
    Suppression définitive d’un utilisateur et de toutes ses données associées,
    en renvoyant des métriques détaillées.

    Ordre de suppression (pour éviter PROTECT et les objets orphelins) :
      1) Supprimer les médias de l’utilisateur (avatar).
      2) Identifier et supprimer les ProductBundle liés aux produits des companies de l’utilisateur :
         - Supprimer les images de chaque bundle.
         - Supprimer les bundles (ProductBundleItem supprimé par CASCADE ;
           Favorite/CartItem supprimés par CASCADE ; OrderItem.bundle = SET_NULL).
      3) Supprimer les médias des companies (logo, images de produits, certifications)
         puis supprimer les companies (ce qui supprime aussi Product / ProductImage par CASCADE).
      4) Supprimer l’utilisateur (Address et autres entités supprimées par CASCADE/SET_NULL).
    """
    from .models import (
        Company,
        Product,
        ProductImage,
        ProductBundle,
        ProductBundleImage,
        ProductBundleItem,
        Certification,
    )

    metrics: Dict[str, Any] = {
        "user_id": user_id,
        "avatar_files_deleted": 0,
        "bundles_found": 0,
        "bundle_images_deleted": 0,
        "bundles_deleted": 0,
        "bundle_items_deleted": 0,         # estimé à partir du comptage avant delete
        "companies_found": 0,
        "company_logos_deleted": 0,
        "products_found": 0,
        "product_images_deleted": 0,
        "cert_files_deleted": 0,
        "companies_deleted": 0,
        "products_deleted": 0,             # estimé (CASCADE via Company)
        "product_images_rows_deleted": 0,  # estimé (CASCADE via Product)
        "certifications_deleted": 0,       # estimé (CASCADE via Company)
        "user_deleted": 0,
    }

    def _safe_delete_file(file_field) -> int:
        # Supprime le fichier du storage si présent, ignore les erreurs.
        try:
            if file_field and getattr(file_field, "name", None):
                name = file_field.name
                storage = file_field.storage
                file_field.delete(save=False)  # enlève la référence du modèle
                if storage.exists(name):
                    storage.delete(name)
                return 1
        except Exception:
            pass
        return 0

    with transaction.atomic():
        # Chargement de l’utilisateur avec verrouillage
        user = User.objects.select_for_update().get(pk=user_id)

        # Étape 1 : avatar de l’utilisateur
        metrics["avatar_files_deleted"] += _safe_delete_file(getattr(user, "avatar", None))

        # Étape 2 : bundles liés aux produits des companies de l’utilisateur
        company_ids = list(
            Company.objects.filter(owner=user).values_list("id", flat=True)
        )
        if company_ids:
            bundles_qs = (
                ProductBundle.objects
                .filter(items__product__company_id__in=company_ids)
                .distinct()
                .prefetch_related("images", "items")
            )
            metrics["bundles_found"] = bundles_qs.count()

            # Compter items avant la suppression (estimation)
            metrics["bundle_items_deleted"] = ProductBundleItem.objects.filter(
                bundle__in=bundles_qs
            ).count()

            # Supprimer images de bundles
            for bundle in bundles_qs:
                for bimg in getattr(bundle, "images", []).all():
                    metrics["bundle_images_deleted"] += _safe_delete_file(
                        getattr(bimg, "image", None)
                    )

            # Supprimer bundles (CASCADE: items; CASCADE: favorites/cartitems; SET_NULL: orderitems)
            num_deleted, details = bundles_qs.delete()  # <- desempaquetar
            if isinstance(details, dict):
                metrics["bundles_deleted"] = details.get(
                    f"{ProductBundle._meta.app_label}.{ProductBundle._meta.model_name}", 0
                )
            else:
                metrics["bundles_deleted"] = num_deleted 

        # Étape 3 : companies de l’utilisateur (et médias associés)
        companies_qs = Company.objects.filter(owner=user).prefetch_related(
            "products__images",
            "certifications",
            "products",
        )
        metrics["companies_found"] = companies_qs.count()

        # Compter produits / images / certifs avant suppressions (pour métriques)
        products_qs = Product.objects.filter(company__owner=user)
        product_images_qs = ProductImage.objects.filter(product__company__owner=user)
        certs_qs = Certification.objects.filter(company__owner=user)
        metrics["products_found"] = products_qs.count()

        # Nettoyage de médias (logos, images de produit, fichiers de certification)
        for company in companies_qs:
            metrics["company_logos_deleted"] += _safe_delete_file(getattr(company, "logo", None))
            for product in getattr(company, "products", []).all():
                for pimg in getattr(product, "images", []).all():
                    metrics["product_images_deleted"] += _safe_delete_file(
                        getattr(pimg, "image", None)
                    )
            for cert in getattr(company, "certifications", []).all():
                metrics["cert_files_deleted"] += _safe_delete_file(getattr(cert, "file", None))

        # Suppression des companies (CASCADE: products, product images, certifications)
        num_deleted_c, details_c = companies_qs.delete()  # <- desempaquetar
        if isinstance(details_c, dict):
            metrics["companies_deleted"] = details_c.get(
                f"{Company._meta.app_label}.{Company._meta.model_name}", 0
            )
            metrics["products_deleted"] = details_c.get(
                f"{Product._meta.app_label}.{Product._meta.model_name}", 0
            )
            metrics["product_images_rows_deleted"] = details_c.get(
                f"{ProductImage._meta.app_label}.{ProductImage._meta.model_name}", 0
            )
            metrics["certifications_deleted"] = details_c.get(
                f"{Certification._meta.app_label}.{Certification._meta.model_name}", 0
            )
        else:
            metrics["companies_deleted"] = num_deleted_c

        # Étape 4 : suppression de l’utilisateur (CASCADE/SET_NULL selon le modèle)
        user_pk = user.pk
        user.delete()
        metrics["user_deleted"] = 0 if User.objects.filter(pk=user_pk).exists() else 1

    return metrics

def producer_id_for(request) -> int:
    # If you have a ProducerProfile, map it here instead
    return request.user.id

def scope_orders_to_producer(qs: QuerySet, request) -> QuerySet:
    """
    Scope orders to commerces owned by the authenticated producer.
    This must be applied BEFORE any annotate/values/union.
    """
    pid = producer_id_for(request)
    return qs.filter(commerce__owner_id=pid)

def scope_items_to_producer(qs: QuerySet, request) -> QuerySet:
    pid = producer_id_for(request)
    return qs.filter(order__commerce__owner_id=pid)