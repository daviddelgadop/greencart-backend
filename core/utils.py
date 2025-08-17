import os
import json
import tempfile
import zipfile
from decimal import Decimal
from django.db.models import Avg, Count, Q
from .models import (
    Cart, OrderItem, ProductBundle, ProductBundleItem, Product,
    Company, CustomUser
)

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