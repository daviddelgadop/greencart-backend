# core/serializers_analytics.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple

from rest_framework import serializers

from .models import (
    Order,
    OrderItem,
    Cart,
    CartItem,
    Product,
    ProductBundle,
    ProductBundleItem,
)

# ============================================================
# Aides utilitaires (FR)
# ============================================================


def _iter_related(maybe_manager):
    if maybe_manager is None:
        return []
    if hasattr(maybe_manager, "all"):
        return maybe_manager.all()
    return maybe_manager


def _float(val, default=0.0) -> float:
    try:
        return float(val)
    except Exception:
        return default

def _safe_get(d: Optional[dict], path: List[str], default=None):
    cur = d or {}
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _collect_item_snapshots(self, obj: Order) -> List[dict]:
    snaps: List[dict] = []
    items = getattr(obj, "items", None)
    if not items:
        return snaps
    for it in items.all() if hasattr(items, "all") else items:
        snap = getattr(it, "bundle_snapshot", None)
        if isinstance(snap, dict):
            snaps.append(snap)
    return snaps


def _category_from_product(prod: Optional[Product]) -> Dict[str, Any]:
    """
    Return product category with a stable shape: {"id", "code", "label"}.
    Tries ORM (catalog_entry.category), then denormalized catalog_entry_data,
    then direct/fallback fields; fills 'label' even if only 'name' exists.
    """
    out = {"id": None, "code": None, "label": None}
    if prod is None:
        return out

    # 1) ORM: product.catalog_entry.category (ProductCategory)
    cat = getattr(getattr(prod, "catalog_entry", None), "category", None)
    if cat is not None:
        return {
            "id": getattr(cat, "id", None),
            "code": getattr(cat, "code", None),
            "label": getattr(cat, "label", None) or getattr(cat, "name", None),
        }

    # 2) Denormalized dict: product.catalog_entry_data["category"]
    ced = getattr(prod, "catalog_entry_data", None)
    if isinstance(ced, dict):
        c = ced.get("category") or {}
        if isinstance(c, dict) and (c.get("id") is not None or c.get("label") or c.get("name") or c.get("code")):
            return {
                "id": c.get("id"),
                "code": c.get("code"),
                "label": c.get("label") or c.get("name"),
            }

    # 3) Direct FK : product.category
    cat = getattr(prod, "category", None)
    if cat is not None:
        return {
            "id": getattr(cat, "id", None),
            "code": getattr(cat, "code", None),
            "label": getattr(cat, "label", None) or getattr(cat, "name", None),
        }

    # 4) Flat fields
    cid = getattr(prod, "category_id", None)
    ccode = getattr(prod, "category_code", None)
    cname = getattr(prod, "category_name", None)
    if cid is not None or ccode or cname:
        return {
            "id": cid,
            "code": ccode,
            "label": cname,  
        }

    return out




def _producers_from_snapshot(snapshot: Optional[dict]) -> Tuple[List[int], List[str]]:
    snap = snapshot or {}
    ids: List[int] = []
    names: List[str] = []
    seen = set()

    cid = snap.get("company_id")
    cname = snap.get("company_name")
    if cid is not None and cid not in seen:
        seen.add(cid)
        ids.append(cid)
        names.append(cname or f"Company {cid}")

    for p in snap.get("products") or []:
        pcid = p.get("company_id")
        pcname = p.get("company_name")
        if pcid is None or pcid in seen:
            continue
        seen.add(pcid)
        ids.append(pcid)
        names.append(pcname or f"Company {pcid}")

    return ids, names


def _addr_obj_to_light(addr) -> Optional[Dict[str, Any]]:
    """
    FR: Convertit un objet Address réel en vue légère.
    Construit line1 depuis street_number + street_name.
    Essaie d'extraire codes/noms via city -> department -> region.
    """
    if not addr:
        return None

    street_number = getattr(addr, "street_number", None) or ""
    street_name = getattr(addr, "street_name", None) or ""
    line1 = f"{street_number} {street_name}".strip() or None

    city = getattr(addr, "city", None)
    postal_code = getattr(city, "postal_code", None)
    city_name = getattr(city, "name", None)

    department_obj = getattr(city, "department", None) if city else None
    region_obj = getattr(department_obj, "region", None) if department_obj else None

    department_code = getattr(department_obj, "code", None)
    department_name = getattr(department_obj, "name", None)
    region_code = getattr(region_obj, "code", None)
    region_name = getattr(region_obj, "name", None)

    return {
        "line1": line1,
        "postal_code": postal_code,
        "city": city_name,
        "department": department_code or department_name,
        "region": region_code or region_name,
        "country": getattr(city, "country_name", None),
    }


# ============================================================
# Serializers “mini”
# ============================================================

class ProductMiniSerializer(serializers.Serializer):
    """FR: Vue minimale d’un produit (pour composants de bundle)."""
    product_id = serializers.IntegerField(source="id")
    title = serializers.CharField()
    sku = serializers.CharField(required=False, allow_null=True, allow_blank=True)
    category = serializers.SerializerMethodField()

    def get_category(self, obj: Product):
        return _category_from_product(obj)


class BundleComponentSerializer(serializers.Serializer):
    """
    FR: Composant d’un bundle (ProductBundleItem) avec infos du produit.
    """
    product_id = serializers.IntegerField(source="product.id")
    title = serializers.CharField(source="product.title", allow_null=True)
    per_bundle_quantity = serializers.IntegerField(source="quantity")
    best_before_date = serializers.DateField(required=False, allow_null=True)
    category = serializers.SerializerMethodField()

    def get_category(self, obj: ProductBundleItem):
        return _category_from_product(getattr(obj, "product", None))


class BundleMiniSerializer(serializers.ModelSerializer):
    """
    FR: Bundle avec ses composants (items).
    """
    items = BundleComponentSerializer(source="items", many=True)

    class Meta:
        model = ProductBundle
        fields = ("id", "title", "stock", "items")


class CategoryLightSerializer(serializers.Serializer):
    """FR: Catégorie minimaliste (adapter si vous avez un modèle Category)."""
    id = serializers.IntegerField()
    code = serializers.CharField()
    label = serializers.CharField()


class ProductLightSerializer(serializers.ModelSerializer):
    category = serializers.SerializerMethodField()

    class Meta:
        model = Product
        fields = ("id", "title", "sku", "stock", "sold_units", "category")

    def get_category(self, obj):
        c = _category_from_product(obj)
        # Return None if completely empty
        if not (c.get("id") or c.get("code") or c.get("label")):
            return None
        return c
    

class BundleItemDeepSerializer(serializers.ModelSerializer):
    """FR: Détail d’un composant de bundle (+ DLC)."""
    product = ProductLightSerializer(read_only=True)

    class Meta:
        model = ProductBundleItem
        fields = ("id", "product", "per_bundle_quantity", "best_before_date", "is_active")


class BundleDeepSerializer(serializers.ModelSerializer):
    """FR: Bundle détaillé avec ses composants."""
    items = BundleItemDeepSerializer(many=True, source="items", read_only=True)

    class Meta:
        model = ProductBundle
        fields = ("id", "title", "stock", "sold_bundles", "items")





# ============================================================
# Order / OrderItem — champs producteurs unifiés
# ============================================================




class SalesOrderItemRowSerializer(serializers.Serializer):
    """
    One row per OrderItem, frozen from the item snapshot at purchase time.
    """
    order_id = serializers.IntegerField()
    created_at = serializers.DateTimeField()
    status = serializers.CharField()

    item_id = serializers.IntegerField()
    quantity = serializers.IntegerField()
    unit_price = serializers.FloatField()
    line_total = serializers.FloatField()

    bundle_id = serializers.IntegerField(allow_null=True)
    bundle_title = serializers.CharField(allow_null=True)

    producer_id = serializers.IntegerField(allow_null=True)
    producer_name = serializers.CharField(allow_null=True)

    company_id = serializers.IntegerField(allow_null=True)
    company_name = serializers.CharField(allow_null=True)

    @staticmethod
    def from_orders(orders: List[Order]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for o in orders:
            items = _iter_related(getattr(o, "items", None))
            for it in items:
                snap = getattr(it, "bundle_snapshot", None)
                snap = snap if isinstance(snap, dict) else {}

                qty = int(getattr(it, "quantity", 0) or 0)
                line_total = _float(getattr(it, "total_price", 0) or 0.0)
                unit_price = round(line_total / qty, 2) if qty > 0 else 0.0

                rows.append({
                    "order_id": o.id,
                    "created_at": o.created_at,
                    "status": o.status,

                    "item_id": it.id,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "line_total": round(line_total, 2),

                    "bundle_id": snap.get("id"),
                    "bundle_title": snap.get("title"),

                    "producer_id": snap.get("producer_id"),
                    "producer_name": snap.get("producer_name"),

                    "company_id": snap.get("company_id"),
                    "company_name": snap.get("company_name"),
                })
        return rows
    

class SalesOrderRowSerializer(serializers.ModelSerializer):
    units = serializers.SerializerMethodField()
    producer_ids = serializers.SerializerMethodField()
    producer_names = serializers.SerializerMethodField()
    company_names = serializers.SerializerMethodField()

    class Meta:
        model = Order
        fields = (
            "id",
            "created_at",
            "total_price",
            "units",
            "status",
            "producer_ids",
            "producer_names",
            "company_names",
        )

    def _collect_item_snapshots(self, obj: Order) -> List[dict]:
        snaps: List[dict] = []
        items = getattr(obj, "items", None)
        if not items:
            return snaps
        for it in items.all() if hasattr(items, "all") else items:
            snap = getattr(it, "bundle_snapshot", None)
            if isinstance(snap, dict):
                snaps.append(snap)
        return snaps

    def get_units(self, obj: Order) -> int:
        items = getattr(obj, "items", None)
        if not items:
            return 0
        total = 0
        for it in items.all() if hasattr(items, "all") else items:
            q = getattr(it, "quantity", 0) or 0
            try:
                total += int(q)
            except Exception:
                pass
        return total

    def get_producer_ids(self, obj: Order) -> List[int]:
        ids: List[int] = []
        seen = set()
        for snap in self._collect_item_snapshots(obj):
            pids, _ = _producers_from_snapshot(snap)
            for pid in pids:
                if pid in seen:
                    continue
                seen.add(pid)
                ids.append(pid)
        return ids

    def get_producer_names(self, obj: Order) -> List[str]:
        names: List[str] = []
        seen = set()
        for snap in self._collect_item_snapshots(obj):
            _, pnames = _producers_from_snapshot(snap)
            for name in pnames:
                if name in seen:
                    continue
                seen.add(name)
                names.append(name)
        return names

    def get_company_names(self, obj: Order) -> List[str]:
        return self.get_producer_names(obj)
    


class OrderItemDeepSerializer(serializers.ModelSerializer):
    unit_price = serializers.SerializerMethodField()
    producer_ids = serializers.SerializerMethodField()
    producer_names = serializers.SerializerMethodField()
    company_names = serializers.SerializerMethodField()
    item_id = serializers.IntegerField(source="id", read_only=True)

    class Meta:
        model = OrderItem
        fields = (
            "id",   
            "item_id", 
            "quantity",
            "total_price",
            "unit_price",
            "bundle_id",
            "bundle_snapshot",
            "producer_ids",
            "producer_names",
            "company_names",
        )

    def get_unit_price(self, obj):
        try:
            q = int(obj.quantity or 0)
            if q <= 0:
                return 0.0
            return float((obj.total_price or 0) / q)
        except Exception:
            return 0.0

    # --- Alive relations (bundle -> items -> product -> company -> owner) ---
    def _bundle_producers_and_companies(self, obj: OrderItem):
        ids, owner_names, company_names, seen = [], [], [], set()
        b = getattr(obj, "bundle", None)
        if not b:
            return ids, owner_names, company_names

        items = getattr(b, "items", None)
        iterable = items.all() if hasattr(items, "all") else (items or [])
        for bi in iterable:
            prod = getattr(bi, "product", None)
            comp = getattr(prod, "company", None)
            if not comp:
                continue

            cid = getattr(comp, "id", None)
            if cid is None or cid in seen:
                continue
            seen.add(cid)
            ids.append(cid)

            # company name
            company_names.append(getattr(comp, "name", None))

            # owner display name
            owner = getattr(comp, "owner", None)
            display = (
                getattr(owner, "public_display_name", None)
                or " ".join(
                    x for x in [
                        (getattr(owner, "first_name", "") or "").strip(),
                        (getattr(owner, "last_name", "") or "").strip(),
                    ] if x
                )
                or getattr(owner, "username", None)
                or getattr(owner, "email", None)
            )
            owner_names.append(display)

        return ids, owner_names, company_names

    def get_producer_ids(self, obj: OrderItem) -> List[int]:
        pids, _, _ = self._bundle_producers_and_companies(obj)
        return pids

    def get_producer_names(self, obj: OrderItem) -> List[str]:
        _, pnames, _ = self._bundle_producers_and_companies(obj)
        return [n for n in pnames if n]

    def get_company_names(self, obj: OrderItem) -> List[str]:
        _, _, cnames = self._bundle_producers_and_companies(obj)
        return [c for c in cnames if c]


class OrderDeepSerializer(serializers.ModelSerializer):
    """
    Rich order (sin usar snapshots para productor/empresa):
    - items (cada item resuelve productor/empresa desde el bundle vivo)
    - payments (o fallback desde payment_method_snapshot)
    - addresses (objetos reales o snapshots)
    - métricas de impacto
    - campos agregados de productor/empresa a nivel pedido (deduplicados) construidos desde items
    """
    user_name = serializers.SerializerMethodField()

    items = OrderItemDeepSerializer(many=True, read_only=True)

    payments = serializers.SerializerMethodField()
    shipping_address = serializers.SerializerMethodField()
    billing_address = serializers.SerializerMethodField()

    producer_ids = serializers.SerializerMethodField()
    producer_names = serializers.SerializerMethodField()
    company_names = serializers.SerializerMethodField()

    class Meta:
        model = Order
        fields = (
            "id", "order_code", "created_at", "status",
            "subtotal", "shipping_cost", "total_price",
            "order_total_avoided_waste_kg", "order_total_avoided_co2_kg", "order_total_savings",
            "items", "payments", "shipping_address", "billing_address",
            "producer_ids", "producer_names", "company_names",
            "user_name",
        )

    # -------------------- Payments --------------------
    def get_payments(self, obj):
        pays = (
            getattr(obj, "payments", None)
            or getattr(obj, "payment_set", None)
            or getattr(obj, "order_payments", None)
        )
        if not pays:
            snap = getattr(obj, "payment_method_snapshot", None)
            if isinstance(snap, dict):
                return [{
                    "method": f"{snap.get('type') or 'unknown'}:{snap.get('provider') or 'unknown'}",
                    "status": "paid" if obj.status in {"confirmed", "delivered"} else obj.status,
                    "amount": float(obj.total_price or 0.0),
                }]
            return []
        iterable = pays.all() if hasattr(pays, "all") else pays
        out = []
        for p in iterable:
            out.append({
                "method": getattr(p, "method", None),
                "status": getattr(p, "status", None),
                "amount": float(getattr(p, "amount", 0) or 0),
            })
        return out

    # -------------------- Addresses --------------------
    def get_shipping_address(self, obj):
        addr = getattr(obj, "shipping_address", None)
        if addr:
            return _addr_obj_to_light(addr)
        snap = getattr(obj, "shipping_address_snapshot", None) or {}
        if isinstance(snap, dict):
            return {
                "line1": snap.get("line1"),
                "postal_code": snap.get("postal_code"),
                "city": snap.get("city"),
                "department": snap.get("department_code") or snap.get("department"),
                "region": snap.get("region_code") or snap.get("region"),
                "country": snap.get("country"),
            }
        return None

    def get_billing_address(self, obj):
        addr = getattr(obj, "billing_address", None)
        if addr:
            return _addr_obj_to_light(addr)
        snap = getattr(obj, "billing_address_snapshot", None) or {}
        if isinstance(snap, dict):
            return {
                "line1": snap.get("line1"),
                "postal_code": snap.get("postal_code"),
                "city": snap.get("city"),
                "department": snap.get("department_code") or snap.get("department"),
                "region": snap.get("region_code") or snap.get("region"),
                "country": snap.get("country"),
            }
        return None

    # -------------------- Producers --------------------
    def _iter_items(self, obj: Order):
        items = getattr(obj, "items", None)
        return items.all() if hasattr(items, "all") else (items or [])

    def get_producer_ids(self, obj: Order) -> List[int]:
        ids, seen = [], set()
        for it in self._iter_items(obj):
            pids = OrderItemDeepSerializer().get_producer_ids(it)
            for pid in pids:
                if pid is None or pid in seen:
                    continue
                seen.add(pid)
                ids.append(pid)
        return ids

    def get_producer_names(self, obj: Order) -> List[str]:
        names, seen = [], set()
        for it in self._iter_items(obj):
            pnames = OrderItemDeepSerializer().get_producer_names(it)
            for n in pnames:
                if not n or n in seen:
                    continue
                seen.add(n)
                names.append(n)
        return names

    def get_company_names(self, obj: Order) -> List[str]:
        names, seen = [], set()
        for it in self._iter_items(obj):
            cnames = OrderItemDeepSerializer().get_company_names(it)
            for n in cnames:
                if not n or n in seen:
                    continue
                seen.add(n)
                names.append(n)
        return names

    # -------------------- User display --------------------
    def get_user_name(self, obj):
        u = getattr(obj, "user", None)
        if not u:
            return None
        return (
            getattr(u, "public_display_name", None)
            or f"{getattr(u, 'first_name', '')} {getattr(u, 'last_name', '')}".strip()
            or getattr(u, "email", None)
            or getattr(u, "username", None)
        )



class PaymentLightSerializer(serializers.Serializer):
    """FR: Paiement minimal (adapter aux champs réels)."""
    method = serializers.CharField()
    status = serializers.CharField(allow_null=True)
    amount = serializers.FloatField()


class AddressLightSerializer(serializers.Serializer):
    """FR: Adresse minimaliste (adapter aux champs réels)."""
    line1 = serializers.CharField(allow_null=True)
    postal_code = serializers.CharField(allow_null=True)
    city = serializers.CharField(allow_null=True)
    department = serializers.CharField(allow_null=True)
    region = serializers.CharField(allow_null=True)
    country = serializers.CharField(allow_null=True)




# ============================================================
# Clients & Carts
# ============================================================

class CustomerRowSerializer(serializers.Serializer):
    """FR: Vue client — agrégats + méta."""
    user_id = serializers.IntegerField()
    first_order = serializers.DateTimeField()
    last_order = serializers.DateTimeField()
    orders = serializers.IntegerField()
    spent = serializers.FloatField()
    segment = serializers.CharField()


class CartItemDeepSerializer(serializers.ModelSerializer):
    """
    FR: CartItem -> bundle -> composants (avec produits).
    NB: On ne duplique pas les champs producteurs ici (les endpoints “cross”/deep
    les ajoutent déjà côté vue si nécessaire via snapshot).
    """
    bundle = BundleMiniSerializer(read_only=True)

    class Meta:
        model = CartItem
        fields = ("id", "bundle", "quantity")


class CartDeepSerializer(serializers.ModelSerializer):
    """
    FR: Panier riche avec items complets.
    """
    cart_items = CartItemDeepSerializer(many=True, read_only=True)

    class Meta:
        model = Cart
        fields = ("id", "user_id", "is_active", "updated_at", "cart_items")


class CartsAbandonedBundleLightSerializer(serializers.Serializer):
    bundle_id = serializers.IntegerField(allow_null=True)
    title = serializers.CharField(allow_null=True, required=False)
    stock = serializers.IntegerField(required=False)
    products = serializers.ListField(child=serializers.DictField(), required=False)
    producer_ids = serializers.ListField(child=serializers.IntegerField(), required=False)
    producer_names = serializers.ListField(child=serializers.CharField(), required=False)
    company_names = serializers.ListField(child=serializers.CharField(), required=False)


class CartsAbandonedItemSerializer(serializers.Serializer):
    cart_item_id = serializers.IntegerField(allow_null=True)
    quantity = serializers.IntegerField()
    unit_price = serializers.FloatField(required=False)  
    line_total = serializers.FloatField(required=False)
    bundle = CartsAbandonedBundleLightSerializer(allow_null=True)


class CartsAbandonedRowSerializer(serializers.Serializer):
    cart_id = serializers.IntegerField()
    user_id = serializers.IntegerField(allow_null=True)
    user_name = serializers.CharField(allow_null=True, required=False)
    updated_at = serializers.DateTimeField(allow_null=True)
    items_qty = serializers.IntegerField()
    amount = serializers.FloatField(required=False)   
    items = CartsAbandonedItemSerializer(many=True)


class CartsAbandonedItemRowSerializer(serializers.Serializer):
    cart_id = serializers.IntegerField()
    user_id = serializers.IntegerField(allow_null=True)
    user_name = serializers.CharField(allow_null=True, required=False)
    updated_at = serializers.DateTimeField(allow_null=True)

    cart_item_id = serializers.IntegerField(allow_null=True)
    quantity = serializers.IntegerField()
    unit_price = serializers.FloatField(required=False)
    line_total = serializers.FloatField(required=False)
    bundle = CartsAbandonedBundleLightSerializer(allow_null=True)


class PaymentsDeepSerializer(serializers.Serializer):
    order_id = serializers.IntegerField()
    order_item_id = serializers.IntegerField(required=False, allow_null=True)
    created_at = serializers.DateTimeField()
    method = serializers.CharField()
    status = serializers.CharField()
    amount = serializers.FloatField()
    producer_ids = serializers.ListField(child=serializers.IntegerField(), required=False)
    producer_names = serializers.ListField(child=serializers.CharField(), required=False)
    company_names = serializers.ListField(child=serializers.CharField(), required=False)
    user_name = serializers.CharField(source="user_name", required=False)
