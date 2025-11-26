from __future__ import annotations
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from collections import Counter, defaultdict
from django.contrib.auth import get_user_model
from .analytics_serializers import _category_from_product

from .models import Order, OrderItem, ProductBundleItem
from .analytics_scope import AnalyticsScopeMixin

from django.db.models import Avg, Count, Sum, Prefetch, Count, Min, Max
from django.conf import settings

from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response


from .models import (
    Order,
    OrderItem,
    Company,
    Cart,
    CartItem,
    Product,
    ProductBundle,
    ProductBundleItem,
)

from .analytics_serializers import (
    OrderDeepSerializer)

from .analytics_scope import AnalyticsScopeMixin

# ============================================================
# Constantes communes (FR)
# ============================================================

VALID_STATUSES = ("confirmed", "delivered")


# ============================================================
# Aides communes (FR)
# ============================================================

def _addr_obj_to_light(addr):
    if not addr:
        return None
    return {
        "id": getattr(addr, "id", None),
        "line1": getattr(addr, "line1", None),
        "line2": getattr(addr, "line2", None),
        "postal_code": getattr(addr, "postal_code", None),
        "city": getattr(addr, "city", None),
        "country": getattr(addr, "country", None),
    }


def _user_display_name(u):
    if not u:
        return None
    return (
        getattr(u, "public_display_name", None)
        or (" ".join(x for x in [(getattr(u, "first_name", "") or "").strip(),
                                 (getattr(u, "last_name", "") or "").strip()] if x) or None)
        or getattr(u, "username", None)
        or getattr(u, "email", None)
    )

def _owner_name_from_company(company):
    if not company:
        return None
    return _user_display_name(getattr(company, "owner", None))

def _bundle_companies_and_owners(bundle):
    company_ids, company_names, owner_names = [], [], []
    seen = set()
    if not bundle:
        return company_ids, company_names, owner_names
    items = getattr(bundle, "items", None)
    iterable = items.all() if hasattr(items, "all") else (items or [])
    for bi in iterable:
        prod = getattr(bi, "product", None)
        comp = getattr(prod, "company", None)
        if not comp:
            continue
        cid = getattr(comp, "id", None)
        if cid in seen:
            continue
        seen.add(cid)
        company_ids.append(cid)
        company_names.append(getattr(comp, "name", None))
        owner_names.append(_owner_name_from_company(comp))
    return company_ids, company_names, owner_names


def _company_owner_display_name(company):
    if not company:
        return None
    # Typical relation in your models: Company.owner -> CustomUser
    owner = getattr(company, "owner", None)
    return _user_display_name(owner)

def _bundle_producers_and_companies(bundle):
    """
    Return (producer_ids, producer_owner_names, company_names) for a live ProductBundle.
    producer_ids are company ids; names are owner (user) display names; company_names are shop names.
    """
    ids, owner_names, company_names, seen = [], [], [], set()
    if not bundle:
        return ids, owner_names, company_names

    b_items = getattr(bundle, "items", None)
    iterable = b_items.all() if hasattr(b_items, "all") else (b_items or [])
    for bi in iterable:
        prod = getattr(bi, "product", None)
        if not prod:
            continue
        comp = getattr(prod, "company", None)
        cid = getattr(comp, "id", None)
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        ids.append(cid)
        company_names.append(getattr(comp, "name", None))
        owner_names.append(_company_owner_display_name(comp))
    return ids, owner_names, company_names


def _owner_id_name_from_company(company):
    if not company:
        return None, None, None
    owner = getattr(company, "owner", None)
    pid = getattr(owner, "id", None)
    pname = (
        getattr(owner, "public_display_name", None)
        or getattr(owner, "full_name", None)
        or (" ".join(filter(None, [getattr(owner, "first_name", None), getattr(owner, "last_name", None)])) or None)
        or getattr(owner, "username", None)
        or getattr(owner, "email", None)
    )
    cname = getattr(company, "name", None)
    return pid, pname, cname


def _producers_from_bundle_instance(bundle):
    if not bundle:
        return ([], [])
    ids, names = [], []
    seen = set()
    for bi in bundle.items.all():
        prod = getattr(bi, "product", None)
        comp = getattr(prod, "company", None)
        pid, pname, _ = _owner_id_name_from_company(comp)
        if pid is None or pid in seen:
            continue
        seen.add(pid)
        ids.append(pid)
        names.append(pname)
    return ids, names



def _company_ids(user) -> List[int]:
    """FR: IDs des entreprises actives du producteur."""
    return list(
        Company.objects.filter(owner=user, is_active=True).values_list("id", flat=True)
    )


def _date_range(request) -> Tuple[Any, Any]:
    """FR: ?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD -> (date_from, date_to)."""
    fmt = "%Y-%m-%d"
    df = request.GET.get("date_from")
    dt = request.GET.get("date_to")
    try:
        df = datetime.strptime(df, fmt).date() if df else None
    except Exception:
        df = None
    try:
        dt = datetime.strptime(dt, fmt).date() if dt else None
    except Exception:
        dt = None
    return df, dt


def _orders_for_producer(user, date_from=None, date_to=None):
    """
    FR: Commandes liées au producteur:
    - Statut dans VALID_STATUSES
    - Au moins un item dont le bundle contient un produit de ses entreprises
    - Filtrage optionnel par dates
    """
    qs = Order.objects.filter(status__in=VALID_STATUSES)
    co_ids = _company_ids(user)
    if co_ids:
        qs = qs.filter(items__bundle__items__product__company_id__in=co_ids).distinct()
    else:
        qs = qs.none()
    if date_from:
        qs = qs.filter(created_at__date__gte=date_from)
    if date_to:
        qs = qs.filter(created_at__date__lte=date_to)
    return qs


def _bucket(dt, bucket="week") -> str:
    """FR: Regroupe une date en jour (YYYY-MM-DD), semaine ISO (YYYY-Www) ou mois (YYYY-MM)."""
    b = (bucket or "week").lower()
    if b == "day":
        try:
            return dt.date().isoformat()
        except Exception:
            return dt.strftime("%Y-%m-%d")
    if b == "month":
        return dt.strftime("%Y-%m")
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def _units_by_order(orders_qs) -> Dict[int, int]:
    """FR: {order_id: unités} via OrderItem.quantity."""
    items = (
        OrderItem.objects.filter(order__in=orders_qs)
        .values("order_id")
        .annotate(units=Sum("quantity"))
    )
    return {r["order_id"]: int(r["units"] or 0) for r in items}


def _pagination(request, default_limit=100) -> Tuple[int, int]:
    """FR: limit/offset (limit par défaut: 100, max 500)."""
    try:
        limit = max(1, min(500, int(request.GET.get("limit", default_limit))))
    except Exception:
        limit = default_limit
    try:
        offset = max(0, int(request.GET.get("offset", 0)))
    except Exception:
        offset = 0
    return limit, offset


def _sort_params(request, default_sort_by: str, allowed: List[str]) -> Tuple[str, str]:
    """FR: sort_by/sort_dir sécurisés."""
    sort_by = request.GET.get("sort_by", default_sort_by)
    sort_dir = request.GET.get("sort_dir", "desc")
    if sort_by not in allowed:
        sort_by = default_sort_by
    if sort_dir not in ("asc", "desc"):
        sort_dir = "desc"
    return sort_by, sort_dir


# ------------------------------------------------------------
# Extraction producteurs (FR)
# ------------------------------------------------------------

def _producers_from_snapshot(snapshot: Optional[dict]) -> Tuple[List[int], List[str]]:
    """FR: Extrait (ids, noms) producteurs depuis bundle_snapshot.products[]."""
    snap = snapshot or {}
    prods = snap.get("products") or []
    ids: List[int] = []
    names: List[str] = []
    seen = set()
    for p in prods:
        cid = p.get("company_id")
        cname = p.get("company_name")
        if cid is None or cid in seen:
            continue
        seen.add(cid)
        ids.append(cid)
        names.append(cname or f"Company {cid}")
    return ids, names



def _attach_producers_to_order_rows(order_ids: List[int]) -> Dict[int, Dict[str, List]]:
    """
    FR: Construit un index {order_id: {producer_ids, producer_names}} à partir des snapshots des items.
    On lit uniquement (order_id, bundle_snapshot) pour éviter les N+1.
    """
    idx: Dict[int, Dict[str, List]] = {}
    rows = OrderItem.objects.filter(order_id__in=order_ids).values("order_id", "bundle_snapshot")
    cache: Dict[int, Tuple[List[int], List[str]]] = {}
    for r in rows:
        oid = r["order_id"]
        snap = r["bundle_snapshot"] or {}
        pids, pnames = _producers_from_snapshot(snap)
        if not pids:
            pass
        cur = idx.setdefault(oid, {"producer_ids": [], "producer_names": []})
        for cid, cname in zip(pids, pnames):
            if cid not in cache.get(oid, ([], []))[0]:
                cur["producer_ids"].append(cid)
                cur["producer_names"].append(cname)
        cache[oid] = (cur["producer_ids"], cur["producer_names"])
    return idx


def _iter_snapshot_products(oi: OrderItem):
    """
    FR: Itère les produits d'un OrderItem.
    - Utilise le snapshot s'il est enrichi (category_id/name)
    - Sinon, retombe sur la BD: ProductBundleItem -> Product -> catalog_entry.category
    Retourne (product_id, title, category_id, category_name, per_bundle_qty)
    """
    snap = getattr(oi, "bundle_snapshot", None) or {}
    prods = snap.get("products") or []
    has_cat = any(("category_id" in p or "category_name" in p) for p in prods)

    if has_cat:
        for p in prods:
            pid = p.get("product_id")
            if pid is None:
                continue
            title = p.get("product_title") or f"Produit {pid}"
            pbq = int(p.get("per_bundle_quantity", 1))
            yield (pid, title, p.get("category_id"), p.get("category_name"), pbq)
        return

    bundle_id = snap.get("id") or getattr(oi, "bundle_id", None)
    if not bundle_id:
        for p in prods:
            pid = p.get("product_id")
            if pid is None:
                continue
            title = p.get("product_title") or f"Produit {pid}"
            pbq = int(p.get("per_bundle_quantity", 1))
            yield (pid, title, None, None, pbq)
        return

    bitems = (
        ProductBundleItem.objects
        .select_related("product__catalog_entry__category")
        .filter(bundle_id=bundle_id, is_active=True)
    )
    pbq_from_snapshot = {p.get("product_id"): int(p.get("per_bundle_quantity", 1))
                         for p in prods if p.get("product_id")}
    for bi in bitems:
        pid = bi.product_id
        title = getattr(bi.product, "title", f"Produit {pid}")
        cat = getattr(getattr(bi.product, "catalog_entry", None), "category", None)
        cat_id = getattr(cat, "id", None)
        cat_name = getattr(cat, "label", None)
        pbq = pbq_from_snapshot.get(pid, int(getattr(bi, "quantity", 1) or 1))
        yield (pid, title, cat_id, cat_name, pbq)


def _category_key(cat_id, cat_name):
    """FR: Clé normalisée pour catégorie."""
    if cat_id is None and not cat_name:
        return ("NA", "Sans catégorie")
    return (cat_id or "NA", cat_name or "Sans catégorie")





# ---------- helpers reviews ----------

def _orderitems_for_producer(user, date_from=None, date_to=None):
    co_ids = _company_ids(user)
    qs = (
        OrderItem.objects
        .filter(order__status__in=VALID_STATUSES)
        .annotate()
    )
    if co_ids:
        qs = qs.filter(bundle__items__product__company_id__in=co_ids)
    if date_from:
        qs = qs.filter(order__created_at__date__gte=date_from)
    if date_to:
        qs = qs.filter(order__created_at__date__lte=date_to)
    return qs.select_related("order", "bundle")



_STOPWORDS_FR = {"le","la","les","de","des","du","un","une","et","ou","au","aux","en","dans","sur",
    "avec","par","pour","pas","ne","que","qui","quoi","ce","cet","cette","ces","je","tu","il","elle",
    "nous","vous","ils","elles","on","y","a","est","sont","été","très","plus","moins","se","sa","son",
    "ses","leurs","leur","mes","tes","vos","d","l","m","t","n"}
_STOPWORDS_ES = {"el","la","los","las","de","del","y","o","en","con","por","para","que","es","son","un","una","lo","al","se"}
_STOPWORDS_EN = {"the","a","an","and","or","in","on","for","to","of","is","are","be","was","were","it","this","that","with"}

def _tokenize(text: str, lang: str) -> List[str]:
    text = re.sub(r"[^\wÀ-ÖØ-öø-ÿ'-]+", " ", text.lower())
    tokens = [t.strip("-'") for t in text.split() if t.strip("-'")]
    stop = _STOPWORDS_FR if lang == "fr" else _STOPWORDS_ES if lang == "es" else _STOPWORDS_EN
    return [t for t in tokens if t not in stop and len(t) >= 3 and not t.isdigit()]




# ============================================================
# 1) Ventes — séries + détails commandes
# ============================================================

class SalesTimeseriesView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def _anchor(self, dt, bucket: str) -> str:
        return self.bucket_anchor_date(dt, bucket)

    def _period_key(self, dt, bucket: str) -> str:
        return _bucket(dt, bucket)

    def _user_display(self, u):
        if not u:
            return None
        return (
            getattr(u, "public_display_name", None)
            or " ".join(x for x in [(getattr(u, "first_name", "") or "").strip(),
                                    (getattr(u, "last_name", "") or "").strip()] if x)
            or getattr(u, "email", None)
        )

    def _company_owner_name(self, company):
        if not company:
            return None
        owner = getattr(company, "owner", None)
        return self._user_display(owner)

    def get(self, request, *args, **kwargs):
        self.initialize_scope(request, **kwargs)

        bucket = (request.GET.get("bucket") or "week").lower()
        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="created_at",
            allowed=["created_at", "status", "line_total", "unit_price", "quantity"],
        )
        reverse = (sort_dir == "desc")

        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        orders = (
            self.get_orders(request, date_from, date_to)
            .select_related("user")
            .prefetch_related(
                Prefetch(
                    "items",
                    queryset=OrderItem.objects.select_related("bundle").only(
                        "id", "order_id", "quantity", "total_price",
                        "bundle_id", "bundle_snapshot"
                    ),
                ),
                "items__bundle__items",
                "items__bundle__items__product",
                "items__bundle__items__product__company",
                "items__bundle__items__product__company__owner",
            )
            .only(
                "id", "order_code", "created_at", "status", "total_price",
                "user__id", "user__public_display_name", "user__first_name",
                "user__last_name", "user__email",
            )
            .order_by("created_at")
        )

        series_map = defaultdict(lambda: {"revenue": 0.0, "orders": 0, "units": 0})
        anchor_date = {}
        total_revenue = 0.0
        total_orders = 0
        total_units = 0

        rows_all = []

        for o in orders:
            dt = getattr(o, "created_at", None)
            period = self._period_key(dt, bucket) if dt else "unknown"
            if period not in anchor_date and dt:
                anchor_date[period] = self._anchor(dt, bucket)

            items_rel = getattr(o, "items", None)
            iterable = items_rel.all() if hasattr(items_rel, "all") else (items_rel or [])

            order_units = 0
            order_item_rev = 0.0
            user = getattr(o, "user", None)

            for it in iterable:
                if allowed_company_ids is not None:
                    snapshot = getattr(it, "bundle_snapshot", None) or {}
                    snapshot_cid = snapshot.get("company_id")
                    if snapshot_cid and snapshot_cid in allowed_company_ids:
                        pass
                    else:
                        b = getattr(it, "bundle", None)
                        b_items = getattr(b, "items", None)
                        b_iter = b_items.all() if hasattr(b_items, "all") else (b_items or [])
                        has_allowed = False
                        for bi in b_iter:
                            prod = getattr(bi, "product", None)
                            cid = getattr(getattr(prod, "company", None), "id", None) if prod else None
                            if cid in allowed_company_ids:
                                has_allowed = True
                                break
                        if not has_allowed:
                            continue

                q = int(getattr(it, "quantity", 0) or 0)
                lt = float(getattr(it, "total_price", 0) or 0.0)
                order_units += q
                order_item_rev += lt
                total_revenue += lt

                snap = getattr(it, "bundle_snapshot", None) or {}
                bundle = getattr(it, "bundle", None)

                company_id = None
                company_name = None
                producer_id = None
                producer_name = None

                if bundle:
                    b_items = getattr(bundle, "items", None)
                    b_iter = b_items.all() if hasattr(b_items, "all") else (b_items or [])
                    for bi in b_iter:
                        prod = getattr(bi, "product", None)
                        comp = getattr(prod, "company", None)
                        if not comp:
                            continue
                        cid = getattr(comp, "id", None)
                        if allowed_company_ids is not None and cid not in allowed_company_ids:
                            continue
                        company_id = cid
                        company_name = getattr(comp, "name", None)
                        owner = getattr(comp, "owner", None)
                        producer_id = getattr(owner, "id", None) if owner else None
                        from .analytics_endpoints import _user_display_name as _udn
                        producer_name = _udn(owner) if owner else None
                        break

                if company_id is None:
                    cid = snap.get("company_id")
                    cname = snap.get("company_name")
                    if cid is not None and (allowed_company_ids is None or cid in allowed_company_ids):
                        company_id = cid
                        company_name = cname

                unit_price = (lt / max(1, q)) if q else lt
                rows_all.append({
                    "order_id": getattr(o, "id", None),
                    "order_code": getattr(o, "order_code", None),
                    "created_at": dt.isoformat() if dt else None,
                    "status": getattr(o, "status", None),
                    "item_id": getattr(it, "id", None),
                    "quantity": q,
                    "unit_price": round(float(unit_price), 2),
                    "line_total": round(float(lt), 2),
                    "bundle_id": getattr(it, "bundle_id", None),
                    "bundle_title": (snap.get("title") if isinstance(snap, dict) else None) or
                                    (getattr(getattr(it, "bundle", None), "title", None)),
                    "producer_id": producer_id,
                    "producer_name": producer_name,
                    "company_id": company_id,
                    "company_name": company_name,
                    "user_name": self._user_display(user),
                })

            series_map[period]["revenue"] += order_item_rev
            series_map[period]["units"] += order_units
            if order_units > 0:
                series_map[period]["orders"] += 1
                total_orders += 1

            total_units += order_units

        if sort_by in {"created_at", "status"}:
            rows_all.sort(key=lambda r: (r.get(sort_by) or ""), reverse=reverse)
        else:
            rows_all.sort(key=lambda r: (r.get(sort_by) or 0), reverse=reverse)

        rows_totals = {
            "revenue": round(sum(float(r.get("line_total") or 0.0) for r in rows_all), 2),
            "units": int(sum(int(r.get("quantity") or 0) for r in rows_all)),
            "orders": len({r.get("order_id") for r in rows_all if r.get("order_id") is not None}),
        }

        count = len(rows_all)
        rows = rows_all[offset: offset + limit]

        series = []
        for p in sorted(series_map.keys()):
            v = series_map[p]
            series.append({
                "period": p,
                "date": anchor_date.get(p),
                "revenue": round(v["revenue"], 2),
                "orders": int(v["orders"]),
                "units": int(v["units"]),
            })

        summary = {
            "revenue": round(total_revenue, 2),
            "orders": total_orders,
            "avg_order_value": float((total_revenue / total_orders) if total_orders else 0.0),
            "units": total_units,
        }

        return Response({
            "summary": summary,
            "series": series,
            "rows": rows,
            "rows_totals": rows_totals,
            "meta": {"count": count, "limit": limit, "offset": offset},
        })



# ============================================================
# 2) Commandes — deep
# ============================================================

class OrdersDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="created_at",
            allowed=["created_at", "total_price", "subtotal", "shipping_cost", "status"],
        )
        status_filter = request.GET.get("status")
        expand_all = request.GET.get("expand") == "*"
        include = set((request.GET.get("include") or "").split(",")) if request.GET.get("include") else set()

        orders = self.get_orders(request, date_from, date_to)
        if status_filter:
            orders = orders.filter(status=status_filter)

        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        if allowed_company_ids is not None:
            items_queryset = (
                OrderItem.objects
                .filter(bundle__items__product__company_id__in=allowed_company_ids)
                .distinct()
                .only("id", "quantity", "total_price", "bundle_snapshot", "bundle_id")
                .select_related("bundle")
            )
        else:
            items_queryset = (
                OrderItem.objects
                .only("id", "quantity", "total_price", "bundle_snapshot", "bundle_id")
                .select_related("bundle")
            )

        prefetch_items = Prefetch("items", queryset=items_queryset)

        orders = (
            orders
            .select_related(
                "shipping_address__city__department__region",
                "billing_address__city__department__region",
            )
            .prefetch_related(
                prefetch_items,
                "items__bundle__items",
                "items__bundle__items__product",
                "items__bundle__items__product__company",
                "items__bundle__items__product__company__owner",
            )
        )

        order_by_expr = ("-" if sort_dir == "desc" else "") + sort_by
        count = orders.count()
        rows_qs = orders.order_by(order_by_expr)[offset: offset + limit]

        serializer = OrderDeepSerializer(
            rows_qs,
            many=True,
            context={"include_products_snapshot": (expand_all or "items_products_light" in include)},
        )
        rows = serializer.data

        if not expand_all and not include:
            for r in rows:
                r.pop("payments", None)
                r.pop("shipping_address", None)
                r.pop("billing_address", None)

        if include:
            for r in rows:
                if "items" not in include:
                    r.pop("items", None)
                if "payments" not in include:
                    r.pop("payments", None)
                if "address" not in include:
                    r.pop("shipping_address", None)
                    r.pop("billing_address", None)

        sums = orders.aggregate(
            revenue=Sum("total_price"),
            subtotal=Sum("subtotal"),
            shipping=Sum("shipping_cost"),
        )
        by_status = orders.values("status").annotate(count=Count("id")).order_by()
        summary = {
            "orders": count,
            "revenue": float(sums["revenue"] or 0),
            "subtotal": float(sums["subtotal"] or 0),
            "shipping": float(sums["shipping"] or 0),
            "aov": float((sums["revenue"] or 0) / count) if count else 0.0,
            "by_status": {r["status"]: r["count"] for r in by_status},
        }

        return Response({"summary": summary, "rows": rows, "meta": {"count": count, "limit": limit, "offset": offset}})
    


# ============================================================
# 3) Clients — résumé + lignes
# ============================================================

# ============================================================
# 3) Clients — résumé + lignes
# ============================================================

class CustomersDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        from django.contrib.auth import get_user_model
        from django.db.models import Prefetch, Sum, Count, Min, Max
        from django.utils import timezone
        User = get_user_model()

        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="created_at",
            allowed=[
                "created_at",
                "amount",
                "user_name",
                "order_id",
                "order_status",
            ],
        )
        reverse = (sort_dir == "desc")
        include = set((request.GET.get("include") or "").split(",")) if request.GET.get("include") else set()

        orders = self.get_orders(request, date_from, date_to)
        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        items_qs = (
            OrderItem.objects
            .filter(order__in=orders)
            .select_related("order", "order__user", "bundle")
            .prefetch_related(
                Prefetch(
                    "bundle__items",
                    queryset=ProductBundleItem.objects.select_related(
                        "product",
                        "product__company__owner",
                    )
                )
            )
            .only(
                "id", "order_id", "quantity", "total_price",
                "bundle_id", "bundle_snapshot",
                "order__order_code",
                "order__created_at", "order__status",
                "order__user__id", "order__user__public_display_name",
                "order__user__first_name", "order__user__last_name", "order__user__email",
            )
            .order_by("-order__created_at", "-id")
        )
        if allowed_company_ids is not None:
            items_qs = items_qs.filter(bundle__items__product__company_id__in=allowed_company_ids).distinct()

        total_items_count = items_qs.count()
        page_items = list(items_qs[offset: offset + limit])

        def _user_display(u):
            if not u:
                return None
            return (
                getattr(u, "public_display_name", None)
                or f"{(getattr(u, 'first_name', '') or '').strip()} {(getattr(u, 'last_name', '') or '').strip()}".strip()
                or getattr(u, "email", None)
                or getattr(u, "username", None)
            )

        orders = (
            orders
            .select_related(
                "shipping_address__city__department__region",
                "billing_address__city__department__region",
            )
            .prefetch_related(
                "items",
                "items__bundle__items",
                "items__bundle__items__product",
                "items__bundle__items__product__company",
                "items__bundle__items__product__company__owner",
            )
        )
        order_by_expr = ("-" if reverse else "") + sort_by
        count = orders.count()
        rows_qs = orders.order_by(order_by_expr)[offset: offset + limit]

        serializer = OrderDeepSerializer(
            rows_qs,
            many=True,
            context={"include_products_snapshot": ("items_products_light" in include)},
        )
        rows = serializer.data

        if not include:
            for r in rows:
                r.pop("payments", None)
                r.pop("shipping_address", None)
                r.pop("billing_address", None)
                r.pop("items", None)
        else:
            for r in rows:
                if "items" not in include:
                    r.pop("items", None)
                if "payments" not in include:
                    r.pop("payments", None)
                if "address" not in include:
                    r.pop("shipping_address", None)
                    r.pop("billing_address", None)

        sums = orders.aggregate(
            revenue=Sum("total_price"),
            subtotal=Sum("subtotal"),
            shipping=Sum("shipping_cost"),
        )
        by_status = orders.values("status").annotate(count=Count("id")).order_by()

        summary = {
            "orders": count,
            "revenue": float(sums["revenue"] or 0),
            "subtotal": float(sums["subtotal"] or 0),
            "shipping": float(sums["shipping"] or 0),
            "aov": float((sums["revenue"] or 0) / count) if count else 0.0,
            "by_status": {r["status"]: r["count"] for r in by_status},
        }

        try:
            has_type_field = any(f.name == "type" for f in User._meta.get_fields())
        except Exception:
            has_type_field = False

        if has_type_field:
            try:
                total_producers_all = User.objects.filter(type="producer").count()
                total_customers_all = User.objects.exclude(type="producer").count()
            except Exception:
                total_producers_all = 0
                total_customers_all = 0
        else:
            try:
                from .models import Company
                producer_ids_qs = Company.objects.values_list("owner_id", flat=True).distinct()
                total_producers_all = User.objects.filter(id__in=producer_ids_qs).distinct().count()
                total_customers_all = User.objects.exclude(id__in=producer_ids_qs).count()
            except Exception:
                total_producers_all = 0
                total_customers_all = 0

        summary.update({
            "total_customers_all": int(total_customers_all),
            "total_producers_all": int(total_producers_all),
        })

        unique_customers = orders.values("user_id").distinct().count()
        summary["unique_customers"] = unique_customers

        order_ids_allowed = list(items_qs.values_list("order_id", flat=True).distinct())
        orders_allowed_qs = orders.filter(id__in=order_ids_allowed) if order_ids_allowed else orders.none()

        agg = (
            orders_allowed_qs.values("user_id")
            .annotate(
                orders=Count("id"),
                first_order=Min("created_at"),
                last_order=Max("created_at"),
            )
        )

        seg_counts = {"loyal": 0, "new": 0, "occasional": 0}
        for r in agg:
            n = int(r["orders"] or 0)
            if n <= 1:
                seg = "new"
            elif n >= 3:
                seg = "loyal"
            else:
                seg = "occasional"
            seg_counts[seg] += 1

        summary["segments"] = seg_counts

        return Response({"summary": summary, "rows": rows, "meta": {"count": count, "limit": limit, "offset": offset}})




# ============================================================
# 4) Paniers abandonnés — deep 
# ============================================================

class CartsAbandonedDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def _producer_from_bundle_instance(self, bundle):
        if not bundle:
            return None, None
        b_items = getattr(bundle, "items", None)
        iterable = b_items.all() if hasattr(b_items, "all") else (b_items or [])
        for bi in iterable:
            prod = getattr(bi, "product", None)
            if not prod:
                continue
            comp = getattr(prod, "company", None)
            if not comp:
                continue
            owner = getattr(comp, "owner", None)
            if not owner:
                continue
            pid = getattr(owner, "id", None)
            pname = (
                getattr(owner, "public_display_name", None)
                or " ".join(x for x in [
                    (getattr(owner, "first_name", "") or "").strip(),
                    (getattr(owner, "last_name", "") or "").strip()
                ] if x) or getattr(owner, "email", None)
            )
            if pid is not None or pname:
                return pid, pname
        return None, None

    def _companies_from_bundle_items(self, bundle):
        ids, names, seen = [], [], set()
        if not bundle:
            return ids, names
        b_items = getattr(bundle, "items", None)
        iterable = b_items.all() if hasattr(b_items, "all") else (b_items or [])
        for bi in iterable:
            prod = getattr(bi, "product", None)
            if not prod:
                continue
            comp = getattr(prod, "company", None)
            cid = getattr(comp, "id", None)
            cname = getattr(comp, "name", None)
            key = cid if cid is not None else cname
            if key in seen:
                continue
            seen.add(key)
            if cid is not None:
                ids.append(cid)
            if cname:
                names.append(cname)
        return ids, names

    def _category_payload_from_product(self, prod):
        cat = getattr(getattr(prod, "catalog_entry", None), "category", None)
        if not cat:
            return None
        return {
            "id": getattr(cat, "id", None),
            "code": getattr(cat, "code", None),
            "label": getattr(cat, "label", None),
        }

    def get(self, request, **kwargs):
        from django.db.models import Prefetch
        from collections import defaultdict
        from django.contrib.auth import get_user_model

        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="updated_at",
            allowed=["updated_at", "items_qty"],
        )
        reverse = (sort_dir == "desc")

        granularity = (request.GET.get("granularity") or "cart").lower()

        allowed_company_ids = None
        if not self.is_admin_scope:
            allowed_company_ids = set(_company_ids(request.user))

        carts = Cart.objects.filter(is_active=True)

        if date_from:
            carts = carts.filter(updated_at__date__gte=date_from)
        if date_to:
            carts = carts.filter(updated_at__date__lte=date_to)

        carts = list(carts.only("id", "user_id", "updated_at"))
        cart_meta = {c.id: {"user_id": c.user_id, "updated_at": c.updated_at} for c in carts}
        cart_ids = [c.id for c in carts]

        if not cart_ids:
            empty_summary = {
                "users_no_purchase": 0,
                "active_carts": 0,
                "avg_cart_qty": 0.0,
                "top_abandoned_products": [],
            }
            return Response({"summary": empty_summary, "rows": [], "meta": {"count": 0, "limit": limit, "offset": offset}})

        # Prefetch: bundle -> items -> product -> company.owner + category
        cart_items_qs = (
            CartItem.objects
            .filter(cart_id__in=cart_ids)
            .select_related("bundle")
            .prefetch_related(
                Prefetch(
                    "bundle__items",
                    queryset=ProductBundleItem.objects.select_related(
                        "product",
                        "product__company__owner",
                        "product__catalog_entry__category",
                    )
                ),
            )
        )

        items_by_cart = defaultdict(list)
        for it in cart_items_qs:
            items_by_cart[it.cart_id].append(it)

        def _qty_sum(cid):
            return sum(int(getattr(it, "quantity", 0) or 0) for it in items_by_cart.get(cid, []))
        key_fn = (lambda cid: cart_meta[cid]["updated_at"]) if sort_by == "updated_at" else _qty_sum
        ordered_ids = sorted(items_by_cart.keys(), key=key_fn, reverse=reverse)

        rows_build = []
        top_products_counter = defaultdict(int)
        product_titles = {}
        for cid in ordered_ids:
            meta = cart_meta[cid]
            items_payload, total_qty = [], 0
            cart_amount = 0.0

            for it in items_by_cart.get(cid, []):
                q = int(getattr(it, "quantity", 0) or 0)
                b = getattr(it, "bundle", None)

                b_company_ids, co_names = self._companies_from_bundle_items(b)

                if allowed_company_ids is not None:
                    if not any((bid in allowed_company_ids) for bid in b_company_ids):
                        continue 

                unit_price = None
                for attr in ("discounted_price", "price", "current_price", "original_price"):
                    val = getattr(b, attr, None) if b is not None else None
                    if val not in (None, ""):
                        try:
                            unit_price = float(val)
                            break
                        except Exception:
                            pass
                unit_price = unit_price or 0.0
                line_total = round(unit_price * q, 2)

                total_qty += q
                cart_amount += line_total

                pid, pname = self._producer_from_bundle_instance(b)

                bundle_payload = {
                    "bundle_id": getattr(b, "id", None),
                    "title": getattr(b, "title", None) or (f"Bundle {getattr(b, 'id', '')}" if b else None),
                    "stock": int(getattr(b, "stock", 0) or 0) if b else 0,
                    "products": [],
                    "producer_ids": [pid] if pid is not None else [],
                    "producer_names": [pname] if pname else [],
                    "company_names": co_names,
                }

                if b:
                    b_items = getattr(b, "items", None)
                    iterable = b_items.all() if hasattr(b_items, "all") else (b_items or [])
                    for bi in iterable:
                        prod = getattr(bi, "product", None)
                        if prod:
                            p_id = getattr(prod, "id", None)
                            product_titles[p_id] = getattr(prod, "title", None)
                            top_products_counter[p_id] += 1
                            category_payload = self._category_payload_from_product(prod)
                        else:
                            p_id = None
                            category_payload = None
                        bundle_payload["products"].append({
                            "product_id": p_id,
                            "title": getattr(prod, "title", None) if prod else None,
                            "per_bundle_quantity": int(getattr(bi, "quantity", 1) or 1),
                            "best_before_date": (
                                getattr(bi, "best_before_date", None).isoformat()
                                if getattr(bi, "best_before_date", None) else None
                            ),
                            "category": category_payload,
                        })

                items_payload.append({
                    "cart_item_id": getattr(it, "id", None),
                    "quantity": q,
                    "unit_price": round(unit_price, 2),
                    "line_total": line_total,
                    "bundle": bundle_payload,
                })

            if not items_payload:
                continue

            rows_build.append({
                "cart_id": cid,
                "user_id": meta["user_id"],
                "updated_at": meta["updated_at"],
                "items_qty": total_qty,
                "amount": round(cart_amount, 2),
                "items": items_payload,
            })

        active_carts = len(rows_build)
        avg_cart_qty = (sum(r["items_qty"] for r in rows_build) / active_carts) if active_carts else 0.0

        top_abandoned_products = []
        for pid, cnt in sorted(top_products_counter.items(), key=lambda kv: kv[1], reverse=True)[:10]:
            if pid is None:
                continue
            top_abandoned_products.append({
                "product_id": pid,
                "label": product_titles.get(pid),
                "count": cnt
            })

        User = get_user_model()
        uid_list = [r["user_id"] for r in rows_build if r.get("user_id")]
        users = {
            u.id: u
            for u in User.objects.filter(id__in=uid_list).only("id", "public_display_name", "first_name", "last_name", "email")
        }

        def _display(u):
            if not u:
                return None
            return (
                getattr(u, "public_display_name", None)
                or (" ".join(x for x in [(u.first_name or "").strip(), (u.last_name or "").strip()] if x) or None)
                or getattr(u, "email", None)
                or f"User {u.id}"
            )

        for r in rows_build:
            r["user_name"] = _display(users.get(r.get("user_id")))

        if granularity == "item":
            items_rows = []
            for r in rows_build:
                base = {
                    "cart_id": r["cart_id"],
                    "user_id": r.get("user_id"),
                    "user_name": r.get("user_name"),
                    "updated_at": r.get("updated_at"),
                }
                for it in (r.get("items") or []):
                    items_rows.append({
                        **base,
                        "cart_item_id": it.get("cart_item_id"),
                        "quantity": it.get("quantity") or 0,
                        "unit_price": it.get("unit_price"),
                        "line_total": it.get("line_total"),
                        "bundle": it.get("bundle"),
                    })

            # Orden simple por fecha o cantidad
            sort_by_item = request.GET.get("sort_by") or "updated_at"
            reverse_item = (request.GET.get("sort_dir") or "desc").lower() == "desc"
            if sort_by_item not in {"updated_at", "quantity", "line_total"}:
                sort_by_item = "updated_at"
            items_rows.sort(key=lambda x: (x.get(sort_by_item) or 0), reverse=reverse_item)

            count_items = len(items_rows)
            page_items = items_rows[offset: offset + limit]

            try:
                from .analytics_serializers import CartsAbandonedItemRowSerializer
                rows_out = CartsAbandonedItemRowSerializer(page_items, many=True).data
            except Exception:
                rows_out = page_items

            return Response({
                "summary": {
                    "users_no_purchase": 0,
                    "active_carts": active_carts,
                    "avg_cart_qty": avg_cart_qty,
                    "top_abandoned_products": top_abandoned_products,
                },
                "rows": rows_out,
                "meta": {"count": count_items, "limit": limit, "offset": offset},
            })

        # --- Respuesta por carrito (default) ---
        from .analytics_serializers import CartsAbandonedRowSerializer
        count_carts = len(rows_build)
        return Response({
            "summary": {
                "users_no_purchase": 0,
                "active_carts": active_carts,
                "avg_cart_qty": avg_cart_qty,
                "top_abandoned_products": top_abandoned_products,
            },
            "rows": CartsAbandonedRowSerializer(rows_build, many=True).data,
            "meta": {"count": count_carts, "limit": limit, "offset": offset},
        })



# ============================================================
# 5) Catalogue — deep 
# ============================================================


class CatalogDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        low_threshold = int(request.GET.get("low_threshold", "5"))
        dlc_days = int(request.GET.get("dlc_days", "7"))

        # pagination/sort products and bundles independently
        p_limit, p_offset = _pagination(request)
        p_sort_by, p_sort_dir = _sort_params(request, default_sort_by="stock", allowed=["stock", "sold", "title"])
        b_limit, b_offset = p_limit, p_offset
        b_sort_by, b_sort_dir = _sort_params(request, default_sort_by="stock", allowed=["stock", "sold", "title"])

        if self.is_admin_scope:
            prods_qs = (
                Product.objects
                .select_related("catalog_entry__category", "company", "company__owner")
            )
            bundles_qs = (
                ProductBundle.objects
                .prefetch_related(
                    "items",
                    "items__product",
                    "items__product__catalog_entry__category",
                    "items__product__company",
                    "items__product__company__owner",
                )
            )
            dlc_filter = {}
        else:
            co_ids = _company_ids(request.user)
            prods_qs = (
                Product.objects
                .filter(company_id__in=co_ids)
                .select_related("catalog_entry__category", "company", "company__owner")
            )
            bundles_qs = (
                ProductBundle.objects
                .filter(items__product__company_id__in=co_ids)
                .distinct()
                .prefetch_related(
                    "items",
                    "items__product",
                    "items__product__catalog_entry__category",
                    "items__product__company",
                    "items__product__company__owner",
                )
            )
            dlc_filter = {"product__company_id__in": co_ids}

        # Products
        products, low_stock = [], []
        for p in prods_qs:
            cat = getattr(getattr(p, "catalog_entry", None), "category", None)
            company = getattr(p, "company", None)
            owner_name = _company_owner_display_name(company)
            company_name = getattr(company, "name", None)

            row = {
                "product_id": p.id,
                "title": p.title,
                "sku": getattr(p, "sku", None),
                "stock": int(p.stock or 0),
                "sold": int(getattr(p, "sold_units", 0) or 0),
                "category": {"id": getattr(cat, "id", None), "name": getattr(cat, "label", None)},
                "producer_ids": [getattr(p, "company_id", None)] if getattr(p, "company_id", None) else [],
                "producer_names": [owner_name] if owner_name else [],
                "company_names": [company_name] if company_name else [],
            }
            products.append(row)
            if row["stock"] <= 0:
                low_stock.append({**row, "level": "red"})
            elif row["stock"] <= low_threshold:
                low_stock.append({**row, "level": "yellow"})

        # Bundles
        bundles = []
        for b in bundles_qs:
            pids, owner_names, company_names = _bundle_producers_and_companies(b)
            items_payload = []
            b_items = getattr(b, "items", None)
            iterable = b_items.all() if hasattr(b_items, "all") else (b_items or [])
            for bi in iterable:
                prod = getattr(bi, "product", None)
                cat = getattr(getattr(prod, "catalog_entry", None), "category", None)
                items_payload.append({
                    "product_id": getattr(prod, "id", None),
                    "title": getattr(prod, "title", None),
                    "per_bundle_quantity": int(getattr(bi, "quantity", 1) or 1),
                    "best_before_date": bi.best_before_date.isoformat() if getattr(bi, "best_before_date", None) else None,
                    "category": {"id": getattr(cat, "id", None), "name": getattr(cat, "label", None)},
                })

            bundles.append({
                "bundle_id": b.id,
                "title": getattr(b, "title", f"Bundle {b.id}"),
                "stock": int(getattr(b, "stock", 0) or 0),
                "sold": int(getattr(b, "sold_bundles", 0) or 0),
                "items": items_payload,
                "producer_ids": pids,
                "producer_names": [n for n in owner_names if n],
                "company_names": [n for n in company_names if n],
            })

        # DLC risk
        today = timezone.localdate()
        limit_d = today + timedelta(days=dlc_days)
        dlc_qs = (
            ProductBundleItem.objects
            .filter(best_before_date__isnull=False,
                    best_before_date__lte=limit_d,
                    bundle__stock__gt=0,
                    is_active=True, **dlc_filter)
            .select_related("product", "bundle", "product__company", "product__company__owner")
            .order_by("best_before_date")
        )
        dlc_risk = [
            {
                "bundle_id": it.bundle_id,
                "product_id": it.product_id,
                "product": getattr(it.product, "title", None),
                "best_before_date": it.best_before_date.isoformat(),
                "bundle_stock": int(getattr(it.bundle, "stock", 0)),
                "producer_ids": [getattr(it.product, "company_id", None)] if getattr(it.product, "company_id", None) else [],
                "producer_names": [
                    _company_owner_display_name(getattr(it.product, "company", None))
                ] if getattr(it.product, "company", None) else [],
                "company_names": [
                    getattr(getattr(it.product, "company", None), "name", None)
                ] if getattr(it.product, "company", None) else [],
            }
            for it in dlc_qs
        ]

        return Response({"products": products, "bundles": bundles, "low_stock": low_stock, "dlc_risk": dlc_risk})




# ============================================================
# 6) Santé produits/bundles 
# ============================================================


class ProductsHealthView(AnalyticsScopeMixin, APIView):
    """
    GET /api/<scope>/analytics/products/health/?date_from&date_to&limit&offset&low_stock_threshold

    Response:
    {
      "summary": {
        "products": { "count": int, "zero_stock": int, "low_stock": int },
        "bundles": { "count": int },
        "dlc_en_risque": int
      },
      "rows": {
        "products": {
          "data": [ { product row ... } ],
          "meta": { "count": int, "limit": int, "offset": int }
        },
        "bundles": {
          "data": [ { bundle row ... } ],
          "meta": { "count": int, "limit": int, "offset": int }
        }
      },
      "dlc_risque": []
    }
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        limit_b, offset_b = _pagination(request, default_limit=100)

        try:
            low_threshold = int(request.GET.get("low_stock_threshold", 5))
        except Exception:
            low_threshold = 5

        # Products queryset
        if self.is_admin_scope:
            prods_qs = (
                Product.objects
                .select_related("company", "company__owner", "catalog_entry__category")
                .only(
                    "id",
                    "title",
                    "stock",
                    "sold_units",
                    "company_id",
                    "catalog_entry_id",  # critical to avoid FieldError with select_related
                )
                .order_by("id")
            )
            bundles_qs = (
                ProductBundle.objects
                .prefetch_related(
                    "items",
                    "items__product",
                    "items__product__company",
                    "items__product__company__owner",
                    "items__product__catalog_entry__category",
                )
                .order_by("id")
            )
        else:
            co_ids = _company_ids(request.user)
            prods_qs = (
                Product.objects
                .filter(company_id__in=co_ids)
                .select_related("company", "company__owner", "catalog_entry__category")
                .only(
                    "id",
                    "title",
                    "stock",
                    "sold_units",
                    "company_id",
                    "catalog_entry_id",  # critical to avoid FieldError with select_related
                )
                .order_by("id")
            )
            bundles_qs = (
                ProductBundle.objects
                .filter(items__product__company_id__in=co_ids)
                .distinct()
                .prefetch_related(
                    "items",
                    "items__product",
                    "items__product__company",
                    "items__product__company__owner",
                    "items__product__catalog_entry__category",
                )
                .order_by("id")
            )

        # Build product rows
        products_all: List[Dict[str, Any]] = []
        zero_stock = 0
        low_stock = 0

        for p in prods_qs:
            company = getattr(p, "company", None)
            company_name = getattr(company, "name", None)
            owner = getattr(company, "owner", None)
            owner_name = (
                getattr(owner, "public_display_name", None)
                or " ".join(x for x in [
                    (getattr(owner, "first_name", "") or "").strip(),
                    (getattr(owner, "last_name", "") or "").strip(),
                ] if x)
                or getattr(owner, "username", None)
                or getattr(owner, "email", None)
            )

            stock_val = int(getattr(p, "stock", 0) or 0)
            sold_units = int(getattr(p, "sold_units", 0) or 0)

            if stock_val <= 0:
                level = "red"
                zero_stock += 1
            elif stock_val <= low_threshold:
                level = "yellow"
                low_stock += 1
            else:
                level = "ok"

            cat_dict = _category_from_product(p)

            products_all.append({
                "product_id": p.id,
                "title": getattr(p, "title", None),
                "stock": stock_val,
                "sold": sold_units,
                "level": level,
                "producer_ids": [getattr(company, "id", None)] if company else [],
                "producer_names": [owner_name] if owner_name else [],
                "company_names": [company_name] if company_name else [],
                "category": cat_dict if any(cat_dict.values()) else None,
            })

        products_count = len(products_all)
        products_rows = products_all[offset: offset + limit]

        # Build bundle rows
        bundles_all: List[Dict[str, Any]] = []
        for b in bundles_qs:
            # Aggregate producers/companies from live bundle graph
            producer_ids, producer_owner_names, company_names = [], [], []
            seen = set()
            items_rel = getattr(b, "items", None)
            items_iter = items_rel.all() if hasattr(items_rel, "all") else (items_rel or [])
            rep_prod = None
            for bi in items_iter:
                prod = getattr(bi, "product", None)
                if prod and rep_prod is None:
                    rep_prod = prod
                comp = getattr(prod, "company", None) if prod else None
                if comp:
                    cid = getattr(comp, "id", None)
                    if cid not in seen:
                        seen.add(cid)
                        producer_ids.append(cid)
                        company_names.append(getattr(comp, "name", None))
                        owner = getattr(comp, "owner", None)
                        owner_name = (
                            getattr(owner, "public_display_name", None)
                            or " ".join(x for x in [
                                (getattr(owner, "first_name", "") or "").strip(),
                                (getattr(owner, "last_name", "") or "").strip(),
                            ] if x)
                            or getattr(owner, "username", None)
                            or getattr(owner, "email", None)
                        )
                        producer_owner_names.append(owner_name)

            representative_category = None
            if rep_prod:
                cdict = _category_from_product(rep_prod)
                representative_category = cdict if any(cdict.values()) else None

            bundles_all.append({
                "bundle_id": getattr(b, "id", None),
                "title": getattr(b, "title", None),
                "stock": int(getattr(b, "stock", 0) or 0),
                "sold": int(getattr(b, "sold_bundles", 0) or 0),
                "producer_ids": producer_ids,
                "producer_names": producer_owner_names,
                "company_names": company_names,
                "category": representative_category,
            })

        bundles_count = len(bundles_all)
        bundles_rows = bundles_all[offset_b: offset_b + limit_b]

        summary = {
            "products": {
                "count": products_count,
                "zero_stock": zero_stock,
                "low_stock": low_stock,
            },
            "bundles": {
                "count": bundles_count,
            },
            "dlc_en_risque": 0,
        }

        return Response({
            "summary": summary,
            "rows": {
                "products": {
                    "data": products_rows,
                    "meta": {"count": products_count, "limit": limit, "offset": offset},
                },
                "bundles": {
                    "data": bundles_rows,
                    "meta": {"count": bundles_count, "limit": limit_b, "offset": offset_b},
                },
            },
            "dlc_risque": [],
        })





# ============================================================
# 7) Impact — totaux + lignes 
# ============================================================

class ImpactView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="created_at",
            allowed=["created_at", "avoided_waste_kg", "avoided_co2_kg", "savings_eur"],
        )
        reverse = (sort_dir == "desc")

        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        items_qs = (
            OrderItem.objects
            .only("id", "order_id", "bundle_id", "bundle_snapshot",
                  "order_item_total_avoided_waste_kg",
                  "order_item_total_avoided_co2_kg",
                  "order_item_savings")
            .select_related("bundle")
        )
        if allowed_company_ids is not None:
            items_qs = items_qs.filter(bundle__items__product__company_id__in=allowed_company_ids).distinct()

        orders = (
            self.get_orders(request, date_from, date_to)
            .select_related("user")
            .prefetch_related(
                Prefetch("items", queryset=items_qs),
                "items__bundle__items",
                "items__bundle__items__product",
                "items__bundle__items__product__company",
                "items__bundle__items__product__company__owner",
            )
            .only(
                "id", "order_code", "created_at", "status",
                "order_total_avoided_waste_kg",
                "order_total_avoided_co2_kg",
                "order_total_savings",
                "user__id", "user__public_display_name",
                "user__first_name", "user__last_name", "user__email",
            )
        )

        def _user_display(u):
            if not u:
                return None
            from .analytics_endpoints import _user_display_name as _ud
            return _ud(u)

        rows_all: List[Dict[str, Any]] = []

        for o in orders:
            user = getattr(o, "user", None)
            user_name = _user_display(user)
            items = o.items.all() if hasattr(o.items, "all") else (o.items or [])
            for it in items:
                snap = getattr(it, "bundle_snapshot", None)
                snap = snap if isinstance(snap, dict) else {}

                aw = getattr(it, "order_item_total_avoided_waste_kg", None)
                ac = getattr(it, "order_item_total_avoided_co2_kg", None)
                sv = getattr(it, "order_item_savings", None)
                avoided_waste = float(aw or snap.get("avoided_waste_kg") or 0.0)
                avoided_co2 = float(ac or snap.get("avoided_co2_kg") or 0.0)
                savings_eur = float(sv or 0.0)

                producer_id = None
                producer_name = None
                company_id = None
                company_name = None

                b = getattr(it, "bundle", None)
                if b is not None:
                    items_rel = getattr(b, "items", None)
                    items_iter = items_rel.all() if hasattr(items_rel, "all") else (items_rel or [])
                    for bi in items_iter:
                        prod = getattr(bi, "product", None)
                        comp = getattr(prod, "company", None)
                        cid = getattr(comp, "id", None) if comp else None
                        if comp and (allowed_company_ids is None or cid in allowed_company_ids):
                            owner = getattr(comp, "owner", None)
                            company_id = cid
                            company_name = getattr(comp, "name", None)
                            from .analytics_endpoints import _user_display_name as _udn
                            producer_id = getattr(owner, "id", None) if owner else None
                            producer_name = _udn(owner) if owner else None
                            break

                if company_id is None:
                    cid = snap.get("company_id")
                    cname = snap.get("company_name")
                    if cid is not None and (allowed_company_ids is None or cid in allowed_company_ids):
                        company_id = cid
                        company_name = cname

                bundle_title = getattr(b, "title", None) if b is not None else None
                if not bundle_title:
                    bundle_title = snap.get("title")

                rows_all.append({
                    "order_id": getattr(o, "id", None),
                    "order_code": getattr(o, "order_code", None),
                    "created_at": getattr(o, "created_at", None).isoformat() if getattr(o, "created_at", None) else None,
                    "status": getattr(o, "status", None),

                    "item_id": getattr(it, "id", None),
                    "bundle_id": getattr(it, "bundle_id", None),
                    "bundle_title": bundle_title,

                    "avoided_waste_kg": round(avoided_waste, 3),
                    "avoided_co2_kg": round(avoided_co2, 3),
                    "savings_eur": round(savings_eur, 2),

                    "producer_id": producer_id,
                    "producer_name": producer_name,
                    "company_id": company_id,
                    "company_name": company_name,

                    "user_name": user_name,
                })

        if sort_by == "created_at":
            rows_all.sort(key=lambda r: (r["created_at"] or ""), reverse=reverse)
        else:
            rows_all.sort(key=lambda r: (r.get(sort_by) or 0.0), reverse=reverse)

        count = len(rows_all)
        rows = rows_all[offset: offset + limit]

        agg = orders.aggregate(
            waste=Sum("order_total_avoided_waste_kg"),
            co2=Sum("order_total_avoided_co2_kg"),
            savings=Sum("order_total_savings"),
        )
        summary = {
            "avoided_waste_kg": round(float(agg["waste"] or 0.0), 2),
            "avoided_co2_kg": round(float(agg["co2"] or 0.0), 2),
            "savings_eur": round(float(agg["savings"] or 0.0), 2),
        }

        return Response({
            "summary": summary,
            "rows": rows,
            "meta": {"count": count, "limit": limit, "offset": offset},
        })



# ============================================================
# 8) Paiements — deep (unifiée)
# ============================================================

class PaymentsDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="revenue",
            allowed=["method", "status", "amount", "created_at", "revenue"],
        )
        reverse = (sort_dir == "desc")

        orders = self.get_orders(request, date_from, date_to)
        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        items_qs = (
            OrderItem.objects
            .filter(order__in=orders)
            .select_related("order", "order__user", "bundle")
            .prefetch_related(
                Prefetch(
                    "bundle__items",
                    queryset=ProductBundleItem.objects.select_related(
                        "product",
                        "product__company__owner",
                    )
                )
            )
            .only(
                "id", "order_id", "quantity", "total_price",
                "bundle_id",
                "order__created_at", "order__order_code", "order__status", "order__payment_method_snapshot",
                "order__user__id", "order__user__public_display_name",
                "order__user__first_name", "order__user__last_name",
                "order__user__email",
            )
            .order_by("-order__created_at", "-id")
        )
        if allowed_company_ids is not None:
            items_qs = items_qs.filter(bundle__items__product__company_id__in=allowed_company_ids).distinct()

        total_items_count = items_qs.count()
        page_items = list(items_qs[offset: offset + limit])

        rows_all: List[Dict[str, Any]] = []
        by_method: Dict[str, Dict[str, Any]] = {}

        def _method_key_from_order(order):
            snap = getattr(order, "payment_method_snapshot", None)
            if isinstance(snap, dict):
                return f"{(snap.get('type') or 'unknown')}:{(snap.get('provider') or 'unknown')}"
            return "unknown:unknown"

        def _user_display(u):
            if not u:
                return None
            name = getattr(u, "public_display_name", None)
            if name:
                return name
            fn = (getattr(u, "first_name", "") or "").strip()
            ln = (getattr(u, "last_name", "") or "").strip()
            full = " ".join([p for p in [fn, ln] if p])
            if full:
                return full
            return getattr(u, "email", None)

        for it in page_items:
            o = getattr(it, "order", None)
            created_at = getattr(o, "created_at", None)
            created_iso = created_at.isoformat() if created_at else None
            status = getattr(o, "status", None)
            status_norm = "paid" if status in {"confirmed", "delivered"} else status
            method_key = _method_key_from_order(o)
            user = getattr(o, "user", None)
            user_name = _user_display(user)
            amount = float(getattr(it, "total_price", 0.0) or 0.0)

            b = getattr(it, "bundle", None)
            if b:
                # CORRECT unpack: (ids, owner_names, company_names)
                cids, owner_names, company_names = _bundle_producers_and_companies(b)
                if allowed_company_ids is not None:
                    filt = [
                        (cid, on, cname)
                        for cid, on, cname in zip(cids, owner_names, company_names)
                        if cid in allowed_company_ids
                    ]
                    cids = [x[0] for x in filt]
                    owner_names = [x[1] for x in filt]
                    company_names = [x[2] for x in filt]
            else:
                cids, owner_names, company_names = [], [], []

            rows_all.append({
                "order_id": getattr(it, "order_id", None),
                 "order_code": getattr(getattr(it, "order", None), "order_code", None),
                "order_item_id": it.id,
                "created_at": created_iso,
                "method": method_key,
                "status": status_norm,
                "amount": amount,
                "producer_ids": cids,                           # company IDs
                "producer_names": [n for n in owner_names if n],# owners
                "company_names": [n for n in company_names if n],# company names
                "user_name": user_name,
            })

            agg = by_method.setdefault(method_key, {"count": 0, "revenue": 0.0, "success": 0, "total": 0})
            agg["count"] += 1
            agg["revenue"] += amount
            agg["total"] += 1
            if status_norm in {"paid", "authorized", "succeeded", "confirmed", "delivered"}:
                agg["success"] += 1

        if sort_by in {"method", "status", "created_at"}:
            rows_all.sort(key=lambda x: (x.get(sort_by) or ""), reverse=reverse)
        else:
            rows_all.sort(key=lambda x: (x.get(sort_by, 0)), reverse=reverse)

        by_method_rows = []
        for method_key, m in by_method.items():
            sr = (m["success"] / m["total"]) if m["total"] else 0.0
            by_method_rows.append({
                "method": method_key,
                "count": m["count"],
                "revenue": round(m["revenue"], 2),
                "success_rate": round(sr, 4),
            })

        summary = {
            "methods": len(by_method_rows),
            "revenue": round(sum(x["revenue"] for x in by_method_rows), 2),
            "count": sum(x["count"] for x in by_method_rows),
        }

        return Response({
            "summary": summary,
            "by_method": by_method_rows,
            "rows": rows_all,
            "meta": {"count": total_items_count, "limit": limit, "offset": offset},
        })


# ============================================================
# 9) Cohorts
# ============================================================

class CohortsMonthlyView(AnalyticsScopeMixin, APIView):
    """
    GET /api/(producer|admin)/analytics/cohorts/monthly/?date_from&date_to
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        orders = (
            self.get_orders(request, date_from, date_to)
            .only("id", "user_id", "created_at", "total_price")
        )

        # First order per user (defines cohort start month)
        first_by_user = {}
        for r in (
            orders.values("user_id")
            .annotate(first_order=Min("created_at"))
            .order_by()
        ):
            if r["user_id"] is not None and r["first_order"]:
                fo = r["first_order"]
                first_by_user[r["user_id"]] = fo.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                )

        order_ids = list(orders.values_list("id", flat=True))

        # Scope restriction (producer)
        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        # Per-order company revenue mapping
        order_company_revenue = defaultdict(lambda: defaultdict(float))
        company_name_by_id = {}
        all_company_ids = set()

        # Prefetch bundle graph
        oi_qs = (
            OrderItem.objects
            .filter(order_id__in=order_ids)
            .select_related("bundle")
            .prefetch_related(
                "bundle__items",
                "bundle__items__product",
                "bundle__items__product__company",
                "bundle__items__product__company__owner",
            )
            .only("id", "order_id", "total_price", "quantity", "bundle_id", "bundle_snapshot")
        )
        if allowed_company_ids is not None:
            oi_qs = oi_qs.filter(bundle__items__product__company_id__in=allowed_company_ids).distinct()

        for it in oi_qs:
            item_total = float(getattr(it, "total_price", 0.0) or 0.0)
            if item_total <= 0:
                continue

            snap = getattr(it, "bundle_snapshot", None) or {}
            b = getattr(it, "bundle", None)

            # company_pbq: {company_id: sum_of_per_bundle_quantity}
            company_pbq = defaultdict(int)
            company_names_local = {}

            products_snap = snap.get("products") or []
            if products_snap:
                for p in products_snap:
                    cid = p.get("company_id")
                    cname = p.get("company_name")
                    pbq = int(p.get("per_bundle_quantity", 1))
                    if cid is None:
                        continue
                    company_pbq[cid] += pbq
                    if cname:
                        company_names_local[cid] = cname

            if not company_pbq and b is not None:
                b_items = getattr(b, "items", None)
                iterable = b_items.all() if hasattr(b_items, "all") else (b_items or [])
                for bi in iterable:
                    prod = getattr(bi, "product", None)
                    if not prod:
                        continue
                    comp = getattr(prod, "company", None)
                    cid = getattr(comp, "id", None)
                    cname = getattr(comp, "name", None)
                    pbq = int(getattr(bi, "quantity", 1) or 1)
                    if cid is None:
                        continue
                    company_pbq[cid] += pbq
                    if cname:
                        company_names_local[cid] = cname

            if not company_pbq and isinstance(snap, dict):
                top_cid = snap.get("company_id")
                top_cname = snap.get("company_name")
                if top_cid is not None:
                    company_pbq[top_cid] += 1
                    if top_cname:
                        company_names_local[top_cid] = top_cname

            # Restrict to producer-owned companies in producer scope
            if allowed_company_ids is not None:
                company_pbq = {cid: pbq for cid, pbq in company_pbq.items() if cid in allowed_company_ids}
                company_names_local = {cid: nm for cid, nm in company_names_local.items() if cid in allowed_company_ids}

            if not company_pbq:
                continue

            total_pbq = sum(company_pbq.values())
            n_companies = len(company_pbq)
            for cid, pbq in company_pbq.items():
                if cid is None:
                    continue
                if total_pbq > 0:
                    share = (pbq / total_pbq)
                else:
                    share = 1.0 / n_companies
                rev_share = item_total * share
                order_company_revenue[it.order_id][cid] += rev_share

                if cid not in company_name_by_id and cid in company_names_local:
                    company_name_by_id[cid] = company_names_local[cid]
                all_company_ids.add(cid)

        # Resolve company -> owner (user)
        company_to_owner_id = {}
        user_display_by_id = {}
        if all_company_ids:
            for comp in Company.objects.filter(id__in=all_company_ids).select_related("owner"):
                owner = getattr(comp, "owner", None)
                uid = getattr(owner, "id", None)
                company_to_owner_id[comp.id] = uid
                if uid is not None and uid not in user_display_by_id:
                    user_display_by_id[uid] = _user_display_name(owner)
                if comp.id not in company_name_by_id:
                    company_name_by_id[comp.id] = getattr(comp, "name", None)

        # Build cohorts and rows
        cohorts = {}
        rows_by_user = defaultdict(lambda: {
            "orders_by_offset": defaultdict(int),
            "producer_user_ids": set(),
            "company_names": set(),
        })

        total_revenue = 0.0
        total_orders = 0

        for o in orders:
            uid = o.user_id
            if uid not in first_by_user:
                continue

            cohort_start = first_by_user[uid]
            offset = (o.created_at.year - cohort_start.year) * 12 + (o.created_at.month - cohort_start.month)
            ckey = cohort_start.strftime("%Y-%m")

            c = cohorts.setdefault(
                ckey,
                {
                    "customers": set(),
                    "periods": defaultdict(lambda: {"orders": 0, "revenue": 0.0, "customers": set()}),
                    "producer_user_ids": set(),
                    "company_names": set(),
                }
            )
            c["customers"].add(uid)
            c["periods"][offset]["orders"] += 1
            c["periods"][offset]["revenue"] += float(getattr(o, "total_price", 0) or 0.0)
            c["periods"][offset]["customers"].add(uid)

            comp_rev_map = order_company_revenue.get(o.id, {})
            if comp_rev_map:
                owner_ids = {
                    company_to_owner_id.get(cid)
                    for cid in comp_rev_map.keys()
                    if company_to_owner_id.get(cid) is not None
                }
                if owner_ids:
                    c["producer_user_ids"].update(owner_ids)
                    rows_by_user[uid]["producer_user_ids"].update(owner_ids)
                comp_names = {
                    company_name_by_id.get(cid)
                    for cid in comp_rev_map.keys()
                    if company_name_by_id.get(cid)
                }
                if comp_names:
                    c["company_names"].update(comp_names)
                    rows_by_user[uid]["company_names"].update(comp_names)

            rows_by_user[uid]["cohort_month"] = ckey
            rows_by_user[uid]["first_order"] = cohort_start.isoformat()
            rows_by_user[uid]["orders_by_offset"][offset] += 1

            total_revenue += float(getattr(o, "total_price", 0) or 0.0)
            total_orders += 1

        cohorts_out = []
        for ckey in sorted(cohorts.keys()):
            v = cohorts[ckey]
            periods = []
            for off in sorted(v["periods"].keys()):
                p = v["periods"][off]
                periods.append({
                    "offset": off,
                    "orders": p["orders"],
                    "revenue": round(p["revenue"], 2),
                    "customers_active": len(p["customers"]),
                })

            uid_list = sorted(list(v["producer_user_ids"]))
            producer_names = [user_display_by_id.get(x) for x in uid_list if user_display_by_id.get(x)]
            cohorts_out.append({
                "cohort_month": ckey,
                "customers": len(v["customers"]),
                "periods": periods,
                "producer_ids": uid_list,
                "producer_names": producer_names,
                "company_names": sorted(list(v["company_names"])),
            })

        rows = []
        for uid, info in rows_by_user.items():
            uid_list = sorted(list(info["producer_user_ids"]))
            producer_names = [user_display_by_id.get(x) for x in uid_list if user_display_by_id.get(x)]
            rows.append({
                "user_id": uid,
                "cohort_month": info.get("cohort_month"),
                "first_order": info.get("first_order"),
                "orders_by_offset": dict(sorted(info["orders_by_offset"].items())),
                "producer_ids": uid_list,
                "producer_names": producer_names,
                "company_names": sorted(list(info["company_names"])),
            })

        # Company-level cohorts
        cohorts_company_map = defaultdict(lambda: {
            "customers": set(),
            "periods": defaultdict(lambda: {"orders": 0, "revenue": 0.0, "customers": set()}),
        })

        for o in orders:
            uid = o.user_id
            if uid not in first_by_user:
                continue
            cohort_start = first_by_user[uid]
            offset = (o.created_at.year - cohort_start.year) * 12 + (o.created_at.month - cohort_start.month)
            ckey = cohort_start.strftime("%Y-%m")

            comp_rev_map = order_company_revenue.get(o.id, {})
            for cid, rev in comp_rev_map.items():
                key = (ckey, cid)
                entry = cohorts_company_map[key]
                entry["customers"].add(uid)
                entry["periods"][offset]["orders"] += 1
                entry["periods"][offset]["revenue"] += float(rev or 0.0)
                entry["periods"][offset]["customers"].add(uid)

        cohorts_company = []
        rows_company = []

        for (ckey, cid), v in sorted(cohorts_company_map.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            cname = company_name_by_id.get(cid)
            owner_uid = company_to_owner_id.get(cid)
            owner_name = user_display_by_id.get(owner_uid) if owner_uid is not None else None

            periods = []
            for off in sorted(v["periods"].keys()):
                p = v["periods"][off]
                periods.append({
                    "offset": off,
                    "orders": p["orders"],
                    "revenue": round(p["revenue"], 2),
                    "customers_active": len(p["customers"]),
                })

            cohorts_company.append({
                "cohort_month": ckey,
                "company_id": cid,
                "company_name": cname,
                "producer_id": owner_uid,
                "producer_name": owner_name,
                "customers": len(v["customers"]),
                "periods": periods,
            })

            rows_company.append({
                "cohort_month": ckey,
                "company_id": cid,
                "company_name": cname,
                "producer_id": owner_uid,
                "producer_name": owner_name,
                "periods": periods,
            })

        summary = {
            "cohorts": len(cohorts_out),
            "customers": sum(c["customers"] for c in cohorts_out),
            "revenue": round(total_revenue, 2),
            "orders": total_orders,
        }

        return Response({
            "summary": summary,
            "cohorts": cohorts_out,
            "rows": rows,
            "cohorts_company": cohorts_company,
            "rows_company": rows_company,
        })



# ============================================================
# 10) Géographie — deep
# ============================================================
class GeoDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        level = (request.GET.get("level") or "department").lower()
        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="revenue",
            allowed=["revenue", "orders", "customers", "zone"],
        )
        reverse = (sort_dir == "desc")

        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        orders = self.get_orders(request, date_from, date_to).select_related(
            "shipping_address",
            "billing_address",
            "shipping_address__city",
            "billing_address__city",
            "shipping_address__city__department",
            "billing_address__city__department",
            "shipping_address__city__department__region",
            "billing_address__city__department__region",
        )

        def _addr_zone(addr):
            if not addr:
                return None, None
            city = getattr(addr, "city", None)
            dep = getattr(city, "department", None) if city else None
            reg = getattr(dep, "region", None) if dep else None
            if level == "region" and reg:
                return getattr(reg, "code", None), getattr(reg, "name", None)
            if level == "department" and dep:
                return getattr(dep, "code", None), getattr(dep, "name", None)
            if level == "city" and city:
                code = getattr(city, "insee_code", None) or getattr(city, "postal_code", None)
                return code, getattr(city, "name", None)
            snap = getattr(addr, "snapshot", None) or {}
            if isinstance(snap, dict):
                if level == "region":
                    code = snap.get("region_code") or snap.get("region")
                    desc = snap.get("region") or code
                    return code, desc
                if level == "department":
                    code = snap.get("department_code") or snap.get("department")
                    desc = snap.get("department") or code
                    return code, desc
                if level == "city":
                    code = snap.get("postal_code") or snap.get("city_code") or snap.get("city")
                    desc = snap.get("city") or code
                    return code, desc
            return None, None

        def _order_zone(o):
            code, desc = _addr_zone(getattr(o, "shipping_address", None))
            if code or desc:
                return code, desc
            code, desc = _addr_zone(getattr(o, "billing_address", None))
            if code or desc:
                return code, desc
            for snap_key in ("shipping_address_snapshot", "billing_address_snapshot"):
                snap = getattr(o, snap_key, None) or {}
                if isinstance(snap, dict):
                    if level == "region":
                        code = snap.get("region_code") or snap.get("region")
                        return code, (snap.get("region") or code)
                    if level == "department":
                        code = snap.get("department_code") or snap.get("department")
                        return code, (snap.get("department") or code)
                    if level == "city":
                        code = snap.get("postal_code") or snap.get("city_code") or snap.get("city")
                        return code, (snap.get("city") or code)
            return None, None

        order_meta = {}
        agg = defaultdict(lambda: {"zone_desc": None, "revenue": 0.0, "orders": 0, "customers": set()})
        for o in orders:
            z_code, z_desc = _order_zone(o)
            z_code = z_code or "unknown"
            z_desc = z_desc or "unknown"
            order_meta[o.id] = (z_code, z_desc, o.created_at)
            a = agg[z_code]
            a["zone_desc"] = z_desc
            a["orders"] += 1

        def _user_display(u):
            if not u:
                return None
            name = getattr(u, "public_display_name", None)
            if name:
                return name
            fn = (getattr(u, "first_name", "") or "").strip()
            ln = (getattr(u, "last_name", "") or "").strip()
            full = " ".join([p for p in (fn, ln) if p])
            if full:
                return full
            return getattr(u, "email", None)

        items_qs = (
            OrderItem.objects
            .filter(order__in=orders)
            .select_related("order", "order__user", "bundle")
            .prefetch_related(
                Prefetch(
                    "bundle__items",
                    queryset=ProductBundleItem.objects.select_related(
                        "product",
                        "product__company__owner",
                    )
                )
            )
            .only(
                "id", "order_id", "quantity", "total_price",
                "bundle_id", "bundle_snapshot",
                "order__created_at", "order__status", "order__order_code",
                "order__user__id", "order__user__public_display_name",
                "order__user__first_name", "order__user__last_name",
                "order__user__email",
            )
        )
        if allowed_company_ids is not None:
            items_qs = items_qs.filter(bundle__items__product__company_id__in=allowed_company_ids).distinct()

        rows = []

        for oi in items_qs:
            z_code, z_desc, created_at = order_meta.get(oi.order_id, ("unknown", "unknown", None))
            a = agg[z_code]
            a["revenue"] += float(getattr(oi, "total_price", 0) or 0)
            a["customers"].add(getattr(getattr(oi, "order", None), "user_id", None))

            parts = list(_iter_snapshot_products(oi))
            q = int(getattr(oi, "quantity", 0) or 0)
            total_price = float(getattr(oi, "total_price", 0) or 0)

            # Filter parts by allowed companies (using live bundle map)
            pid_to_cid = {}
            b = getattr(oi, "bundle", None)
            if b is not None:
                b_items = getattr(b, "items", None)
                b_iter = b_items.all() if hasattr(b_items, "all") else (b_items or [])
                for bi in b_iter:
                    prod = getattr(bi, "product", None)
                    if prod is not None:
                        pid_to_cid[getattr(prod, "id", None)] = getattr(getattr(prod, "company", None), "id", None)

            if allowed_company_ids is not None:
                parts = [p for p in parts if pid_to_cid.get(p[0]) in allowed_company_ids]
                if not parts:
                    continue

            if b is not None:
                pids_company, owner_names, company_names = _bundle_producers_and_companies(b)
                if allowed_company_ids is not None:
                    filt = [(cid, on, cn) for cid, on, cn in zip(pids_company, owner_names, company_names) if cid in allowed_company_ids]
                    pids_company = [x[0] for x in filt]; owner_names = [x[1] for x in filt]; company_names = [x[2] for x in filt]
            else:
                snap = getattr(oi, "bundle_snapshot", None) or {}
                pids_company, company_names = _producers_from_snapshot(snap)
                owner_names = []

            o = getattr(oi, "order", None)
            u = getattr(o, "user", None)
            user_id = getattr(u, "id", None)
            user_name = _user_display(u)

            total_pbq = sum(pbq for *_, pbq in parts) or 1
            for pid, title, cat_id, cat_name, pbq in parts:
                share = (pbq / total_pbq)
                units_share = q * pbq
                revenue_share = total_price * share
                rows.append({
                    "order_id": oi.order_id,
                    "order_code": getattr(getattr(oi, "order", None), "order_code", None),
                    "order_item_id": oi.id,
                    "created_at": created_at.isoformat() if created_at else None,
                    "zone": z_code,
                    "zone_desc": z_desc,
                    "product_id": pid,
                    "product_title": title,
                    "category_id": cat_id,
                    "category_name": cat_name,
                    "quantity_bundle": q,
                    "per_bundle_quantity": pbq,
                    "units_share": units_share,
                    "revenue_share": round(revenue_share, 4),
                    "producer_ids": list(pids_company),
                    "producer_names": [n for n in owner_names if n] or None,
                    "company_names": [n for n in company_names if n] or None,
                    "user_id": user_id,
                    "user_name": user_name,
                })

        by_zone_all = [
            {
                "zone": z_code,
                "zone_desc": v["zone_desc"] or z_code,
                "revenue": round(v["revenue"], 2),
                "orders": v["orders"],
                "customers": len([u for u in v["customers"] if u is not None]),
            }
            for z_code, v in agg.items()
        ]
        by_zone_all.sort(
            key=lambda x: (x[sort_by] if sort_by != "zone" else (x["zone"] or "")),
            reverse=reverse,
        )
        z_count = len(by_zone_all)
        by_zone = by_zone_all[offset: offset + limit]

        summary = {
            "zones": z_count,
            "orders": sum(x["orders"] for x in by_zone_all),
            "revenue": round(sum(x["revenue"] for x in by_zone_all), 2),
            "customers": sum(x["customers"] for x in by_zone_all),
        }

        return Response({
            "summary": summary,
            "by_zone": by_zone,
            "rows": rows,
            "meta": {"count": z_count, "limit": limit, "offset": offset},
        })


# ============================================================
# 11) Evaluations
# ============================================================


class EvaluationsDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def _bundle_companies_and_owners_instance(self, bundle, allowed_company_ids=None):
        company_ids, company_names, owner_ids, owner_names = [], [], [], []
        if not bundle:
            return company_ids, company_names, owner_ids, owner_names
        seen = set()
        items_rel = getattr(bundle, "items", None)
        iterable = items_rel.all() if hasattr(items_rel, "all") else (items_rel or [])
        for bi in iterable:
            prod = getattr(bi, "product", None)
            comp = getattr(prod, "company", None) if prod else None
            if not comp:
                continue
            cid = getattr(comp, "id", None)
            if cid is None or cid in seen:
                continue
            if allowed_company_ids is not None and cid not in allowed_company_ids:
                continue
            seen.add(cid)
            company_ids.append(cid)
            company_names.append(getattr(comp, "name", None))
            owner = getattr(comp, "owner", None)
            owner_ids.append(getattr(owner, "id", None) if owner else None)
            owner_names.append(
                (getattr(owner, "public_display_name", None)
                 or " ".join(x for x in [
                        (getattr(owner, "first_name", "") or "").strip(),
                        (getattr(owner, "last_name", "") or "").strip()
                    ] if x)
                 or getattr(owner, "email", None)
                 or getattr(owner, "username", None))
                if owner else None
            )
        return company_ids, company_names, owner_ids, owner_names

    def _user_display(self, user):
        if not user:
            return None
        return (
            getattr(user, "public_display_name", None)
            or " ".join(x for x in [
                (getattr(user, "first_name", "") or "").strip(),
                (getattr(user, "last_name", "") or "").strip()
            ] if x)
            or getattr(user, "email", None)
            or getattr(user, "username", None)
        )

    def get(self, request, **kwargs):
        from django.db.models import Sum, Count, Prefetch

        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        kind = (request.GET.get("kind") or "all").lower()

        orders = self.get_orders(request, date_from, date_to)

        # Rated items queryset
        items_qs = (
            OrderItem.objects
            .filter(order__in=orders, customer_rating__isnull=False)
            .select_related("order", "order__user", "bundle")
            .prefetch_related(
                Prefetch(
                    "bundle__items",
                    queryset=ProductBundleItem.objects.select_related(
                        "product",
                        "product__company__owner",
                    )
                )
            )
        )

        if not self.is_admin_scope:
            allowed_company_ids = set(_company_ids(request.user))
            if allowed_company_ids:
                items_qs = items_qs.filter(
                    bundle__items__product__company_id__in=allowed_company_ids
                ).distinct()
            else:
                items_qs = items_qs.none()
        else:
            allowed_company_ids = None

        orders_qs = (
            orders
            .filter(customer_rating__isnull=False)
        )

        # Summary: averages and distributions (rated only)
        items_agg = items_qs.aggregate(n=Count("id"), s=Sum("customer_rating"))
        orders_agg = orders_qs.aggregate(n=Count("id"), s=Sum("customer_rating"))

        summary = {
            "avg_item_rating": round(float((items_agg["s"] or 0) / items_agg["n"]) if items_agg["n"] else 0.0, 2),
            "item_ratings_count": int(items_agg["n"] or 0),
            "avg_order_rating": round(float((orders_agg["s"] or 0) / orders_agg["n"]) if orders_agg["n"] else 0.0, 2),
            "order_ratings_count": int(orders_agg["n"] or 0),
            "distribution_items": {
                str(r["customer_rating"]): r["n"]
                for r in items_qs.values("customer_rating").annotate(n=Count("id")).order_by()
            },
            "distribution_orders": {
                str(r["customer_rating"]): r["n"]
                for r in orders_qs.values("customer_rating").annotate(n=Count("id")).order_by()
            },
        }

        # Totals including NOT rated (for later calculations)
        # 1) Total number of orders where the producer participates (already scoped by get_orders)
        orders_total = orders.count()

        # 2) Total units of producer-owned items in those orders
        total_units_all = 0
        items_all = (
            OrderItem.objects
            .filter(order__in=orders)
            .select_related("bundle")
            .prefetch_related(
                Prefetch(
                    "bundle__items",
                    queryset=ProductBundleItem.objects.select_related(
                        "product",
                        "product__company",
                    )
                )
            )
        )
        for oi in items_all:
            q = int(getattr(oi, "quantity", 0) or 0)

            # Build parts from snapshot/live (product_id, title, cat_id, cat_name, pbq)
            parts = list(_iter_snapshot_products(oi))

            # Map product_id -> company_id using live bundle
            pid_to_cid = {}
            b = getattr(oi, "bundle", None)
            if b is not None:
                b_items = getattr(b, "items", None)
                b_iter = b_items.all() if hasattr(b_items, "all") else (b_items or [])
                for bi in b_iter:
                    prod_inst = getattr(bi, "product", None)
                    if prod_inst is not None:
                        pid_to_cid[getattr(prod_inst, "id", None)] = getattr(getattr(prod_inst, "company", None), "id", None)

            # Filter parts to allowed companies if needed
            if allowed_company_ids is not None:
                parts = [p for p in parts if pid_to_cid.get(p[0]) in allowed_company_ids]
                if not parts:
                    continue

            per_bundle_qty_sum = sum(pbq for *_, pbq in parts)
            if per_bundle_qty_sum and q:
                total_units_all += q * per_bundle_qty_sum

        summary.update({
            "orders_total": int(orders_total),
            "items_units_total": int(total_units_all),
        })

        rows = []

        if kind in ("item", "all"):
            sort_by, sort_dir = _sort_params(
                request,
                default_sort_by="rated_at",
                allowed=["rated_at", "rating", "created_at", "line_total"],
            )
            order_field = "customer_rating" if sort_by == "rating" else sort_by
            order_by_expr = ("-" if sort_dir == "desc" else "") + order_field

            page_items = list(items_qs.order_by(order_by_expr)[offset: offset + limit])
            for oi in page_items:
                b = getattr(oi, "bundle", None)
                cids, cnames, owner_ids, owner_names = self._bundle_companies_and_owners_instance(
                    b, allowed_company_ids=allowed_company_ids
                )

                if allowed_company_ids is not None and not cids:
                    continue

                user = getattr(getattr(oi, "order", None), "user", None)
                rows.append({
                    "type": "item",
                    "order_id": getattr(oi, "order_id", None),
                    "order_code": getattr(getattr(oi, "order", None), "order_code", None),
                    "item_id": oi.id,
                    "bundle_id": getattr(oi, "bundle_id", None),
                    "bundle_title": getattr(b, "title", None),
                    "rating": getattr(oi, "customer_rating", None),
                    "note": getattr(oi, "customer_note", "") or "",
                    "rated_at": getattr(oi, "rated_at", None).isoformat() if getattr(oi, "rated_at", None) else None,
                    "created_at": getattr(getattr(oi, "order", None), "created_at", None).isoformat()
                                  if getattr(getattr(oi, "order", None), "created_at", None) else None,
                    "quantity": int(getattr(oi, "quantity", 0) or 0),
                    "line_total": float(getattr(oi, "total_price", 0) or 0.0),
                    "company_ids": cids or None,
                    "company_names": cnames or None,
                    "producer_ids": owner_ids or None,
                    "producer_names": [n for n in owner_names if n] or None,
                    "user_id": getattr(user, "id", None),
                    "user_name": self._user_display(user),
                })

        if kind in ("order", "all"):
            sort_by, sort_dir = _sort_params(
                request,
                default_sort_by="rated_at",
                allowed=["rated_at", "rating", "created_at", "total_price"],
            )
            order_field = "customer_rating" if sort_by == "rating" else sort_by
            order_by_expr = ("-" if sort_dir == "desc" else "") + order_field

            orders_qs = orders_qs.select_related("user").prefetch_related(
                Prefetch(
                    "items",
                    queryset=OrderItem.objects.select_related("bundle").prefetch_related(
                        Prefetch(
                            "bundle__items",
                            queryset=ProductBundleItem.objects.select_related(
                                "product",
                                "product__company__owner",
                            )
                        )
                    )
                )
            )

            page_orders = list(orders_qs.order_by(order_by_expr)[offset: offset + limit])
            for o in page_orders:
                comp_ids_set, comp_names_set, owner_ids_set, owner_names_set = set(), set(), set(), set()

                items_rel = getattr(o, "items", None)
                iterable = items_rel.all() if hasattr(items_rel, "all") else (items_rel or [])
                for it in iterable:
                    b = getattr(it, "bundle", None)
                    cids, cnames, owner_ids, owner_names = self._bundle_companies_and_owners_instance(
                        b, allowed_company_ids=allowed_company_ids
                    )
                    for cid in cids:
                        comp_ids_set.add(cid)
                    for n in cnames:
                        if n:
                            comp_names_set.add(n)
                    for pid in owner_ids:
                        if pid is not None:
                            owner_ids_set.add(pid)
                    for pn in owner_names:
                        if pn:
                            owner_names_set.add(pn)

                if allowed_company_ids is not None and not comp_ids_set:
                    continue

                user = getattr(o, "user", None)
                rows.append({
                    "type": "order",
                    "order_id": getattr(o, "id", None),
                    "order_code": getattr(o, "order_code", None),
                    "rating": getattr(o, "customer_rating", None),
                    "note": getattr(o, "customer_note", "") or "",
                    "rated_at": getattr(o, "rated_at", None).isoformat() if getattr(o, "rated_at", None) else None,
                    "created_at": getattr(o, "created_at", None).isoformat() if getattr(o, "created_at", None) else None,
                    "total_price": float(getattr(o, "total_price", 0) or 0.0),
                    "company_ids": sorted(list(comp_ids_set)) or None,
                    "company_names": sorted(list(comp_names_set)) or None,
                    "producer_ids": sorted(list(owner_ids_set)) or None,
                    "producer_names": sorted(list(owner_names_set)) or None,
                    "user_id": getattr(user, "id", None),
                    "user_name": self._user_display(user),
                })

        return Response({
            "summary": summary,
            "rows": rows,
            "meta": {"limit": limit, "offset": offset},
        })





# ---------- 2) Reviews Keywords ----------

class ReviewsKeywordsView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/reviews/keywords/?date_from&date_to&lang=fr|es|en&top_k=100
    GET /api/admin/analytics/reviews/keywords/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)
        lang = (request.GET.get("lang") or "fr").lower()
        try:
            top_k = max(10, min(500, int(request.GET.get("top_k", 100))))
        except Exception:
            top_k = 100

        if self.is_admin_scope:
            items_qs = OrderItem.objects.filter(order__status__in=VALID_STATUSES, customer_note__isnull=False)\
                                        .exclude(customer_note__exact="")
            orders_qs = Order.objects.filter(status__in=VALID_STATUSES, customer_note__isnull=False)\
                                     .exclude(customer_note__exact="")
        else:
            items_qs = _orderitems_for_producer(request.user, date_from, date_to)\
                        .filter(customer_note__isnull=False).exclude(customer_note__exact="")
            orders_qs = (
                Order.objects.filter(
                    status__in=VALID_STATUSES,
                    items__bundle__items__product__company_id__in=_company_ids(request.user),
                    customer_note__isnull=False,
                ).exclude(customer_note__exact="")
                .distinct()
            )
        if date_from:
            items_qs = items_qs.filter(order__created_at__date__gte=date_from)
            orders_qs = orders_qs.filter(created_at__date__gte=date_from)
        if date_to:
            items_qs = items_qs.filter(order__created_at__date__lte=date_to)
            orders_qs = orders_qs.filter(created_at__date__lte=date_to)

        notes = list(items_qs.values_list("customer_note", flat=True)) + list(orders_qs.values_list("customer_note", flat=True))
        c = Counter()
        for note in notes:
            c.update(_tokenize(note or "", lang))

        rows = [{"token": tok, "count": cnt} for tok, cnt in c.most_common(top_k)]
        summary = {"notes_count": len(notes), "unique_keywords": len(c)}
        return Response({"summary": summary, "rows": rows})

# ---------- 3) Cross: Sales vs Ratings (unificado; añade 'date') ----------

class SalesVsRatingsView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/sales-vs-ratings/?bucket=week|month|day&date_from&date_to
    GET /api/admin/analytics/cross/sales-vs-ratings/?...
    - series_by_period: [{period, date, revenue, orders, avg_item_rating, item_ratings}]
    - scatter_bundles: [{bundle_id, title, revenue, avg_item_rating, ratings_count, producer_names?}]
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        bucket = request.GET.get("bucket", "week").lower()
        date_from, date_to = _date_range(request)

        orders = self.get_orders(request, date_from, date_to)

        # Series
        series_map = defaultdict(lambda: {"revenue": 0.0, "orders": 0, "ratings_sum": 0.0, "ratings_cnt": 0})
        date_anchor = {}
        for o in orders.only("id", "total_price", "created_at"):
            p = _bucket(o.created_at, bucket)
            series_map[p]["revenue"] += float(o.total_price or 0)
            series_map[p]["orders"] += 1
            if p not in date_anchor:
                date_anchor[p] = self.bucket_anchor_date(o.created_at, bucket)

        rated_items = OrderItem.objects.filter(order__in=orders, customer_rating__isnull=False)\
                                       .only("id", "customer_rating", "order__created_at")
        for oi in rated_items:
            p = _bucket(oi.order.created_at, bucket)
            series_map[p]["ratings_sum"] += float(oi.customer_rating or 0)
            series_map[p]["ratings_cnt"] += 1
            if p not in date_anchor:
                date_anchor[p] = self.bucket_anchor_date(oi.order.created_at, bucket)

        series_by_period = []
        for period, v in sorted(series_map.items()):
            avg_item_rating = (v["ratings_sum"] / v["ratings_cnt"]) if v["ratings_cnt"] else 0.0
            series_by_period.append({
                "period": period,
                "date": date_anchor.get(period),
                "revenue": round(v["revenue"], 2),
                "orders": v["orders"],
                "avg_item_rating": round(avg_item_rating, 2),
                "item_ratings": v["ratings_cnt"],
            })

        # Scatter por bundle (con producer_names en admin)
        bundles = (
            OrderItem.objects
            .filter(order__in=orders)
            .values("bundle_id", "bundle_snapshot")
            .annotate(revenue=Sum("total_price"))
        )
        bundles_ids = [b["bundle_id"] for b in bundles if b["bundle_id"]]
        rated_qs = (OrderItem.objects.filter(bundle_id__in=bundles_ids, customer_rating__isnull=False)
                    .values("bundle_id").annotate(avg=Avg("customer_rating"), cnt=Count("id"))
                    .values_list("bundle_id", "avg", "cnt"))
        rated_by_bundle = {bid: (avg, cnt) for bid, avg, cnt in rated_qs}

        scatter = []
        for b in bundles:
            bid = b["bundle_id"]
            if not bid:
                continue
            snapshot = b.get("bundle_snapshot") or {}
            title = snapshot.get("title")
            avg, cnt = rated_by_bundle.get(bid, (None, 0))
            row = {
                "bundle_id": bid,
                "title": title,
                "revenue": float(b["revenue"] or 0),
                "avg_item_rating": round(float(avg or 0), 2) if avg is not None else None,
                "ratings_count": int(cnt or 0),
            }
            if self.is_admin_scope:
                row["producer_names"] = self.producer_names_from_snapshot(snapshot) or None
            scatter.append(row)

        return Response({"series_by_period": series_by_period, "scatter_bundles": scatter})






# ============================================================
#  Ventes par catégorie — deep (unifiée)    
# ============================================================

class SalesByCategoryDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def _producers_from_snapshot_with_owners(self, snap: dict, owner_cache: dict):
        ids, cnames = [], []
        seen = set()
        top_cid = snap.get("company_id")
        top_cname = snap.get("company_name")
        if top_cid is not None and top_cid not in seen:
            seen.add(top_cid); ids.append(top_cid); cnames.append(top_cname)
        for p in (snap.get("products") or []):
            cid = p.get("company_id"); cname = p.get("company_name")
            if cid is None or cid in seen: continue
            seen.add(cid); ids.append(cid); cnames.append(cname)
        onames = []
        missing = [cid for cid in ids if cid not in owner_cache]
        if missing:
            for comp in Company.objects.filter(id__in=missing).select_related("owner"):
                owner_cache[comp.id] = _user_display_name(getattr(comp, "owner", None))
        for cid in ids:
            onames.append(owner_cache.get(cid))
        return ids, onames, cnames

    def _resolve_producers(self, oi, owner_cache):
        b = getattr(oi, "bundle", None)
        if b is not None:
            return _bundle_producers_and_companies(b)
        snap = getattr(oi, "bundle_snapshot", None) or {}
        return self._producers_from_snapshot_with_owners(snap, owner_cache)

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="revenue",
            allowed=["revenue", "orders", "units", "category_name"],
        )
        reverse = (sort_dir == "desc")

        orders = self.get_orders(request, date_from, date_to)
        allowed_company_ids = set(_company_ids(request.user)) if not self.is_admin_scope else None

        items = (
            OrderItem.objects
            .filter(order__in=orders)
            .only(
                "id", "order_id", "quantity", "total_price",
                "bundle_snapshot", "bundle_id",
                "order__created_at",
            )
            .select_related("order", "bundle")
            .prefetch_related(
                "bundle__items",
                "bundle__items__product",
                "bundle__items__product__company",
                "bundle__items__product__company__owner",
            )
        )
        if allowed_company_ids is not None:
            items = items.filter(bundle__items__product__company_id__in=allowed_company_ids).distinct()

        cat = {}
        prod = {}
        ultra_rows = []
        owner_cache = {}

        for oi in items:
            q = int(oi.quantity or 0)
            total = float(oi.total_price or 0.0)

            parts = list(_iter_snapshot_products(oi))  # (product_id, title, cat_id, cat_name, pbq)

            # Map product_id -> company_id using live bundle
            pid_to_cid = {}
            b = getattr(oi, "bundle", None)
            if b is not None:
                b_items = getattr(b, "items", None)
                b_iter = b_items.all() if hasattr(b_items, "all") else (b_items or [])
                for bi in b_iter:
                    prod_inst = getattr(bi, "product", None)
                    if prod_inst is not None:
                        pid_to_cid[getattr(prod_inst, "id", None)] = getattr(getattr(prod_inst, "company", None), "id", None)

            if allowed_company_ids is not None:
                parts = [p for p in parts if pid_to_cid.get(p[0]) in allowed_company_ids]
                if not parts:
                    continue

            total_pbq = sum(pbq for *_, pbq in parts) or 1
            pids_company, owner_names, company_names = self._resolve_producers(oi, owner_cache)

            seen_cat_keys = set()
            seen_prod_ids = set()
            order_dt = getattr(getattr(oi, "order", None), "created_at", None)
            created_iso = order_dt.isoformat() if order_dt else None
            order_date = timezone.localdate(order_dt).isoformat() if order_dt else None

            for pid, title, cat_id, cat_name, pbq in parts:
                share = (pbq / total_pbq)
                revenue_share = total * share
                units_share = q * pbq

                ckey = _category_key(cat_id, cat_name)
                c = cat.setdefault(ckey, {"revenue": 0.0, "orders": set(), "units": 0})
                c["revenue"] += revenue_share
                c["units"] += units_share
                if ckey not in seen_cat_keys:
                    c["orders"].add(oi.order_id)
                    seen_cat_keys.add(ckey)

                p = prod.setdefault(pid, {
                    "product_id": pid,
                    "label": title,
                    "category": {"id": ckey[0], "name": ckey[1]},
                    "revenue": 0.0,
                    "units": 0,
                    "orders": set(),
                    "producer_ids": list(pids_company),
                    "producer_names": [n for n in owner_names if n],
                    "company_names": [n for n in company_names if n],
                })
                p["revenue"] += revenue_share
                p["units"] += units_share
                if pid not in seen_prod_ids:
                    p["orders"].add(oi.order_id)
                    seen_prod_ids.add(pid)

                ultra_rows.append({
                    "order_id": oi.order_id,
                    "order_item_id": oi.id,
                    "product_id": pid,
                    "label": title,
                    "category_id": ckey[0],
                    "category_name": ckey[1],
                    "quantity_bundle": q,
                    "per_bundle_quantity": pbq,
                    "units_share": units_share,
                    "revenue_share": round(revenue_share, 4),
                    "created_at": created_iso,
                    "order_date": order_date,
                    "producer_ids": list(pids_company),
                    "producer_names": [n for n in owner_names if n],
                    "company_names": [n for n in company_names if n],
                })

        by_category_all = [
            {
                "category_id": k[0],
                "category_name": k[1],
                "revenue": round(v["revenue"], 2),
                "orders": len(v["orders"]),
                "units": int(v["units"]),
            }
            for k, v in cat.items()
        ]
        by_category_all.sort(
            key=lambda x: (x[sort_by] if sort_by != "category_name" else (x["category_name"] or "")),
            reverse=reverse,
        )
        c_count = len(by_category_all)
        by_category = by_category_all[offset: offset + limit]

        products_rows = [
            {**v, "revenue": round(v["revenue"], 2), "orders": len(v["orders"])}
            for v in prod.values()
        ]
        products_rows.sort(key=lambda x: x["revenue"], reverse=True)

        summary = {
            "categories": c_count,
            "revenue": round(sum(x["revenue"] for x in by_category_all), 2),
            "orders": sum(x["orders"] for x in by_category_all),
            "units": sum(x["units"] for x in by_category_all),
        }

        return Response({
            "summary": summary,
            "by_category": by_category,
            "rows": {
                "products": products_rows,
                "items": ultra_rows[:2000],
            },
            "meta": {"count": c_count, "limit": limit, "offset": offset},
        })


