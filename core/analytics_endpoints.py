# analytics_endpoints.py
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict
from decimal import Decimal

from django.conf import settings
from django.db.models import Prefetch, QuerySet

from django.db.models import Sum, Count, Min, Max
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
    SalesOrderItemRowSerializer, 
    SalesOrderRowSerializer,
    OrderDeepSerializer, 
    CustomerRowSerializer, 
    CartsAbandonedRowSerializer,
    CartDeepSerializer)

from .analytics_scope import AnalyticsScopeMixin


# ============================================================
# Constantes communes (FR)
# ============================================================

VALID_STATUSES = ("confirmed", "delivered")


# ============================================================
# Aides communes (FR)
# ============================================================


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


def _pagination(request, default_limit=50) -> Tuple[int, int]:
    """FR: limit/offset (limit par défaut: 50, max 500)."""
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
            # pas de snapshot => on laisse vide (au pire sera None)
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
    1) Utilise le snapshot s'il est enrichi (category_id/name)
    2) Sinon, retombe sur la BD: ProductBundleItem -> Product -> catalog_entry.category
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


# ============================================================
# 1) Ventes — séries + détails commandes (unifiée)
# ============================================================


class SalesTimeseriesView(APIView):
    def _parse_date(self, value: str) -> datetime:
        return datetime.strptime(value, "%Y-%m-%d")

    def _daterange_bounds(self, request) -> Tuple[datetime, datetime]:
        tz = timezone.get_default_timezone()
        end_param = request.query_params.get("end")
        start_param = request.query_params.get("start")

        if end_param:
            end_dt = timezone.make_aware(self._parse_date(end_param), tz) + timedelta(days=1) - timedelta(seconds=1)
        else:
            end_dt = timezone.now()

        if start_param:
            start_dt = timezone.make_aware(self._parse_date(start_param), tz)
        else:
            start_dt = end_dt - timedelta(days=30)

        return start_dt, end_dt

    def _base_queryset(self, request) -> QuerySet[Order]:
        start_dt, end_dt = self._daterange_bounds(request)
        status_param = request.query_params.getlist("status") or ["confirmed", "delivered"]

        qs = (
            Order.objects.filter(
                created_at__gte=start_dt,
                created_at__lte=end_dt,
                status__in=status_param,
            )
            .only("id", "created_at", "status", "total_price")
            .prefetch_related(
                Prefetch(
                    "items",
                    queryset=OrderItem.objects.only("id", "quantity", "total_price", "bundle_snapshot"),
                )
            )
            .order_by("-created_at")
        )
        return qs

    def _units_for_order(self, order: Order) -> int:
        total = 0
        items = getattr(order, "items", None)
        if not items:
            return 0
        iterable = items.all() if hasattr(items, "all") else items
        for it in iterable:
            try:
                total += int(getattr(it, "quantity", 0) or 0)
            except Exception:
                pass
        return total

    def _series_bucket_key(self, dt: datetime) -> Tuple[str, str]:
        local_dt = timezone.localtime(dt, timezone.get_default_timezone())
        iso_year, iso_week, iso_weekday = local_dt.isocalendar()
        monday = local_dt - timedelta(days=iso_weekday - 1)
        period_key = f"{iso_year}-W{iso_week:02d}"
        date_label = monday.strftime("%Y-%m-%d")
        return period_key, date_label

    def _compute_summary_series(self, orders: List[Order]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        revenue_total = Decimal("0")
        orders_count = 0
        bucket_map: Dict[str, Dict[str, Any]] = {}

        for o in orders:
            revenue = Decimal(getattr(o, "total_price", 0) or 0)
            units = self._units_for_order(o)
            orders_count += 1
            revenue_total += revenue

            key, date_label = self._series_bucket_key(o.created_at)
            b = bucket_map.setdefault(
                key,
                {"period": key, "date": date_label, "revenue": Decimal("0"), "orders": 0, "units": 0},
            )
            b["revenue"] += revenue
            b["orders"] += 1
            b["units"] += units

        avg_order_value = float(revenue_total / orders_count) if orders_count else 0.0

        summary = {
            "revenue": float(revenue_total),
            "orders": orders_count,
            "avg_order_value": round(avg_order_value, 2),
        }

        series = [
            {
                "period": k,
                "date": v["date"],
                "revenue": float(v["revenue"]),
                "orders": v["orders"],
                "units": v["units"],
            }
            for k, v in sorted(bucket_map.items(), key=lambda kv: kv[0])
        ]

        return summary, series

    def get(self, request, *args, **kwargs):
        qs = self._base_queryset(request)
        orders_list = list(qs)

        summary, series = self._compute_summary_series(orders_list)

        item_rows = SalesOrderItemRowSerializer.from_orders(orders_list)

        limit = int(request.query_params.get("limit", 50))
        offset = int(request.query_params.get("offset", 0))
        rows_slice = item_rows[offset: offset + limit]

        payload = {
            "summary": summary,
            "series": series,
            "rows": rows_slice,
            "meta": {
                "count": len(item_rows),
                "limit": limit,
                "offset": offset,
            },
        }
        return Response(payload)


# ============================================================
# 2) Commandes — deep (unifiée)
# ============================================================

class OrdersDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=25)
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

        prefetch_items = Prefetch(
            "items",
            queryset=OrderItem.objects.only("id", "quantity", "total_price", "bundle_snapshot", "bundle_id").select_related("bundle")
        )

        orders = (
            orders
            .select_related(
                "shipping_address__city__department__region",
                "billing_address__city__department__region",
            )
            .prefetch_related(
                prefetch_items,
                # Do NOT prefetch "payments" here unless you are sure of the related_name.
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
# 3) Clients — résumé + lignes (unifiée)
# ============================================================

class CustomersDeepView(AnalyticsScopeMixin, APIView):
    """
    GET /api/(producer|admin)/analytics/customers/deep/?date_from&date_to&limit&offset&sort_by&sort_dir&include=top_products,carts
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=50)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="spent",
            allowed=["orders", "spent", "first_order", "last_order"],
        )
        include = set((request.GET.get("include") or "").split(",")) if request.GET.get("include") else set()

        orders = self.get_orders(request, date_from, date_to)
        agg = (
            orders.values("user_id")
            .annotate(
                orders=Count("id"),
                first_order=Min("created_at"),
                last_order=Max("created_at"),
                spent=Sum("total_price"),
            )
        )

        now = timezone.now()
        rows_all = []
        seg_counts = {"loyal": 0, "new": 0, "occasional": 0}

        top_per_user = {}
        if "top_products" in include:
            top_qs = (
                OrderItem.objects.filter(order__in=orders)
                .values("order__user_id", "bundle__items__product_id", "bundle__items__product__title")
                .annotate(units=Sum("quantity"))
                .order_by("order__user_id", "-units")
            )
            for r in top_qs:
                arr = top_per_user.setdefault(r["order__user_id"], [])
                if len(arr) < 10:
                    arr.append({
                        "product_id": r["bundle__items__product_id"],
                        "label": r["bundle__items__product__title"],
                        "units": int(r["units"] or 0),
                    })

        carts_by_user = {}
        if "carts" in include:
            carts_qs = Cart.objects.filter(
                is_active=True, user_id__in=agg.values_list("user_id", flat=True)
            ).prefetch_related(
                "cart_items", "cart_items__bundle", "cart_items__bundle__items", "cart_items__bundle__items__product"
            )
            for c in carts_qs:
                carts_by_user.setdefault(c.user_id, []).append({
                    "cart_id": c.id,
                    "updated_at": c.updated_at.isoformat(),
                    "items": [
                        {
                            "bundle_id": it.bundle_id,
                            "quantity": it.quantity,
                            "products": [
                                {
                                    "product_id": bi.product_id,
                                    "title": getattr(bi.product, "title", None),
                                    "per_bundle_quantity": bi.per_bundle_quantity,
                                } for bi in getattr(it.bundle, "items", []).all()
                            ]
                        }
                        for it in getattr(c, "cart_items", []).all()
                    ]
                })

        total_orders = orders.count()
        total_rev = orders.aggregate(r=Sum("total_price"))["r"] or 0
        aov = float(total_rev / total_orders) if total_orders else 0.0

        for r in agg:
            uid = r["user_id"]
            n = int(r["orders"] or 0)
            first_dt = r["first_order"] or now
            last_dt = r["last_order"] or now
            days_since_first = (now - first_dt).days
            days_since_last = (now - last_dt).days
            if n == 1 and days_since_first <= 30:
                seg = "new"
            elif (n >= 3 and (now - first_dt).days <= 90) or days_since_last <= 30:
                seg = "loyal"
            else:
                seg = "occasional"
            seg_counts[seg] += 1

            row = {
                "user_id": uid,
                "orders": n,
                "spent": float(r["spent"] or 0),
                "first_order": first_dt.isoformat(),
                "last_order": last_dt.isoformat(),
                "segment": seg,
            }
            if "top_products" in include:
                row["top_products"] = top_per_user.get(uid, [])
            if "carts" in include:
                row["carts_history"] = carts_by_user.get(uid, [])
            rows_all.append(row)

        reverse = (sort_dir == "desc")
        rows_all.sort(key=lambda x: x[sort_by], reverse=reverse)
        count = len(rows_all)
        rows = rows_all[offset: offset + limit]

        summary = {
            "customers": agg.count(),
            "orders": total_orders,
            "revenue": float(total_rev),
            "aov": aov,
            "segments": seg_counts,
        }
        return Response({"summary": summary, "rows": rows, "meta": {"count": count, "limit": limit, "offset": offset}})


# ============================================================
# 4) Paniers abandonnés — deep (unifiée)
# ============================================================


class CartsAbandonedDeepView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def _producer_from_bundle_instance(self, bundle):
        # producer = owner (user) of the company behind any product in the bundle
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
        # Uses ORM path: Product -> catalog_entry -> category
        cat = getattr(getattr(prod, "catalog_entry", None), "category", None)
        if not cat:
            return None
        return {"id": getattr(cat, "id", None), "code": getattr(cat, "code", None), "label": getattr(cat, "label", None)}

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="updated_at",
            allowed=["updated_at", "items_qty"],
        )
        reverse = (sort_dir == "desc")

        carts = Cart.objects.filter(is_active=True)

        if not self.is_admin_scope:
            company_ids = _company_ids(request.user)
            carts = carts.filter(items__bundle__items__product__company_id__in=company_ids).distinct()

        if date_from:
            carts = carts.filter(updated_at__date__gte=date_from)
        if date_to:
            carts = carts.filter(updated_at__date__lte=date_to)

        carts = list(carts.only("id", "user_id", "updated_at"))
        cart_meta = {c.id: {"user_id": c.user_id, "updated_at": c.updated_at} for c in carts}
        cart_ids = [c.id for c in carts]

        if not cart_ids:
            return Response({
                "summary": {"users_no_purchase": 0, "active_carts": 0, "avg_cart_qty": 0.0, "top_abandoned_products": []},
                "rows": [],
                "meta": {"count": 0, "limit": limit, "offset": offset},
            })

        # Prefetch: product + company.owner + catalog_entry.category
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

        # sort
        key_fn = (
            (lambda cid: cart_meta[cid]["updated_at"]) if sort_by == "updated_at"
            else (lambda cid: sum(int(getattr(it, "quantity", 0) or 0) for it in items_by_cart.get(cid, [])))
        )
        ordered_ids = sorted(items_by_cart.keys(), key=key_fn, reverse=reverse)

        count = len(ordered_ids)
        page_ids = ordered_ids[offset: offset + limit]

        rows_build = []
        top_products_counter = defaultdict(int)
        product_titles = {}  # pid -> title (for summary)

        for cid in page_ids:
            meta = cart_meta[cid]
            items_payload, total_qty = [], 0

            for it in items_by_cart.get(cid, []):
                total_qty += int(getattr(it, "quantity", 0) or 0)
                b = getattr(it, "bundle", None)

                pid, pname = self._producer_from_bundle_instance(b)
                _, co_names = self._companies_from_bundle_items(b)

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
                            "best_before_date": getattr(bi, "best_before_date", None).isoformat()
                                                 if getattr(bi, "best_before_date", None) else None,
                            "category": category_payload,  # <-- here: id/code/label from ProductCategory
                        })

                items_payload.append({
                    "cart_item_id": getattr(it, "id", None),
                    "quantity": int(getattr(it, "quantity", 0) or 0),
                    "bundle": bundle_payload,
                })

            rows_build.append({
                "cart_id": cid,
                "user_id": meta["user_id"],
                "updated_at": meta["updated_at"],
                "items_qty": total_qty,
                "items": items_payload,
            })

        active_carts = len(ordered_ids)
        avg_cart_qty = (sum(r["items_qty"] for r in rows_build) / active_carts) if active_carts else 0.0

        top_abandoned_products = []
        for pid, cnt in sorted(top_products_counter.items(), key=lambda kv: kv[1], reverse=True)[:10]:
            if pid is None:
                continue
            top_abandoned_products.append({
                "product_id": pid,
                "label": product_titles.get(pid),  # product title
                "count": cnt
            })

        return Response({
            "summary": {
                "users_no_purchase": 0,
                "active_carts": active_carts,
                "avg_cart_qty": avg_cart_qty,
                "top_abandoned_products": top_abandoned_products,
            },
            "rows": CartsAbandonedRowSerializer(rows_build, many=True).data,
            "meta": {"count": count, "limit": limit, "offset": offset},
        })
    


# ============================================================
# 5) Catalogue — deep (unifiée)
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
# 6) Santé produits/bundles — unifiée (avec producteurs)
# ============================================================

class ProductsHealthView(AnalyticsScopeMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        low_threshold = int(request.GET.get("low_threshold", "5"))
        dlc_days = int(request.GET.get("dlc_days", "7"))

        p_limit, p_offset = _pagination(request)
        p_sort_by, p_sort_dir = _sort_params(request, default_sort_by="stock", allowed=["stock", "sold", "title"])

        b_limit, b_offset = p_limit, p_offset
        b_sort_by, b_sort_dir = _sort_params(request, default_sort_by="stock", allowed=["stock", "sold", "title"])

        if self.is_admin_scope:
            prods_qs = (
                Product.objects
                .select_related("company", "company__owner")
                .only("id", "title", "stock", "sold_units", "company_id")
            )
            bundles_qs = (
                ProductBundle.objects
                .prefetch_related(
                    "items",
                    "items__product",
                    "items__product__company",
                    "items__product__company__owner",
                )
                .all()
            )
            dlc_filter = {}
        else:
            co_ids = _company_ids(request.user)
            prods_qs = (
                Product.objects
                .filter(company_id__in=co_ids)
                .select_related("company", "company__owner")
                .only("id", "title", "stock", "sold_units", "company_id")
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
                )
            )
            dlc_filter = {"product__company_id__in": co_ids}

        # ---- Products
        prows = []
        for p in prods_qs:
            comp = getattr(p, "company", None)
            owner_name = _owner_name_from_company(comp)              # <-- producer_names = owner
            company_name = getattr(comp, "name", None)               # <-- company_names = company
            row = {
                "product_id": p.id,
                "title": p.title,
                "stock": int(p.stock or 0),
                "sold": int(getattr(p, "sold_units", 0) or 0),
                "level": "red" if (p.stock or 0) <= 0 else ("yellow" if (p.stock or 0) <= low_threshold else "ok"),
                "producer_ids": [getattr(p, "company_id", None)] if getattr(p, "company_id", None) else [],
                "producer_names": [owner_name] if owner_name else [],
                "company_names": [company_name] if company_name else [],
            }
            prows.append(row)

        reverse_p = (p_sort_dir == "desc")
        prows.sort(key=lambda x: (x[p_sort_by], x["title"] if p_sort_by != "title" else ""), reverse=reverse_p)
        p_count = len(prows)
        products_rows = prows[p_offset: p_offset + p_limit]

        # ---- Bundles
        brows = []
        for b in bundles_qs:
            company_ids, company_names, owner_names = _bundle_companies_and_owners(b)
            brows.append({
                "bundle_id": b.id,
                "title": getattr(b, "title", f"Bundle {b.id}"),
                "stock": int(getattr(b, "stock", 0) or 0),
                "sold": int(getattr(b, "sold_bundles", 0) or 0),
                "producer_ids": company_ids,              # keep ids as company ids
                "producer_names": [n for n in owner_names if n],    # owner names only
                "company_names": [n for n in company_names if n],   # company names only
            })

        reverse_b = (b_sort_dir == "desc")
        brows.sort(key=lambda x: (x[b_sort_by], x["title"] if b_sort_by != "title" else ""), reverse=reverse_b)
        b_count = len(brows)
        bundles_rows = brows[b_offset: b_offset + b_limit]

        # ---- DLC soon
        today = timezone.localdate()
        limit_d = today + timedelta(days=dlc_days)
        dlc_qs = (
            ProductBundleItem.objects
            .filter(
                best_before_date__isnull=False,
                best_before_date__lte=limit_d,
                bundle__stock__gt=0,
                is_active=True,
                **dlc_filter,
            )
            .select_related("product", "bundle", "product__company", "product__company__owner")
            .order_by("best_before_date")
        )
        dlc_risque = [
            {
                "bundle_id": it.bundle_id,
                "product_id": it.product_id,
                "product": getattr(it.product, "title", None),
                "best_before_date": it.best_before_date.isoformat(),
                "bundle_stock": int(getattr(it.bundle, "stock", 0)),
                "producer_ids": [getattr(it.product, "company_id", None)] if getattr(it.product, "company_id", None) else [],
                "producer_names": [
                    _owner_name_from_company(getattr(it.product, "company", None))
                ] if getattr(it.product, "company", None) else [],
                "company_names": [
                    getattr(getattr(it.product, "company", None), "name", None)
                ] if getattr(it.product, "company", None) else [],
            }
            for it in dlc_qs
        ]

        summary = {
            "products": {
                "count": p_count,
                "zero_stock": sum(1 for r in prows if r["stock"] <= 0),
                "low_stock": sum(1 for r in prows if 0 < r["stock"] <= low_threshold),
            },
            "bundles": {"count": b_count},
            "dlc_en_risque": len(dlc_risque),
        }

        return Response({
            "summary": summary,
            "rows": {
                "products": {"data": products_rows, "meta": {"count": p_count, "limit": p_limit, "offset": p_offset}},
                "bundles": {"data": bundles_rows, "meta": {"count": b_count, "limit": b_limit, "offset": b_offset}},
            },
            "dlc_risque": dlc_risque,
        })


# ============================================================
# 7) Impact — totaux + lignes (unifiée)
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

        orders = self.get_orders(request, date_from, date_to).prefetch_related(
            "items",
            "items__bundle",
        ).only(
            "id",
            "created_at",
            "order_total_avoided_waste_kg",
            "order_total_avoided_co2_kg",
            "order_total_savings",
            "status",
        )

        rows_all: List[Dict[str, Any]] = []
        for o in orders:
            items = o.items.all() if hasattr(o.items, "all") else (o.items or [])
            for it in items:
                snap = getattr(it, "bundle_snapshot", None)
                snap = snap if isinstance(snap, dict) else {}

                # Metrics per item
                aw = getattr(it, "order_item_total_avoided_waste_kg", None)
                ac = getattr(it, "order_item_total_avoided_co2_kg", None)
                sv = getattr(it, "order_item_savings", None)
                avoided_waste = float(aw or snap.get("avoided_waste_kg") or 0.0)
                avoided_co2 = float(ac or snap.get("avoided_co2_kg") or 0.0)
                savings_eur = float(sv or 0.0)

                # Producer (person) from snapshot, fallback to bundle.producer_data
                producer_id = snap.get("producer_id")
                producer_name = snap.get("producer_name")
                if producer_id is None or not producer_name:
                    b = getattr(it, "bundle", None)
                    if b is not None:
                        pdata = getattr(b, "producer_data", None)
                        if isinstance(pdata, dict):
                            producer_id = producer_id or pdata.get("id") or getattr(b, "producer_id", None)
                            producer_name = producer_name or pdata.get("public_display_name") or pdata.get("display_name")

                # Company from snapshot, fallback to bundle.company_data
                company_id = snap.get("company_id")
                company_name = snap.get("company_name")
                if company_id is None or not company_name:
                    b = getattr(it, "bundle", None)
                    if b is not None:
                        cdata = getattr(b, "company_data", None)
                        if isinstance(cdata, dict):
                            company_id = company_id or cdata.get("id") or getattr(b, "company_id", None)
                            company_name = company_name or cdata.get("name") or getattr(b, "company_name", None)

                rows_all.append({
                    "order_id": o.id,
                    "created_at": o.created_at.isoformat(),
                    "status": o.status,

                    "item_id": it.id,
                    "bundle_id": getattr(it, "bundle_id", None),
                    "bundle_title": snap.get("title"),

                    "avoided_waste_kg": round(avoided_waste, 3),
                    "avoided_co2_kg": round(avoided_co2, 3),
                    "savings_eur": round(savings_eur, 2),

                    "producer_id": producer_id,
                    "producer_name": producer_name,
                    "company_id": company_id,
                    "company_name": company_name,
                })

        reverse = (sort_dir == "desc")
        if sort_by == "created_at":
            rows_all.sort(key=lambda r: r["created_at"], reverse=reverse)
        else:
            rows_all.sort(key=lambda r: r.get(sort_by, 0.0), reverse=reverse)

        count = len(rows_all)
        rows = rows_all[offset: offset + limit]

        agg = orders.aggregate(
            waste=Sum("order_total_avoided_waste_kg"),
            co2=Sum("order_total_avoided_co2_kg"),
            savings=Sum("order_total_savings"),
        )
        summary = {
            "avoided_waste_kg": float(agg["waste"] or 0.0),
            "avoided_co2_kg": float(agg["co2"] or 0.0),
            "savings_eur": float(agg["savings"] or 0.0),
        }

        return Response({"summary": summary, "rows": rows, "meta": {"count": count, "limit": limit, "offset": offset}})
    


# ============================================================
# 8) Ventes par catégorie — deep (unifiée)
# ============================================================
class SalesByCategoryDeepView(AnalyticsScopeMixin, APIView):
    """
    GET /api/(producer|admin)/analytics/sales/by-category/deep/?date_from&date_to&limit&offset&sort_by&sort_dir
    Returns: by_category + rows.products + rows.items (each item row includes created_at/order_date and producer/company)
    """
    permission_classes = [IsAuthenticated]

    def _producers_from_snapshot_with_owners(self, snap: dict, owner_cache: dict):
        """
        From a bundle snapshot, gather company ids/names and resolve owner (producer) names via Company.owner.
        Returns (producer_ids=company_ids, producer_names=owner_names, company_names=company_names).
        """
        ids, cnames = [], []
        seen = set()

        # Top-level company fields (if present)
        top_cid = snap.get("company_id")
        top_cname = snap.get("company_name")
        if top_cid is not None and top_cid not in seen:
            seen.add(top_cid)
            ids.append(top_cid)
            cnames.append(top_cname)

        # Products[]
        for p in (snap.get("products") or []):
            cid = p.get("company_id")
            cname = p.get("company_name")
            if cid is None or cid in seen:
                continue
            seen.add(cid)
            ids.append(cid)
            cnames.append(cname)

        # Resolve owner names from Company.owner
        onames = []
        missing = [cid for cid in ids if cid not in owner_cache]
        if missing:
            for cid, owner_name in (
                Company.objects.filter(id__in=missing)
                .select_related("owner")
                .values_list("id", "owner__public_display_name", "owner__first_name", "owner__last_name", "owner__username", "owner__email")
            ):
                # reconstruct a display name similar to _user_display_name
                display = owner_name or " ".join(
                    x for x in [
                        (_ or "").strip() for _ in [owner_name, None]  # placeholder
                    ] if x
                )
                # Above query doesn't give first/last separately in one slot; re-run a more explicit way:
                # For safety fallback to email/username when public_display_name is missing.
                if not display:
                    # fetch again with more fields (kept simple here)
                    display = None
                owner_cache[cid] = display  # may be None; we fill below with better fallback

        # Fill onames with cached names, fallback to snapshot.producer_name if available
        fallback_name = snap.get("producer_name")
        for cid in ids:
            name = owner_cache.get(cid)
            if not name:
                name = fallback_name
            onames.append(name)

        return ids, onames, cnames

    def _resolve_producers(self, oi, owner_cache):
        """
        Prefer live bundle (accurate owner names). Fallback to snapshot.
        Returns (producer_ids=company_ids, producer_names=owner_names, company_names=company_names).
        """
        b = getattr(oi, "bundle", None)
        if b is not None:
            pids, owner_names, company_names = _bundle_producers_and_companies(b)
            return pids, owner_names, company_names

        snap = getattr(oi, "bundle_snapshot", None) or {}
        return self._producers_from_snapshot_with_owners(snap, owner_cache)

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=50)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="revenue",
            allowed=["revenue", "orders", "units", "category_name"],
        )

        orders = self.get_orders(request, date_from, date_to)

        # Prefetch order date + bundle graph to resolve companies/owners without N+1
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

        cat = {}    # {(cat_id, cat_name): {"revenue": float, "orders": set(), "units": int}}
        prod = {}   # {product_id: {... aggregated ...}}
        ultra_rows = []
        owner_cache = {}  # company_id -> owner display name

        for oi in items:
            q = int(oi.quantity or 0)
            total = float(oi.total_price or 0.0)

            parts = list(_iter_snapshot_products(oi))  # (product_id, title, cat_id, cat_name, pbq)
            total_pbq = sum(pbq for *_, pbq in parts) or 1

            # Unified producers: ids = company_ids; producer_names = owner display names; company_names = company names
            pids_company, owner_names, company_names = self._resolve_producers(oi, owner_cache)

            seen_cat_keys = set()
            seen_prod_ids = set()

            # Order timestamp for each item row
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
                    "created_at": created_iso,         # <-- added
                    "order_date": order_date,           # <-- added (YYYY-MM-DD)
                    "producer_ids": list(pids_company),
                    "producer_names": [n for n in owner_names if n],
                    "company_names": [n for n in company_names if n],
                })

        # by_category
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
        reverse = (sort_dir == "desc")
        by_category_all.sort(
            key=lambda x: (x[sort_by] if sort_by != "category_name" else (x["category_name"] or "")),
            reverse=reverse,
        )
        c_count = len(by_category_all)
        by_category = by_category_all[offset: offset + limit]

        # product rows
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




# ============================================================
# 9) Paiements — deep (unifiée)
# ============================================================

class PaymentsDeepView(AnalyticsScopeMixin, APIView):
    """
    GET /api/(producer|admin)/analytics/payments/deep/?date_from&date_to&limit&offset&sort_by&sort_dir
    -> rows: 1 registro por item de pedido (OrderItem), con productores por item
    """
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

        items_qs = (
            OrderItem.objects
            .filter(order__in=orders)
            .select_related("order", "bundle")
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
                "bundle_snapshot", "bundle_id",
                "order__created_at", "order__status", "order__payment_method_snapshot",
            )
            .order_by("-order__created_at", "-id")
        )

        total_items_count = items_qs.count()
        page_items = list(items_qs[offset: offset + limit])

        needed_cids = set()
        snapshot_company_names_by_cid = {}

        def _snapshot_companies(snap: dict):
            cids, cnames, seen = [], [], set()
            if not isinstance(snap, dict):
                return cids, cnames
            top_cid = snap.get("company_id")
            top_cname = snap.get("company_name")
            if top_cid is not None and top_cid not in seen:
                seen.add(top_cid); cids.append(top_cid); cnames.append(top_cname)
            for p in (snap.get("products") or []):
                cid = p.get("company_id"); cname = p.get("company_name")
                if cid is None or cid in seen:
                    continue
                seen.add(cid); cids.append(cid); cnames.append(cname)
            return cids, cnames

        for it in page_items:
            b = getattr(it, "bundle", None)
            if b:
                continue
            snap = getattr(it, "bundle_snapshot", None) or {}
            cids, cnames = _snapshot_companies(snap)
            for cid, cname in zip(cids, cnames):
                if cid is None:
                    continue
                needed_cids.add(cid)
                if cname:
                    snapshot_company_names_by_cid[cid] = cname

        owners_cache = {}
        if needed_cids:
            for cid, pdn, fn, ln, un, em, cname in (
                Company.objects.filter(id__in=needed_cids)
                .select_related("owner")
                .values_list(
                    "id",
                    "owner__public_display_name",
                    "owner__first_name",
                    "owner__last_name",
                    "owner__username",
                    "owner__email",
                    "name",
                )
            ):
                display = pdn or " ".join(x for x in [(fn or "").strip(), (ln or "").strip()] if x) or un or em
                owners_cache[cid] = display
                snapshot_company_names_by_cid.setdefault(cid, cname)

        rows_all = []
        by_method = {}

        def _method_key_from_order(order):
            snap = getattr(order, "payment_method_snapshot", None)
            if isinstance(snap, dict):
                return f"{(snap.get('type') or 'unknown')}:{(snap.get('provider') or 'unknown')}"
            return "unknown:unknown"

        for it in page_items:
            o = getattr(it, "order", None)
            created_at = getattr(o, "created_at", None)
            created_iso = created_at.isoformat() if created_at else None
            status = getattr(o, "status", None)
            status_norm = "paid" if status in {"confirmed", "delivered"} else status
            method_key = _method_key_from_order(o)

            amount = float(getattr(it, "total_price", 0.0) or 0.0)

            b = getattr(it, "bundle", None)
            if b:
                cids, company_names, owner_names = _bundle_companies_and_owners(b)
                seen = set()
                triplets = []
                for cid, cname, oname in zip(cids, company_names, owner_names):
                    if cid in seen or cid is None:
                        continue
                    seen.add(cid)
                    triplets.append((cid, cname, oname))
                producer_ids = [cid for cid, _, _ in triplets]
                company_names = [c for _, c, _ in triplets if c]
                producer_names = [n for _, _, n in triplets if n]
            else:
                snap = getattr(it, "bundle_snapshot", None) or {}
                cids, cnames = _snapshot_companies(snap)
                producer_ids, company_names, producer_names, seen = [], [], [], set()
                for cid, cname in zip(cids, cnames):
                    if cid is None or cid in seen:
                        continue
                    seen.add(cid)
                    producer_ids.append(cid)
                    company_names.append(cname or snapshot_company_names_by_cid.get(cid))
                    producer_names.append(owners_cache.get(cid))

                company_names = [x for x in company_names if x]
                producer_names = [x for x in producer_names if x]

            rows_all.append({
                "order_id": getattr(it, "order_id", None),
                "order_item_id": it.id,
                "created_at": created_iso,
                "method": method_key,
                "status": status_norm,
                "amount": amount,
                "producer_ids": producer_ids,
                "producer_names": producer_names,
                "company_names": company_names,
            })

            agg = by_method.setdefault(method_key, {"count": 0, "revenue": 0.0, "success": 0, "total": 0})
            agg["count"] += 1
            agg["revenue"] += amount
            agg["total"] += 1
            if status_norm in {"paid", "authorized", "succeeded", "confirmed", "delivered"}:
                agg["success"] += 1

        if sort_by in {"method", "status", "created_at"}:
            rows_all.sort(key=lambda x: x.get(sort_by) or "", reverse=reverse)
        else:
            rows_all.sort(key=lambda x: x.get(sort_by, 0), reverse=reverse)

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
# 10) Cohortes mensuelles — deep (unifiée)
# ============================================================


class CohortsMonthlyView(AnalyticsScopeMixin, APIView):
    """
    GET /api/(producer|admin)/analytics/cohorts/monthly/?date_from&date_to

    Groups customers by the month of their first purchase (cohort_month).
    Each cohort contains periods at monthly offsets from the first purchase,
    with aggregated orders, revenue, and active customers.

    Added company-level breakdown:
      - cohorts_company: aggregated metrics per (cohort_month, company)
      - rows_company: one row per company (never multiple companies in the same row)

    Producer metadata for frontend filtering:
      - producer_ids   -> USER IDs (Company.owner.id)
      - producer_names -> Owner display names (Company.owner)
      - company_names  -> Company names (Company.name)
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

        # ------------------------------------------------------------
        # Per-order company revenue mapping, built from OrderItem(s)
        # ------------------------------------------------------------
        order_ids = list(orders.values_list("id", flat=True))

        # order_id -> {company_id: revenue_share_float}
        order_company_revenue = defaultdict(lambda: defaultdict(float))
        # company_id -> company_name (best known)
        company_name_by_id = {}

        # Collect all company ids seen (to resolve owners in batch)
        all_company_ids = set()

        # Prefetch bundle graph to avoid N+1 and include money fields
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

        for it in oi_qs:
            item_total = float(getattr(it, "total_price", 0.0) or 0.0)
            if item_total <= 0:
                continue

            snap = getattr(it, "bundle_snapshot", None) or {}
            b = getattr(it, "bundle", None)

            # Build per-company PBQ map for this item
            # company_pbq: {company_id: sum_of_per_bundle_quantity}
            company_pbq = defaultdict(int)
            company_names_local = {}

            # Prefer snapshot products (carry company_id/company_name + per_bundle_quantity)
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

            # If no snapshot products, fallback to live bundle instance
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

            # As a last resort, consider snapshot top-level company (single-company bundle)
            if not company_pbq and isinstance(snap, dict):
                top_cid = snap.get("company_id")
                top_cname = snap.get("company_name")
                if top_cid is not None:
                    company_pbq[top_cid] += 1
                    if top_cname:
                        company_names_local[top_cid] = top_cname

            if not company_pbq:
                continue  # cannot attribute this item

            # Revenue split across companies in this item
            total_pbq = sum(company_pbq.values())
            n_companies = len(company_pbq)
            for cid, pbq in company_pbq.items():
                if cid is None:
                    continue
                if total_pbq > 0:
                    share = (pbq / total_pbq)
                else:
                    # No PBQ information: split evenly
                    share = 1.0 / n_companies
                rev_share = item_total * share
                order_company_revenue[it.order_id][cid] += rev_share

                # Track company name and seen company ids
                if cid not in company_name_by_id and cid in company_names_local:
                    company_name_by_id[cid] = company_names_local[cid]
                all_company_ids.add(cid)

        # ------------------------------------------------------------
        # Resolve company -> owner (user) ids and display names
        # ------------------------------------------------------------
        company_to_owner_id = {}
        user_display_by_id = {}
        if all_company_ids:
            for comp in Company.objects.filter(id__in=all_company_ids).select_related("owner"):
                owner = getattr(comp, "owner", None)
                uid = getattr(owner, "id", None)
                company_to_owner_id[comp.id] = uid
                if uid is not None and uid not in user_display_by_id:
                    user_display_by_id[uid] = _user_display_name(owner)
                # Ensure we still have a name even if it didn't come from snapshot/bundle
                if comp.id not in company_name_by_id:
                    company_name_by_id[comp.id] = getattr(comp, "name", None)

        # ------------------------------------------------------------
        # Build original cohorts + per-user rows (unchanged behavior)
        # ------------------------------------------------------------
        cohorts = {}  # key -> {"customers": set(), "periods": {offset -> {...}}, "producer_user_ids": set(), "company_names": set()}
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

            # Attach producer user IDs and company names present in this order
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

        # Original cohorts output
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
                "producer_ids": uid_list,                         # USER IDs
                "producer_names": producer_names,                 # Owner display names
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
                "producer_ids": uid_list,                        # USER IDs
                "producer_names": producer_names,
                "company_names": sorted(list(info["company_names"])),
            })

        # ------------------------------------------------------------
        # NEW: company-level cohorts + rows (one row per company)
        # ------------------------------------------------------------
        cohorts_company_map = defaultdict(lambda: {
            "customers": set(),
            "periods": defaultdict(lambda: {"orders": 0, "revenue": 0.0, "customers": set()}),
            # metadata filled later
        })

        for o in orders:
            uid = o.user_id
            if uid not in first_by_user:
                continue
            cohort_start = first_by_user[uid]
            offset = (o.created_at.year - cohort_start.year) * 12 + (o.created_at.month - cohort_start.month)
            ckey = cohort_start.strftime("%Y-%m")

            comp_rev_map = order_company_revenue.get(o.id, {})
            # For each company present in this order, increment its per-company cohort
            for cid, rev in comp_rev_map.items():
                key = (ckey, cid)
                entry = cohorts_company_map[key]
                entry["customers"].add(uid)
                entry["periods"][offset]["orders"] += 1
                entry["periods"][offset]["revenue"] += float(rev or 0.0)
                entry["periods"][offset]["customers"].add(uid)

        # Build company-level output
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
                "producer_id": owner_uid,         # USER ID
                "producer_name": owner_name,
                "customers": len(v["customers"]),
                "periods": periods,
            })

            rows_company.append({
                "cohort_month": ckey,
                "company_id": cid,
                "company_name": cname,
                "producer_id": owner_uid,         # USER ID
                "producer_name": owner_name,
                "periods": periods,               # keep the same detailed breakdown per row
            })

        summary = {
            "cohorts": len(cohorts_out),
            "customers": sum(c["customers"] for c in cohorts_out),
            "revenue": round(total_revenue, 2),
            "orders": total_orders,
        }

        return Response({
            "summary": summary,
            "cohorts": cohorts_out,             # original aggregate across companies
            "rows": rows,                       # original per-user rows
            "cohorts_company": cohorts_company, # NEW: per-company cohorts
            "rows_company": rows_company,       # NEW: one row per company
        })
    


# ============================================================
# 11) Géographie — deep (unifiée, avec producteurs)
# ============================================================

class GeoDeepView(AnalyticsScopeMixin, APIView):
    """
    GET /api/(producer|admin)/analytics/geo/deep/?level=department|region|city&date_from&date_to&limit&offset&sort_by&sort_dir

    - by_zone: revenue/orders/customers aggregated by geography level.
    - rows: one row per order item product.
      Per-row identity:
        * producer_id   -> owner user id (Company.owner.id)
        * producer_name -> owner display name
        * company_id    -> Company.id
        * company_name  -> Company.name
      Order-level arrays (preserved for compatibility):
        * producer_ids   -> list of company ids involved in the order
        * producer_names -> list of owner display names (resolved from those company ids)
        * company_names  -> list of company names
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)

        level = request.GET.get("level", "department")
        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=100)
        sort_by, sort_dir = _sort_params(
            request,
            default_sort_by="revenue",
            allowed=["revenue", "orders", "customers", "zone"],
        )
        reverse = (sort_dir == "desc")

        # Orders with addresses for zone resolution
        orders = self.get_orders(request, date_from, date_to).select_related(
            "shipping_address", "billing_address",
            "shipping_address__city", "billing_address__city",
            "shipping_address__city__department", "billing_address__city__department",
            "shipping_address__city__department__region", "billing_address__city__department__region",
        )

        def zone_from_order(o):
            def from_addr(addr):
                if not addr:
                    return None
                city = getattr(addr, "city", None)
                dep = getattr(city, "department", None) if city else None
                reg = getattr(dep, "region", None) if dep else None
                if level == "region":
                    return getattr(reg, "code", None)
                if level == "city":
                    return getattr(city, "postal_code", None) or getattr(city, "name", None)
                return getattr(dep, "code", None)
            z = from_addr(getattr(o, "shipping_address", None)) or from_addr(getattr(o, "billing_address", None))
            if z:
                return z
            snap = getattr(o, "shipping_address_snapshot", None) or getattr(o, "billing_address_snapshot", None) or {}
            if isinstance(snap, dict):
                if level == "region":
                    return snap.get("region_code") or snap.get("region")
                if level == "city":
                    return snap.get("postal_code") or snap.get("city")
                return snap.get("department_code") or snap.get("department")
            return "NA"

        # Aggregate by zone (order-level)
        agg = defaultdict(lambda: {"revenue": 0.0, "orders": 0, "customers": set()})
        order_zone = {}
        for o in orders:
            z = zone_from_order(o) or "NA"
            a = agg[z]
            a["revenue"] += float(getattr(o, "total_price", 0) or 0.0)
            a["orders"] += 1
            if getattr(o, "user_id", None):
                a["customers"].add(o.user_id)
            order_zone[o.id] = z

        # Order-level arrays from snapshots (company ids and names)
        order_ids = list(order_zone.keys())
        order_arrays = _attach_producers_to_order_rows(order_ids)

        # Batch resolve owner/name for all company ids present in arrays
        all_company_ids_for_arrays = set()
        for meta in order_arrays.values():
            all_company_ids_for_arrays.update([cid for cid in meta.get("producer_ids", []) if cid is not None])

        owner_name_by_company_id, owner_id_by_company_id, company_name_by_company_id = {}, {}, {}
        if all_company_ids_for_arrays:
            for comp in Company.objects.filter(id__in=all_company_ids_for_arrays).select_related("owner"):
                owner_name_by_company_id[comp.id] = _user_display_name(getattr(comp, "owner", None))
                owner_id_by_company_id[comp.id] = getattr(getattr(comp, "owner", None), "id", None)
                company_name_by_company_id[comp.id] = getattr(comp, "name", None)

        # Build item-level rows
        items_qs = (
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
                "bundle__items__product__catalog_entry__category",
                "bundle__items__product__company",
                "bundle__items__product__company__owner",
            )
        )

        rows = []
        # Caches to avoid N+1
        company_cache_by_id = {}      # id -> {"owner_id", "owner_name", "company_name"}
        company_cache_by_name = {}    # name -> {"company_id", "owner_id", "owner_name"}

        for oi in items_qs:
            q = int(getattr(oi, "quantity", 0) or 0)
            total = float(getattr(oi, "total_price", 0.0) or 0.0)
            if q <= 0 and total <= 0:
                continue

            snap = getattr(oi, "bundle_snapshot", None) or {}

            # Top-level snapshot fallbacks (single-company bundles)
            snap_top_company_id = snap.get("company_id")
            snap_top_company_name = snap.get("company_name")
            snap_top_producer_id = snap.get("producer_id")      # user id
            snap_top_producer_name = snap.get("producer_name")  # owner display name

            # Build product parts from snapshot first (prefer products[] because it carries per-product quantities)
            parts = []
            for p in (snap.get("products") or []):
                pid = p.get("product_id")
                if pid is None:
                    continue
                parts.append({
                    "product_id": pid,
                    "label": p.get("product_title") or f"Product {pid}",
                    "category_id": p.get("category_id"),
                    "category_name": p.get("category_name"),
                    "pbq": int(p.get("per_bundle_quantity", 1)),
                    "company_id": p.get("company_id"),
                    "company_name": p.get("company_name"),
                })

            # Fallback to live bundle if snapshot is incomplete
            if not parts and getattr(oi, "bundle", None) is not None:
                b = getattr(oi, "bundle", None)
                iterable = b.items.all() if hasattr(b.items, "all") else (b.items or [])
                for bi in iterable:
                    prod = getattr(bi, "product", None)
                    if not prod:
                        continue
                    cat = getattr(getattr(prod, "catalog_entry", None), "category", None)
                    comp = getattr(prod, "company", None)
                    parts.append({
                        "product_id": getattr(prod, "id", None),
                        "label": getattr(prod, "title", None),
                        "category_id": getattr(cat, "id", None),
                        "category_name": getattr(cat, "label", None),
                        "pbq": int(getattr(bi, "quantity", 1) or 1),
                        "company_id": getattr(comp, "id", None) if comp else None,
                        "company_name": getattr(comp, "name", None) if comp else None,
                    })

            if not parts:
                # Nothing we can attribute
                continue

            # If products[] lack company info but top-level snapshot has it, apply it (single-company bundle)
            if (snap_top_company_id is not None or snap_top_company_name) and all(p.get("company_id") is None for p in parts):
                for p in parts:
                    p["company_id"] = p.get("company_id") or snap_top_company_id
                    p["company_name"] = p.get("company_name") or snap_top_company_name

            total_pbq = sum(p["pbq"] for p in parts) or 1
            created_iso = getattr(getattr(oi, "order", None), "created_at", None)
            created_iso = created_iso.isoformat() if created_iso else None
            z = order_zone.get(oi.order_id, "NA")

            # Preserve order-level arrays
            arrays_meta = order_arrays.get(oi.order_id, {"producer_ids": [], "producer_names": []})
            order_company_ids = arrays_meta.get("producer_ids", [])
            order_company_names = arrays_meta.get("producer_names", [])
            order_owner_names = [owner_name_by_company_id.get(cid) for cid in order_company_ids if owner_name_by_company_id.get(cid)]

            # Build name->cid map for this order
            name_to_cid_for_order = {}
            for cid in order_company_ids:
                nm = company_name_by_company_id.get(cid)
                if nm:
                    name_to_cid_for_order[nm] = cid
            for nm in order_company_names:
                if nm and nm not in name_to_cid_for_order:
                    # lazy DB lookup by name (cached)
                    if nm in company_cache_by_name:
                        name_to_cid_for_order[nm] = company_cache_by_name[nm]["company_id"]
                    else:
                        comp = Company.objects.filter(name=nm).select_related("owner").first()
                        if comp:
                            company_cache_by_name[nm] = {
                                "company_id": comp.id,
                                "owner_id": getattr(getattr(comp, "owner", None), "id", None),
                                "owner_name": _user_display_name(getattr(comp, "owner", None)),
                            }
                            name_to_cid_for_order[nm] = comp.id

            for p in parts:
                share = (p["pbq"] / total_pbq) if total_pbq else 0.0
                revenue_share = total * share
                units_share = q * p["pbq"]

                cid = p.get("company_id")
                cname = p.get("company_name")

                # Fill company_id from name using order-level mapping
                if cid is None and cname:
                    mapped = name_to_cid_for_order.get(cname)
                    if mapped is not None:
                        cid = mapped

                # If still missing name, fill from batch cache when we already know cid
                if not cname and cid in company_name_by_company_id:
                    cname = company_name_by_company_id[cid]

                # Resolve owner info. Prefer caches/batch; fall back to DB; finally use snapshot top-level producer if it matches the company.
                owner_id = None
                owner_name = None
                if cid is not None:
                    if cid in company_cache_by_id:
                        owner_id = company_cache_by_id[cid]["owner_id"]
                        owner_name = company_cache_by_id[cid]["owner_name"]
                        cname = cname or company_cache_by_id[cid]["company_name"]
                    elif cid in owner_id_by_company_id or cid in owner_name_by_company_id:
                        owner_id = owner_id_by_company_id.get(cid)
                        owner_name = owner_name_by_company_id.get(cid)
                        cname = cname or company_name_by_company_id.get(cid)
                        company_cache_by_id[cid] = {"owner_id": owner_id, "owner_name": owner_name, "company_name": cname}
                    else:
                        comp = Company.objects.filter(id=cid).select_related("owner").first()
                        if comp:
                            owner_id = getattr(getattr(comp, "owner", None), "id", None)
                            owner_name = _user_display_name(getattr(comp, "owner", None))
                            cname = cname or getattr(comp, "name", None)
                            company_cache_by_id[cid] = {"owner_id": owner_id, "owner_name": owner_name, "company_name": cname}

                # Final fallback: if still missing owner and snapshot top-level matches this company, use it.
                if owner_id is None and snap_top_company_id is not None and cid == snap_top_company_id:
                    owner_id = snap_top_producer_id
                    owner_name = owner_name or snap_top_producer_name

                rows.append({
                    "order_id": oi.order_id,
                    "order_item_id": oi.id,
                    "created_at": created_iso,
                    "zone": z,

                    "product_id": p["product_id"],
                    "label": p["label"],
                    "category_id": p["category_id"],
                    "category_name": p["category_name"],
                    "quantity_bundle": q,
                    "per_bundle_quantity": p["pbq"],
                    "units_share": units_share,
                    "revenue_share": round(revenue_share, 4),

                    "producer_id": owner_id,
                    "producer_name": owner_name,
                    "company_id": cid,
                    "company_name": cname,

                })

        # Build by_zone output
        by_zone_all = [
            {
                "zone": z,
                "revenue": round(v["revenue"], 2),
                "orders": v["orders"],
                "customers": len(v["customers"]),
            }
            for z, v in agg.items()
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