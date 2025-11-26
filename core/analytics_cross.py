# core/analytics_cross.py
from __future__ import annotations
from collections import defaultdict, Counter
from datetime import datetime
from statistics import median
from typing import Any, Dict, List, Tuple, Optional

from django.db.models import Sum, Count, Avg, F, Q, Min, Max, Prefetch
from django.utils import timezone
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from decimal import Decimal

from .models import (
    Order,
    OrderItem,
    ProductBundle,
    ProductBundleItem,
    Product,
    ProductCatalog,
    ProductCategory,
    CartItem,
    Favorite,
)
from .analytics_endpoints import (
    VALID_STATUSES,
    _date_range,
    _pagination,
    _sort_params,
    _bucket,
    _company_ids,
    _orders_for_producer,
    _iter_snapshot_products,  # -> (product_id, title, cat_id, cat_name, per_bundle_qty)
)
from .analytics_scope import AnalyticsScopeMixin


# ---------------------------------------------------------------------
# Utilitaires locaux
# ---------------------------------------------------------------------

def _extract_snapshot_companies(oi: OrderItem) -> List[Tuple[int, str]]:
    snap = getattr(oi, "bundle_snapshot", None) or {}
    prods = snap.get("products") or []
    out, seen = [], set()
    for p in prods:
        cid, cname = p.get("company_id"), p.get("company_name")
        if cid and cid not in seen:
            seen.add(cid)
            out.append((cid, cname or f"Company {cid}"))
    return out

def _safe_float(x) -> float:
    try:
        return float(x or 0)
    except Exception:
        return 0.0


def _bucket_anchor_date(dt, bucket: str) -> str:
    """
    Renvoie la date d’ancrage (YYYY-MM-DD) du bucket temporel.
    - day   -> date exacte de l’événement
    - week  -> lundi ISO de la semaine correspondante
    - month -> premier jour du mois
    """
    b = (bucket or "week").lower()
    if b == "day":
        return dt.date().isoformat()
    if b == "month":
        return f"{dt.year:04d}-{dt.month:02d}-01"
    iso = dt.isocalendar()
    return datetime.fromisocalendar(iso.year, iso.week, 1).date().isoformat()


def _producer_scope_orders(user, date_from, date_to):
    return _orders_for_producer(user, date_from, date_to)


def _admin_scope_orders(date_from, date_to):
    qs = Order.objects.filter(status__in=VALID_STATUSES)
    if date_from:
        qs = qs.filter(created_at__date__gte=date_from)
    if date_to:
        qs = qs.filter(created_at__date__lte=date_to)
    return qs


def _item_queryset_for_orders(orders_qs):
    return OrderItem.objects.filter(order__in=orders_qs)


def _bundle_producer_names_from_snapshot(snapshot: dict) -> List[str]:
    """
    Extrait une liste de noms de producteurs depuis un snapshot de bundle.
    """
    names: List[str] = []
    seen = set()
    for p in (snapshot or {}).get("products", []) or []:
        cid = p.get("company_id")
        cname = p.get("company_name")
        if cid and cid not in seen:
            seen.add(cid)
            names.append(cname or f"Company {cid}")
    return names


def _bundle_producer_names_from_pbis(bundle_id: int) -> List[str]:
    """
    Construit la liste des producteurs d’un bundle via ProductBundleItem -> Product.company.
    Sert de repli si aucun snapshot n’est disponible.
    """
    names: List[str] = []
    seen = set()
    for bi in ProductBundleItem.objects.select_related("product__company").filter(bundle_id=bundle_id):
        comp = getattr(getattr(bi, "product", None), "company", None)
        if comp and comp.id not in seen:
            seen.add(comp.id)
            names.append(comp.name or f"Company {comp.id}")
    return names


def _bundle_producer_names(bundle_id: int, snapshot: Optional[dict]) -> List[str]:
    """
    Renvoie les noms de producteurs pour un bundle, en tentant d’abord le snapshot,
    puis en retombant sur la relation PBIs si besoin.
    """
    names = _bundle_producer_names_from_snapshot(snapshot or {})
    if names:
        return names
    return _bundle_producer_names_from_pbis(bundle_id)


# =====================================================================
# 1) Impact vs Revenue (period | category | product) — UNIFIÉ
# =====================================================================

class ImpactVsRevenueView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/impact-vs-revenue/?bucket=day|week|month&group_by=period|category|product&date_from&date_to
    GET /api/admin/analytics/cross/impact-vs-revenue/?...

    Réponses :
      - summary: {revenue, co2_kg, waste_kg}
      - series (group_by=period): [{period, date, revenue, orders, co2_kg, waste_kg, co2_per_eur}]
      - rows   (group_by=category|product) ; en admin, ajoute producer_names
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        bucket = (request.GET.get("bucket") or "week").lower()
        group_by = (request.GET.get("group_by") or "period").lower()
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)

        agg = orders.aggregate(
            revenue=Sum("total_price"),
            co2_kg=Sum("order_total_avoided_co2_kg"),
            waste_kg=Sum("order_total_avoided_waste_kg"),
        )
        summary = {
            "revenue": round(_safe_float(agg["revenue"]), 2),
            "co2_kg": round(_safe_float(agg["co2_kg"]), 2),
            "waste_kg": round(_safe_float(agg["waste_kg"]), 2),
        }

        if group_by == "period":
            roll = defaultdict(lambda: {"revenue": 0.0, "orders": 0, "co2_kg": 0.0, "waste_kg": 0.0})
            date_anchor = {}
            for o in orders.only("created_at", "total_price", "order_total_avoided_co2_kg", "order_total_avoided_waste_kg"):
                key = _bucket(o.created_at, bucket)
                r = roll[key]
                r["revenue"] += _safe_float(o.total_price)
                r["orders"] += 1
                r["co2_kg"] += _safe_float(o.order_total_avoided_co2_kg)
                r["waste_kg"] += _safe_float(o.order_total_avoided_waste_kg)
                if key not in date_anchor:
                    date_anchor[key] = _bucket_anchor_date(o.created_at, bucket)

            series = [
                {
                    "period": k,
                    "date": date_anchor.get(k),
                    "revenue": round(v["revenue"], 2),
                    "orders": v["orders"],
                    "co2_kg": round(v["co2_kg"], 2),
                    "waste_kg": round(v["waste_kg"], 2),
                    "co2_per_eur": round((v["co2_kg"] / v["revenue"]) if v["revenue"] else 0.0, 4),
                }
                for k, v in sorted(roll.items())
            ]
            return Response({"summary": summary, "series": series})

        items = _item_queryset_for_orders(orders).values(
            "bundle_id",
            "bundle_snapshot",
            "total_price",
            "order_item_total_avoided_co2_kg",
            "order_item_total_avoided_waste_kg",
        )

        if group_by == "category":
            roll: Dict[Any, Dict[str, Any]] = defaultdict(
                lambda: {"revenue": 0.0, "orders": 0, "co2_kg": 0.0, "waste_kg": 0.0, "producer_names": None}
            )
            for it in items:
                snap = it.get("bundle_snapshot") or {}
                names = _bundle_producer_names(it.get("bundle_id"), snap) if self.is_admin_scope else None
                for pid, title, cat_id, cat_name, pbq in _iter_snapshot_products(snap, it.get("bundle_id")):
                    key = (cat_id or "NA", cat_name or "Uncategorized")
                    r = roll[key]
                    r["revenue"] += _safe_float(it["total_price"])
                    r["co2_kg"] += _safe_float(it["order_item_total_avoided_co2_kg"])
                    r["waste_kg"] += _safe_float(it["order_item_total_avoided_waste_kg"])
                    r["orders"] += 1
                    if self.is_admin_scope and names and not r["producer_names"]:
                        r["producer_names"] = names
            rows = [
                {
                    "category_id": k[0],
                    "category_name": k[1],
                    "revenue": round(v["revenue"], 2),
                    "orders": v["orders"],
                    "co2_kg": round(v["co2_kg"], 2),
                    "waste_kg": round(v["waste_kg"], 2),
                    "co2_per_eur": round((v["co2_kg"] / v["revenue"]) if v["revenue"] else 0.0, 4),
                    **({"producer_names": v["producer_names"]} if self.is_admin_scope else {}),
                }
                for k, v in sorted(roll.items(), key=lambda kv: (-kv[1]["revenue"], -kv[1]["co2_kg"]))
            ]
            return Response({"summary": summary, "rows": rows})

        roll_p = defaultdict(
            lambda: {"title": None, "revenue": 0.0, "orders": 0, "co2_kg": 0.0, "waste_kg": 0.0, "producer_names": None}
        )
        for it in items:
            snap = it.get("bundle_snapshot") or {}
            names = _bundle_producer_names(it.get("bundle_id"), snap) if self.is_admin_scope else None
            for pid, title, cat_id, cat_name, pbq in _iter_snapshot_products(snap, it.get("bundle_id")):
                r = roll_p[pid]
                r["title"] = title
                r["revenue"] += _safe_float(it["total_price"])
                r["co2_kg"] += _safe_float(it["order_item_total_avoided_co2_kg"])
                r["waste_kg"] += _safe_float(it["order_item_total_avoided_waste_kg"])
                r["orders"] += 1
                if self.is_admin_scope and names and not r["producer_names"]:
                    r["producer_names"] = names
        rows = [
            {
                "product_id": pid,
                "title": v["title"],
                "revenue": round(v["revenue"], 2),
                "orders": v["orders"],
                "co2_kg": round(v["co2_kg"], 2),
                "waste_kg": round(v["waste_kg"], 2),
                "co2_per_eur": round((v["co2_kg"] / v["revenue"]) if v["revenue"] else 0.0, 4),
                **({"producer_names": v["producer_names"]} if self.is_admin_scope else {}),
            }
            for pid, v in sorted(roll_p.items(), key=lambda kv: (-kv[1]["revenue"], -kv[1]["co2_kg"]))
        ]
        return Response({"summary": summary, "rows": rows})


# =====================================================================
# 2) Discount vs Conversion (abandons -> achat) — UNIFIÉ
# =====================================================================

class DiscountVsConversionView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/discount-vs-conversion/?date_from&date_to
    GET /api/admin/analytics/cross/discount-vs-conversion/?...

    Sortie par bundle_id :
      {bundle_id, title, discount_pct, abandoned, purchased, conv_from_abandon, avg_aov, producer_names?}
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("bundle_id", "bundle_snapshot", "total_price")

        purchased_roll = defaultdict(lambda: {"total": 0, "rev": 0.0, "disc_sum": 0.0, "aov_sum": 0.0})
        bundle_ids = set()
        for it in items:
            snap = it.get("bundle_snapshot") or {}
            bundle = (snap.get("bundle") or {}) if isinstance(snap.get("bundle"), dict) else {}
            orig = bundle.get("original_price") or (snap.get("original_price"))
            disc = bundle.get("discounted_price") or (snap.get("discounted_price"))
            try:
                pct = (float(orig) - float(disc)) / float(orig) if orig else 0.0
            except Exception:
                pct = 0.0
            r = purchased_roll[it["bundle_id"]]
            r["total"] += 1
            r["rev"] += _safe_float(it["total_price"])
            r["disc_sum"] += pct
            r["aov_sum"] += _safe_float(it["total_price"])
            bundle_ids.add(it["bundle_id"])

        cart_qs = CartItem.objects.filter(is_active=True)
        if not self.is_admin_scope:
            co_ids = _company_ids(request.user)
            cart_qs = cart_qs.filter(bundle__items__product__company_id__in=co_ids) if co_ids else cart_qs.none()
        if date_from:
            cart_qs = cart_qs.filter(created_at__date__gte=date_from)
        if date_to:
            cart_qs = cart_qs.filter(created_at__date__lte=date_to)

        abandoned_roll = defaultdict(lambda: 0)
        title_cache = {}
        for c in cart_qs.select_related("bundle").values("bundle_id", "bundle__title"):
            abandoned_roll[c["bundle_id"]] += 1
            title_cache.setdefault(c["bundle_id"], c["bundle__title"])
            bundle_ids.add(c["bundle_id"])

        # cache des noms de producteurs (admin)
        names_cache = {}
        if self.is_admin_scope and bundle_ids:
            for bid in bundle_ids:
                snap = OrderItem.objects.filter(bundle_id=bid).order_by("-created_at").values_list("bundle_snapshot", flat=True).first()
                names_cache[bid] = _bundle_producer_names(bid, snap or {})

        out = []
        for bid, pr in purchased_roll.items():
            abandoned = abandoned_roll.get(bid, 0)
            purchased = pr["total"]
            conv = purchased / (abandoned + purchased) if (abandoned + purchased) else 0.0
            avg_disc = (pr["disc_sum"] / purchased) if purchased else 0.0
            avg_aov = (pr["aov_sum"] / purchased) if purchased else 0.0
            title = title_cache.get(bid)
            row = {
                "bundle_id": bid,
                "title": title,
                "discount_pct": round(avg_disc, 4),
                "abandoned": abandoned,
                "purchased": purchased,
                "conv_from_abandon": round(conv, 4),
                "avg_aov": round(avg_aov, 2),
            }
            if self.is_admin_scope:
                row["producer_names"] = names_cache.get(bid) or None
            out.append(row)

        out.sort(key=lambda r: (-r["conv_from_abandon"], -r["purchased"]))
        return Response({"rows": out})


# =====================================================================
# 3) Expiry vs Velocity (priorisation de déstockage) — UNIFIÉ
# =====================================================================

class ExpiryVsVelocityView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/expiry-vs-velocity/?horizon_days=30&date_from&date_to
    GET /api/admin/analytics/cross/expiry-vs-velocity/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)
        try:
            horizon_days = int(request.GET.get("horizon_days", 30))
        except Exception:
            horizon_days = 30

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("bundle_id", "bundle_snapshot", "quantity")

        weekly = defaultdict(lambda: 0.0)
        for it in items:
            snap = it.get("bundle_snapshot") or {}
            for pid, title, cat_id, cat_name, pbq in _iter_snapshot_products(snap, it.get("bundle_id")):
                weekly[pid] += float(it.get("quantity", 0) or 0) * float(pbq or 1) / 4.0

        out = []
        pbis = ProductBundleItem.objects.select_related("bundle", "product__company").filter(is_active=True, bundle__is_active=True)
        if not self.is_admin_scope:
            co_ids = _company_ids(request.user)
            pbis = pbis.filter(product__company_id__in=co_ids) if co_ids else pbis.none()

        today = timezone.now().date()
        for bi in pbis:
            pid = bi.product_id
            bbd = bi.best_before_date
            stock = int(getattr(bi.bundle, "stock", 0) or 0)
            w = weekly.get(pid, 0.0)
            days_stock = (stock / (w / 7.0)) if w > 0 else None
            days_to_expire = (bbd - today).days if bbd else None
            risk_level = None
            if days_to_expire is not None and days_stock is not None:
                if days_to_expire < days_stock:
                    risk_level = "HIGH"
                elif days_to_expire < days_stock * 1.5:
                    risk_level = "MEDIUM"
                else:
                    risk_level = "LOW"
            row = {
                "bundle_id": bi.bundle_id,
                "product_id": pid,
                "best_before_date": bbd.isoformat() if bbd else None,
                "stock": stock,
                "weekly_units": round(w, 2),
                "days_to_expire": days_to_expire,
                "days_of_stock": round(days_stock, 1) if days_stock else None,
                "risk_level": risk_level,
            }
            if self.is_admin_scope:
                comp = getattr(getattr(bi, "product", None), "company", None)
                row["producer_name"] = getattr(comp, "name", None)
            out.append(row)

        out.sort(key=lambda r: (r["risk_level"] != "HIGH", r["days_to_expire"] if r["days_to_expire"] is not None else 9999))
        return Response({"rows": out})


# =====================================================================
# 4) Paiements × AOV × Notes × Géo — UNIFIÉ
# =====================================================================

class PaymentsAovRatingsGeoView(AnalyticsScopeMixin, APIView):
    """
    /api/<scope>/analytics/cross/payments-aov-ratings-geo/
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        self.initialize_scope(request, **kwargs)

        date_from = request.GET.get("date_from")
        date_to = request.GET.get("date_to")

        qs = self.get_orders(request, date_from, date_to).select_related("payment_method").only(
            "id",
            "total_price",
            "status",
            "payment_method",
            "payment_method_snapshot",
            "customer_rating",
            "shipping_address_snapshot",
            "created_at",
        )

        # prefetch items for producer/company extraction
        prefetch_items = Prefetch(
            "items",
            queryset=OrderItem.objects.only("id", "bundle_id", "bundle_snapshot")
        )
        qs = qs.prefetch_related(prefetch_items)

        agg = defaultdict(lambda: {"orders": 0, "revenue": Decimal("0.0"), "success": 0, "label": "unknown"})
        order_rows = []

        def _pm_label(order):
            pm = getattr(order, "payment_method", None)
            if pm:
                return (
                    getattr(pm, "code", None)
                    or getattr(pm, "name", None)
                    or getattr(pm, "provider_name", None)
                    or str(pm)
                )
            snap = getattr(order, "payment_method_snapshot", None)
            if isinstance(snap, dict):
                t = (snap.get("type") or "unknown")
                p = (snap.get("provider") or snap.get("provider_name") or "unknown")
                return f"{t}:{p}"
            return "unknown"

        def _is_success(order):
            st = str(getattr(order, "status", "") or "").lower()
            if st in ("confirmed", "delivered"):
                return True
            payments_rel = getattr(order, "payments", None) or getattr(order, "payment_set", None)
            if payments_rel is not None:
                iterable = payments_rel.all() if hasattr(payments_rel, "all") else payments_rel
                for pay in iterable:
                    pst = str(getattr(pay, "status", "") or "").lower()
                    if pst in ("paid", "succeeded", "success", "captured", "completed"):
                        return True
            return False

        for o in qs:
            pm_key = _pm_label(o)
            price = Decimal(str(getattr(o, "total_price", 0) or 0))
            success = _is_success(o)

            rec = agg[pm_key]
            rec["orders"] += 1
            rec["revenue"] += price
            if success:
                rec["success"] += 1
            rec["label"] = pm_key

            # collect producer/company names from order items' snapshots
            pnames = set()
            cnames = set()
            items_rel = getattr(o, "items", None)
            iterable = items_rel.all() if hasattr(items_rel, "all") else (items_rel or [])
            for it in iterable:
                snap = getattr(it, "bundle_snapshot", None) or {}
                # producer display names (owners)
                for nm in (_bundle_producer_names(getattr(it, "bundle_id", None), snap) or []):
                    if nm:
                        pnames.add(nm)
                # company names
                for (_cid, nm) in (_extract_snapshot_companies(snap) or []):
                    if nm:
                        cnames.add(nm)

            order_rows.append({
                "order_id": o.id,
                "item_id": None,
                "payment_method": pm_key,
                "amount": float(price),
                "producer_names": sorted(pnames) if pnames else [],
                "company_names": sorted(cnames) if cnames else [],
            })

        rows = []
        for key, v in agg.items():
            n = v["orders"] or 1
            rows.append({
                "payment_method": v["label"],
                "aov": float(v["revenue"] / n),
                "success_rate": float(v["success"] / n),
            })

        rows.sort(key=lambda r: (-r["aov"], r["payment_method"]))
        return Response({"rows": rows, "order_rows": order_rows})
    

# =====================================================================
# 5) Performance géo : Revenus × Note × Impact — UNIFIÉ
# =====================================================================

class GeoRevenueRatingImpactView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/geo-revenue-rating-impact/?level=region|department&date_from&date_to
    GET /api/admin/analytics/cross/geo-revenue-rating-impact/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        level = (request.GET.get("level") or "region").lower()
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)

        roll = defaultdict(lambda: {"orders": 0, "revenue": 0.0, "rating_sum": 0.0, "rating_cnt": 0, "co2": 0.0, "waste": 0.0})
        for o in orders.only("total_price", "shipping_address_snapshot", "customer_rating", "order_total_avoided_co2_kg", "order_total_avoided_waste_kg"):
            addr = getattr(o, "shipping_address_snapshot", None) or {}
            key = addr.get(level) or addr.get("region") or addr.get("department") or "unknown"
            r = roll[key]
            r["orders"] += 1
            r["revenue"] += _safe_float(o.total_price)
            r["co2"] += _safe_float(o.order_total_avoided_co2_kg)
            r["waste"] += _safe_float(o.order_total_avoided_waste_kg)
            if o.customer_rating is not None:
                r["rating_sum"] += float(o.customer_rating)
                r["rating_cnt"] += 1

        rows = []
        for k, v in roll.items():
            avg_rating = (v["rating_sum"] / v["rating_cnt"]) if v["rating_cnt"] else None
            rows.append({
                "geo": k,
                "orders": v["orders"],
                "revenue": round(v["revenue"], 2),
                "avg_rating": round(avg_rating, 2) if avg_rating is not None else None,
                "total_waste_kg": round(v["waste"], 2),
                "total_co2_kg": round(v["co2"], 2),
                "co2_per_eur": round((v["co2"] / v["revenue"]) if v["revenue"] else 0.0, 4),
            })
        rows.sort(key=lambda r: (-r["revenue"], - (r["avg_rating"] or 0)))
        return Response({"rows": rows})


# =====================================================================
# 6) Catégorie × Économie (€) × Impact — UNIFIÉ
# =====================================================================

class CategorySavingsImpactView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/category-savings-impact/?date_from&date_to
    GET /api/admin/analytics/cross/category-savings-impact/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("bundle_id", "bundle_snapshot", "total_price")

        roll = defaultdict(lambda: {"revenue": 0.0, "savings": 0.0, "orders": 0})
        for it in items:
            snap = it.get("bundle_snapshot") or {}
            bundle = (snap.get("bundle") or {}) if isinstance(snap.get("bundle"), dict) else {}
            orig = bundle.get("original_price") or (snap.get("original_price"))
            disc = bundle.get("discounted_price") or (snap.get("discounted_price"))
            savings = 0.0
            try:
                savings = (float(orig) - float(disc)) if (orig and disc) else 0.0
            except Exception:
                savings = 0.0
            for pid, title, cat_id, cat_name, pbq in _iter_snapshot_products(snap, it.get("bundle_id")):
                key = (cat_id or "NA", cat_name or "Uncategorized")
                r = roll[key]
                r["revenue"] += _safe_float(it["total_price"])
                r["orders"] += 1
                r["savings"] += savings

        rows = []
        for k, v in roll.items():
            rows.append({
                "category_id": k[0],
                "category_name": k[1],
                "revenue": round(v["revenue"], 2),
                "savings_eur": round(v["savings"], 2),
                "savings_rate": round((v["savings"] / v["revenue"]) if v["revenue"] else 0.0, 4),
                # Les impacts par ligne ne sont pas disponibles dans ce calcul agrégé.
                "co2_kg": 0.0,
                "waste_kg": 0.0,
                "co2_per_€": 0.0,
            })
        rows.sort(key=lambda r: (-r["savings_eur"], -r["revenue"]))
        return Response({"rows": rows})


# =====================================================================
# 7) Certifications × Ventes × Note — UNIFIÉ
# =====================================================================

class CertificationsPerformanceView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/certifications-performance/?date_from&date_to
    GET /api/admin/analytics/cross/certifications-performance/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("bundle_id", "bundle_snapshot", "total_price", "customer_rating")

        roll = defaultdict(lambda: {"revenue": 0.0, "orders": 0, "rating_sum": 0.0, "rating_cnt": 0, "producer_names": None})
        for it in items:
            snap = it.get("bundle_snapshot") or {}
            names = _bundle_producer_names(it.get("bundle_id"), snap) if self.is_admin_scope else None
            for p in (snap.get("products") or []):
                certs = p.get("certifications") or []
                for code in certs:
                    r = roll[code]
                    r["revenue"] += _safe_float(it["total_price"])
                    r["orders"] += 1
                    rat = it.get("customer_rating")
                    if rat is not None:
                        r["rating_sum"] += float(rat)
                        r["rating_cnt"] += 1
                    if self.is_admin_scope and names and not r["producer_names"]:
                        r["producer_names"] = names

        rows = []
        total_rev = sum(v["revenue"] for v in roll.values())
        for code, v in roll.items():
            avg_rating = v["rating_sum"] / v["rating_cnt"] if v["rating_cnt"] else None
            rows.append({
                "cert_code": code,
                "revenue": round(v["revenue"], 2),
                "orders": v["orders"],
                "avg_rating": round(avg_rating, 2) if avg_rating is not None else None,
                "share_of_revenue": round((v["revenue"] / total_rev) if total_rev else 0.0, 4),
                **({"producer_names": v["producer_names"]} if self.is_admin_scope else {}),
            })
        rows.sort(key=lambda r: (-r["revenue"], -r["orders"]))
        return Response({"rows": rows})


# =====================================================================
# 8) ÉcoScore : ventes / notes / impact — UNIFIÉ
# =====================================================================

class EcoScorePerformanceView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/ecoscore-performance/?date_from&date_to
    GET /api/admin/analytics/cross/ecoscore-performance/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("bundle_id", "bundle_snapshot", "total_price", "customer_rating")

        roll = defaultdict(lambda: {"revenue": 0.0, "orders": 0, "rating_sum": 0.0, "rating_cnt": 0, "producer_names": None})
        for it in items:
            snap = it.get("bundle_snapshot") or {}
            names = _bundle_producer_names(it.get("bundle_id"), snap) if self.is_admin_scope else None
            for p in (snap.get("products") or []):
                ecoscore = p.get("eco_score") or p.get("ecoscore") or p.get("ecoScore") or "NA"
                r = roll[ecoscore]
                r["revenue"] += _safe_float(it["total_price"])
                r["orders"] += 1
                rat = it.get("customer_rating")
                if rat is not None:
                    r["rating_sum"] += float(rat)
                    r["rating_cnt"] += 1
                if self.is_admin_scope and names and not r["producer_names"]:
                    r["producer_names"] = names

        rows = []
        for es, v in roll.items():
            avg_rating = v["rating_sum"] / v["rating_cnt"] if v["rating_cnt"] else None
            rows.append({
                "ecoscore": es,
                "revenue": round(v["revenue"], 2),
                "orders": v["orders"],
                "avg_rating": round(avg_rating, 2) if avg_rating is not None else None,
                **({"producer_names": v["producer_names"]} if self.is_admin_scope else {}),
            })
        rows.sort(key=lambda r: (-r["revenue"], -r["orders"]))
        return Response({"rows": rows})


# =====================================================================
# 9) Favoris → Achat (time-to-buy & conversion) — UNIFIÉ
# =====================================================================

class FavoritesToPurchaseView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/favorites-to-purchase/?date_from&date_to
    GET /api/admin/analytics/cross/favorites-to-purchase/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        favs = Favorite.objects.all()
        if not self.is_admin_scope:
            co_ids = _company_ids(request.user)
            favs = favs.filter(bundle__items__product__company_id__in=co_ids).distinct() if co_ids else favs.none()
        if date_from:
            favs = favs.filter(created_at__date__gte=date_from)
        if date_to:
            favs = favs.filter(created_at__date__lte=date_to)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("order_id", "bundle_id", "bundle_snapshot", "created_at")

        fav_by_bundle = defaultdict(list)
        for f in favs.values("bundle_id", "created_at"):
            fav_by_bundle[f["bundle_id"]].append(f["created_at"])

        purch_by_bundle = defaultdict(list)
        for it in items:
            purch_by_bundle[it["bundle_id"]].append(it["created_at"])

        bundle_ids = set(list(fav_by_bundle.keys()) + list(purch_by_bundle.keys()))
        names_cache = {}
        if self.is_admin_scope and bundle_ids:
            for bid in bundle_ids:
                snap = OrderItem.objects.filter(bundle_id=bid).order_by("-created_at").values_list("bundle_snapshot", flat=True).first()
                names_cache[bid] = _bundle_producer_names(bid, snap or {})

        rows = []
        for bid, fav_times in fav_by_bundle.items():
            purchases = purch_by_bundle.get(bid, [])
            conv = (len(purchases) / (len(fav_times) + len(purchases))) if (len(fav_times) + len(purchases)) else 0.0
            tts = []
            for ft in fav_times:
                later = [pt for pt in purchases if pt and ft and pt >= ft]
                if later:
                    delta = (later[0].date() - ft.date()).days
                    tts.append(delta)
            med_days = int(median(tts)) if tts else None
            row = {
                "bundle_id": bid,
                "favorites": len(fav_times),
                "purchases": len(purchases),
                "conversion_rate": round(conv, 4),
                "median_days_to_buy": med_days,
            }
            if self.is_admin_scope:
                row["producer_names"] = names_cache.get(bid) or None
            rows.append(row)

        rows.sort(key=lambda r: (-r["conversion_rate"], -r["purchases"]))
        return Response({"rows": rows})


# =====================================================================
# 10) Cohortes (premier achat) × Impact cumulé — UNIFIÉ
# =====================================================================

class CohortsImpactView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/cohorts-impact/?date_from&date_to
    GET /api/admin/analytics/cross/cohorts-impact/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)

        first_by_user = {}
        for o in orders.only("user_id", "created_at"):
            uid = o.user_id
            d = o.created_at.date()
            if uid not in first_by_user or d < first_by_user[uid]:
                first_by_user[uid] = d

        cohorts = defaultdict(lambda: {"users": set(), "d30_rev": 0.0, "d60_rev": 0.0, "d90_rev": 0.0,
                                       "d30_co2": 0.0, "d60_co2": 0.0, "d90_co2": 0.0})
        for o in orders.only("user_id", "created_at", "total_price", "order_total_avoided_co2_kg"):
            uid = o.user_id
            if uid not in first_by_user:
                continue
            cohort_month = f"{first_by_user[uid].year:04d}-{first_by_user[uid].month:02d}"
            delta_days = (o.created_at.date() - first_by_user[uid]).days
            c = cohorts[cohort_month]
            c["users"].add(uid)
            if delta_days <= 30:
                c["d30_rev"] += _safe_float(o.total_price); c["d30_co2"] += _safe_float(o.order_total_avoided_co2_kg)
            if delta_days <= 60:
                c["d60_rev"] += _safe_float(o.total_price); c["d60_co2"] += _safe_float(o.order_total_avoided_co2_kg)
            if delta_days <= 90:
                c["d90_rev"] += _safe_float(o.total_price); c["d90_co2"] += _safe_float(o.order_total_avoided_co2_kg)

        rows = []
        for m, v in sorted(cohorts.items()):
            rows.append({
                "cohort_month": m,
                "retained_users": len(v["users"]),
                "revenue_d30": round(v["d30_rev"], 2),
                "revenue_d60": round(v["d60_rev"], 2),
                "revenue_d90": round(v["d90_rev"], 2),
                "co2_d30": round(v["d30_co2"], 2),
                "co2_d60": round(v["d60_co2"], 2),
                "co2_d90": round(v["d90_co2"], 2),
            })
        return Response({"rows": rows})


# =====================================================================
# 11) RFM × Note — UNIFIÉ
# =====================================================================

class RfmRatingsView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/rfm-ratings/?date_from&date_to
    GET /api/admin/analytics/cross/rfm-ratings/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        by_user = defaultdict(lambda: {"freq": 0, "monetary": 0.0, "last": None, "ratings": []})
        for o in orders.only("user_id", "created_at", "total_price", "customer_rating"):
            u = by_user[o.user_id]
            u["freq"] += 1
            u["monetary"] += _safe_float(o.total_price)
            u["last"] = max(u["last"], o.created_at) if u["last"] else o.created_at
            if o.customer_rating is not None:
                u["ratings"].append(float(o.customer_rating))

        now = timezone.now()
        segments = defaultdict(lambda: {"users": 0, "revenue": 0.0, "avg_rating": 0.0, "rating_cnt": 0, "aov": 0.0})
        for uid, u in by_user.items():
            recency_days = (now - u["last"]).days if u["last"] else 999
            if u["freq"] >= 5 and u["monetary"] >= 200:
                seg = "Champions"
            elif u["freq"] >= 3 and u["monetary"] >= 100:
                seg = "Loyal"
            elif recency_days <= 30:
                seg = "Recent"
            else:
                seg = "AtRisk"
            segrow = segments[seg]
            segrow["users"] += 1
            segrow["revenue"] += u["monetary"]
            if u["ratings"]:
                segrow["avg_rating"] += sum(u["ratings"])
                segrow["rating_cnt"] += len(u["ratings"])

        rows = []
        for seg, v in segments.items():
            avg_rating = (v["avg_rating"] / v["rating_cnt"]) if v["rating_cnt"] else None
            rows.append({
                "segment": seg,
                "users": v["users"],
                "revenue": round(v["revenue"], 2),
                "avg_rating": round(avg_rating, 2) if avg_rating is not None else None,
                "aov": round((v["revenue"] / v["users"]) if v["users"] else 0.0, 2),
            })
        rows.sort(key=lambda r: (-r["revenue"], -r["users"]))
        return Response({"rows": rows})


# =====================================================================
# 12) Part du producteur dans les commandes — UNIFIÉ
# =====================================================================

class ProducerShareInOrdersView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/producer-share-in-orders/?date_from&date_to
    GET /api/admin/analytics/cross/producer-share-in-orders/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)

        rows = []
        for o in orders.only("id", "total_price"):
            total = _safe_float(o.total_price)
            by_producer = defaultdict(lambda: 0.0)
            names = {}
            for it in o.items.all():
                snap = getattr(it, "bundle_snapshot", None) or {}
                price = _safe_float(it.total_price)
                prods = snap.get("products") or []
                if not prods:
                    continue
                share = price / len(prods)
                for p in prods:
                    cid = p.get("company_id")
                    cname = p.get("company_name") or (f"Company {cid}" if cid else None)
                    if cid:
                        by_producer[cid] += share
                        if cid not in names and cname:
                            names[cid] = cname

            my_rev = None
            other_producers = []
            if self.is_admin_scope:
                other_producers = [{"company_id": cid, "company_name": names.get(cid), "revenue": round(val, 2)} for cid, val in by_producer.items()]
            else:
                for cid, val in by_producer.items():
                    if cid in _company_ids(request.user):
                        my_rev = (my_rev or 0.0) + val
                    else:
                        other_producers.append({"company_id": cid, "revenue": round(val, 2)})
            share_pct = (my_rev / total) if (my_rev is not None and total) else None
            rows.append({
                "order_id": o.id,
                "total_price": round(total, 2),
                "my_revenue": round(my_rev, 2) if my_rev is not None else None,
                "share_pct": round(share_pct, 4) if share_pct is not None else None,
                "other_producers": other_producers if other_producers else None,
            })
        return Response({"rows": rows})


# =====================================================================
# 13) Remise × Note — UNIFIÉ
# =====================================================================

class DiscountVsRatingView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/discount-vs-rating/?date_from&date_to
    GET /api/admin/analytics/cross/discount-vs-rating/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("bundle_id", "bundle_snapshot", "customer_rating", "total_price")

        def bucket_disc(pct: float) -> str:
            if pct is None:
                return "NA"
            b = int((pct * 100) // 10) * 10
            hi = b + 10
            return f"{b:02d}-{hi:02d}"

        roll = defaultdict(lambda: {"orders": 0, "discount_sum": 0.0, "rating_sum": 0.0, "rating_cnt": 0})
        for it in items:
            snap = it.get("bundle_snapshot") or {}
            bundle = (snap.get("bundle") or {}) if isinstance(snap.get("bundle"), dict) else {}
            orig = bundle.get("original_price") or (snap.get("original_price"))
            disc = bundle.get("discounted_price") or (snap.get("discounted_price"))
            try:
                pct = (float(orig) - float(disc)) / float(orig) if orig else None
            except Exception:
                pct = None
            bkey = bucket_disc(pct)
            r = roll[bkey]
            r["orders"] += 1
            if pct is not None:
                r["discount_sum"] += pct
            rat = it.get("customer_rating")
            if rat is not None:
                r["rating_sum"] += float(rat)
                r["rating_cnt"] += 1

        rows = []
        for b, v in sorted(roll.items()):
            rows.append({
                "bucket": b,
                "orders": v["orders"],
                "avg_discount": round((v["discount_sum"] / v["orders"]) if v["orders"] else 0.0, 4),
                "avg_rating": round((v["rating_sum"] / v["rating_cnt"]) if v["rating_cnt"] else 0.0, 2),
            })
        return Response({"rows": rows})


# =====================================================================
# 14) Efficience d’inventaire : Stock × Ventes × Impact — UNIFIÉ
# =====================================================================

class InventoryEfficiencyView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/cross/inventory-efficiency/?date_from&date_to
    GET /api/admin/analytics/cross/inventory-efficiency/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)

        orders = _admin_scope_orders(date_from, date_to) if self.is_admin_scope else _producer_scope_orders(request.user, date_from, date_to)
        items = _item_queryset_for_orders(orders).values("bundle_id", "bundle_snapshot", "quantity")

        weekly_by_bundle = defaultdict(float)
        for it in items:
            qty = float(it.get("quantity", 0) or 0)
            weekly_by_bundle[it["bundle_id"]] += qty / 4.0

        impact_by_bundle = defaultdict(float)
        for it in _item_queryset_for_orders(orders).values("bundle_id", "order_item_total_avoided_co2_kg"):
            impact_by_bundle[it["bundle_id"]] += _safe_float(it["order_item_total_avoided_co2_kg"])

        bundles = ProductBundle.objects.filter(is_active=True)
        if not self.is_admin_scope:
            co_ids = _company_ids(request.user)
            bundles = bundles.filter(items__product__company_id__in=co_ids).distinct() if co_ids else bundles.none()

        names_cache = {}
        if self.is_admin_scope:
            # pré-remplissage via dernier snapshot disponible, sinon via PBIs
            for b in bundles:
                snap = OrderItem.objects.filter(bundle_id=b.id).order_by("-created_at").values_list("bundle_snapshot", flat=True).first()
                names_cache[b.id] = _bundle_producer_names(b.id, snap or {})

        rows = []
        for b in bundles.only("id", "title", "stock"):
            w = weekly_by_bundle.get(b.id, 0.0)
            stock = int(getattr(b, "stock", 0) or 0)
            days_stock = (stock / (w / 7.0)) if w > 0 else None
            imp = impact_by_bundle.get(b.id, 0.0)
            row = {
                "bundle_id": b.id,
                "title": b.title,
                "stock": stock,
                "weekly_units": round(w, 2),
                "days_of_stock": round(days_stock, 1) if days_stock else None,
                "impact_per_stock_unit": round((imp / stock), 4) if stock else None,
            }
            if self.is_admin_scope:
                row["producer_names"] = names_cache.get(b.id) or None
            rows.append(row)

        rows.sort(key=lambda r: (-(r["impact_per_stock_unit"] or 0), -(r["weekly_units"] or 0)))
        return Response({"rows": rows})
