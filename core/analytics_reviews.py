# analytics_reviews.py
from __future__ import annotations
from collections import Counter, defaultdict
import re
from typing import Any, Dict, List, Tuple

from django.db.models import Avg, Count, F, Sum
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Order, OrderItem, ProductBundleItem
from .analytics_endpoints import (
    _company_ids, _date_range, _pagination, _sort_params, VALID_STATUSES, _bucket
)
from .analytics_scope import AnalyticsScopeMixin

# ---------- helpers ----------

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

# ---------- 1) Evaluations Deep (unificado) ----------

class EvaluationsDeepView(AnalyticsScopeMixin, APIView):
    """
    GET /api/producer/analytics/evaluations/deep/?date_from&date_to&limit&offset&sort_by&sort_dir&kind=item|order|all
    GET /api/admin/analytics/evaluations/deep/?...
    """
    permission_classes = [IsAuthenticated]

    def get(self, request, **kwargs):
        self.initialize_scope(request, **kwargs)
        date_from, date_to = _date_range(request)
        limit, offset = _pagination(request, default_limit=50)
        kind = request.GET.get("kind", "all").lower()

        if self.is_admin_scope:
            items_qs = OrderItem.objects.filter(order__status__in=VALID_STATUSES, customer_rating__isnull=False)
            orders_qs = Order.objects.filter(status__in=VALID_STATUSES, customer_rating__isnull=False)
        else:
            items_qs = _orderitems_for_producer(request.user, date_from, date_to).filter(customer_rating__isnull=False)
            orders_qs = (
                Order.objects.filter(
                    status__in=VALID_STATUSES,
                    items__bundle__items__product__company_id__in=_company_ids(request.user),
                    customer_rating__isnull=False,
                ).distinct()
            )
        if date_from:
            items_qs = items_qs.filter(order__created_at__date__gte=date_from)
            orders_qs = orders_qs.filter(created_at__date__gte=date_from)
        if date_to:
            items_qs = items_qs.filter(order__created_at__date__lte=date_to)
            orders_qs = orders_qs.filter(created_at__date__lte=date_to)

        summary = {
            "avg_item_rating": round(float(items_qs.aggregate(x=Avg("customer_rating"))["x"] or 0), 2),
            "item_ratings_count": items_qs.count(),
            "avg_order_rating": round(float(orders_qs.aggregate(x=Avg("customer_rating"))["x"] or 0), 2),
            "order_ratings_count": orders_qs.count(),
            "distribution_items": {str(k["customer_rating"]): k["n"] for k in items_qs.values("customer_rating").annotate(n=Count("id")).order_by()},
            "distribution_orders": {str(k["customer_rating"]): k["n"] for k in orders_qs.values("customer_rating").annotate(n=Count("id")).order_by()},
        }

        rows: List[Dict[str, Any]] = []
        if kind in ("item", "all"):
            sort_by, sort_dir = _sort_params(request, default_sort_by="rated_at",
                                             allowed=["rated_at", "rating", "created_at", "line_total"])
            order_by_expr = ("-" if sort_dir == "desc" else "") + ("customer_rating" if sort_by == "rating" else sort_by)
            for oi in items_qs.select_related("order", "bundle").order_by(order_by_expr)[offset: offset + limit]:
                base = {
                    "type": "item",
                    "order_id": oi.order_id,
                    "item_id": oi.id,
                    "bundle_id": oi.bundle_id,
                    "bundle_title": (getattr(oi, "bundle_snapshot", {}) or {}).get("title"),
                    "rating": oi.customer_rating,
                    "note": oi.customer_note or "",
                    "rated_at": oi.rated_at.isoformat() if oi.rated_at else None,
                    "created_at": oi.order.created_at.isoformat() if oi.order and oi.order.created_at else None,
                    "quantity": int(oi.quantity or 0),
                    "line_total": float(oi.total_price or 0),
                }
                if self.is_admin_scope:
                    base["producer_names"] = [name for (_cid, name) in _extract_snapshot_companies(oi)] or None
                rows.append(base)

        if kind in ("order", "all"):
            sort_by, sort_dir = _sort_params(request, default_sort_by="rated_at",
                                             allowed=["rated_at", "rating", "created_at", "total_price"])
            order_by_expr = ("-" if sort_dir == "desc" else "") + ("customer_rating" if sort_by == "rating" else sort_by)
            for o in orders_qs.order_by(order_by_expr)[offset: offset + limit]:
                base = {
                    "type": "order",
                    "order_id": o.id,
                    "order_code": o.order_code,
                    "rating": o.customer_rating,
                    "note": o.customer_note or "",
                    "rated_at": o.rated_at.isoformat() if o.rated_at else None,
                    "created_at": o.created_at.isoformat() if o.created_at else None,
                    "total_price": float(o.total_price or 0),
                }
                if self.is_admin_scope:
                    it = o.items.order_by("-rated_at", "-created_at").first()
                    base["producer_names"] = [name for (_cid, name) in _extract_snapshot_companies(it)] if it else None
                rows.append(base)

        return Response({"summary": summary, "rows": rows, "meta": {"limit": limit, "offset": offset}})

# ---------- 2) Reviews Keywords (unificado) ----------

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
