from datetime import datetime, timedelta, date
from collections import defaultdict
import json
import os
import re
import logging
import requests

from django.conf import settings
from django.db.models import Exists, OuterRef, Sum, Count, Min, Max
from django.utils import timezone
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Company, ProductBundleItem, OrderItem

logger = logging.getLogger(__name__)
VALID_STATUSES = ("confirmed", "delivered")


def _monday(d: datetime) -> date:
    d = d.date() if isinstance(d, datetime) else d
    return d - timedelta(days=d.weekday())


def _company_ids(user):
    return list(Company.objects.filter(owner=user, is_active=True).values_list("id", flat=True))


def _orderitems_for_producer(user):
    company_ids = _company_ids(user)
    exists_company_item = ProductBundleItem.objects.filter(
        bundle=OuterRef("bundle"),
        product__company_id__in=company_ids,
    )
    return (
        OrderItem.objects.annotate(has_company=Exists(exists_company_item))
        .filter(has_company=True, order__status__in=VALID_STATUSES)
        .select_related("order")
    )


def _timeseries_by_product(user):
    ts = {}
    name_map = {}
    base = _orderitems_for_producer(user)
    for oi in base.only("id", "quantity", "bundle_snapshot", "order__created_at"):
        snap = oi.bundle_snapshot or {}
        products = snap.get("products", [])
        if not products:
            continue
        week = _monday(oi.order.created_at)
        for p in products:
            pid = p.get("product_id")
            if pid is None:
                continue
            title = p.get("product_title") or f"Produit {pid}"
            per_bundle_qty = int(p.get("per_bundle_quantity", 1))
            units = per_bundle_qty * int(oi.quantity)
            name_map[pid] = title
            ts.setdefault(pid, defaultdict(int))
            ts[pid][week] += units
    return ts, name_map


def _moving_avg(values, window=4):
    if not values:
        return 0.0
    vals = list(values)[-window:]
    return sum(vals) / max(1, len(vals))


def _forecast_next_week(series_map):
    if not series_map:
        return 0, 0, 0
    weeks = sorted(series_map.keys())
    vals = [series_map[w] for w in weeks]
    baseline = _moving_avg(vals, window=4)
    if len(vals) < 2:
        return (vals[-1] if vals else 0), baseline, 0
    deltas = [vals[i] - vals[i - 1] for i in range(1, len(vals))]
    avg_delta = sum(deltas[-4:]) / max(1, len(deltas[-4:]))
    forecast = max(0, vals[-1] + avg_delta)
    return forecast, baseline, vals[-1]


def _status_label(pct_diff):
    if pct_diff >= 0.20:
        return "Demande élevée prévue"
    if pct_diff <= -0.10:
        return "Baisse probable"
    return "Demande stable"


def _seasonal_forecasts(user, top_k=3):
    ts, name_map = _timeseries_by_product(user)
    out = []
    for pid, series in ts.items():
        fcast, base, last = _forecast_next_week(series)
        pct = 0.0 if base == 0 else (fcast - base) / base
        out.append({
            "product_id": pid,
            "label": name_map.get(pid, f"Produit {pid}"),
            "forecasted_units": int(round(fcast)),
            "baseline_units": int(round(base)),
            "last_week_units": int(round(last)),
            "forecast_delta_pct": round(pct * 100, 2),
            "status": _status_label(pct),
        })
    out.sort(key=lambda x: (x["forecasted_units"] - x["baseline_units"]), reverse=True)
    return out[:top_k], out


def _soon_expiring_alert(user, days=5):
    today = timezone.localdate()
    limit = today + timedelta(days=days)
    qs = (
        ProductBundleItem.objects
        .filter(
            product__company__owner=user,
            best_before_date__isnull=False,
            best_before_date__lte=limit,
            bundle__stock__gt=0,
            is_active=True,
        )
        .select_related("product", "bundle")
        .order_by("best_before_date")
    )
    first = qs.first()
    if not first:
        return None
    delta = (first.best_before_date - today).days
    title = first.product.title
    return {
        "type": "alert",
        "message": f"Vos {title} arrivent à expiration dans {max(delta, 0)} jours"
    }


def _low_stock_alerts(user, low_threshold=5):
    qs = (
        ProductBundleItem.objects
        .filter(product__company__owner=user, is_active=True)
        .select_related("product", "bundle")
    )
    alerts = []
    for item in qs:
        stock = getattr(item.bundle, "stock", 0)
        title = item.product.title
        if stock <= 0:
            alerts.append({"type": "alert", "level": "red", "message": f"{title} est en rupture de stock"})
        elif stock <= low_threshold:
            alerts.append({"type": "alert", "level": "yellow", "message": f"Stock faible pour {title} ({stock} restants)"})
    return alerts


def _stock_suggestion(top_forecasts):
    if not top_forecasts:
        return None
    top = top_forecasts[0]
    pct = max(0, top["forecast_delta_pct"])
    recommended = int(round(min(30, pct)))
    if recommended <= 0:
        return None
    return {
        "type": "suggestion",
        "message": f"Augmentez votre stock de {top['label']} de {recommended}% pour la semaine prochaine"
    }


def _customer_clusters(user):
    base = _orderitems_for_producer(user)
    agg = (
        base.values("order__user_id", "order__user__first_name", "order__user__last_name", "order__user__email")
        .annotate(
            orders=Count("order_id", distinct=True),
            first_order=Min("order__created_at"),
            last_order=Max("order__created_at"),
            amount=Sum("total_price"),
        )
    )
    now = timezone.now()
    loyal, new, occasional = [], [], []
    for r in agg:
        uid = r["order__user_id"]
        first_dt = r["first_order"] or now
        last_dt = r["last_order"] or now
        orders = int(r["orders"] or 0)
        days_since_last = (now - last_dt).days
        days_since_first = (now - first_dt).days
        name = f"{(r.get('order__user__first_name') or '').strip()} {(r.get('order__user__last_name') or '').strip()}".strip()
        email = (r.get("order__user__email") or "").strip()
        customer = {"id": uid, "name": name or email.split("@")[0], "email": email}
        if orders == 1 and days_since_first <= 30:
            new.append(customer)
        elif (orders >= 3 and (now - first_dt).days <= 90) or days_since_last <= 30:
            loyal.append(customer)
        else:
            occasional.append(customer)
    return {
        "loyal": {"count": len(loyal), "customers": loyal},
        "new": {"count": len(new), "customers": new},
        "occasional": {"count": len(occasional), "customers": occasional},
    }


class _LLMDebug:
    last_error = None
    last_status = None
    last_body = None


def _messages_for_ai(context):
    system = (
        "You are an assistant that generates short French retail recommendations for a local food producer. "
        "Only return a JSON array of objects with fields: type (suggestion|alert) and message (French). "
        "Max 3 items. No prose around the JSON."
    )
    user_msg = {
        "role": "user",
        "content": (
            "Context:\n"
            + json.dumps(context, ensure_ascii=False)
            + "\nRules:\n"
            "- If growth > 15% for any product, suggest increasing stock with a concrete %.\n"
            "- If negative trend or low demand, suggest a promo or bundle.\n"
            "- Keep messages concise and in French.\n"
            "- Output only JSON."
        )
    }
    return [{"role": "system", "content": system}, user_msg]


def _extract_json(text):
    if not text:
        return None
    m = re.search(r"\[.*\]", text, flags=re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _parse_ai_json(text):
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            if "recommendations" in parsed and isinstance(parsed["recommendations"], list):
                parsed = parsed["recommendations"]
            else:
                parsed = parsed.get("items", [])
    except Exception:
        parsed = _extract_json(text)
    if not isinstance(parsed, list):
        return None
    cleaned = []
    for r in parsed[:3]:
        t = str(r.get("type", "")).strip().lower()
        msg = str(r.get("message", "")).strip()
        if t in {"suggestion", "alert"} and msg:
            cleaned.append({"type": t, "message": msg})
    return cleaned[:3]


def _call_chat_completions(api_base, api_key, model, messages, timeout=15):
    url = f"{api_base.rstrip('/')}/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": messages, "temperature": 0.2, "response_format": {"type": "json_object"}}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
        _LLMDebug.last_status = r.status_code
        _LLMDebug.last_body = r.text[:2000]
        r.raise_for_status()
        data = r.json()
        text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = _parse_ai_json(text)
        return parsed or []
    except Exception as e:
        _LLMDebug.last_error = str(e)
        return []


def _join_output_text_from_responses(data):
    text = data.get("output_text")
    if text:
        if isinstance(text, list):
            return "".join(text)
        return text
    pieces = []
    for msg in data.get("output", []):
        for part in msg.get("content", []):
            if part.get("type") in ("output_text", "text"):
                t = part.get("text")
                if isinstance(t, dict):
                    continue
                if t:
                    pieces.append(t)
    return "".join(pieces)


def _call_responses_api(api_base, api_key, model, messages, timeout=15):
    url = f"{api_base.rstrip('/')}/v1/responses"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {"model": model, "input": messages, "temperature": 0.2, "text": {"format": {"type": "json_object"}}}
    try:
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
        _LLMDebug.last_status = r.status_code
        _LLMDebug.last_body = r.text[:2000]
        r.raise_for_status()
        data = r.json()
        text = _join_output_text_from_responses(data)
        parsed = _parse_ai_json(text)
        return parsed or []
    except Exception as e:
        _LLMDebug.last_error = str(e)
        return []


def _ai_recommendations(context):
    key = (getattr(settings, "OPEIA_API_KEY", None) or os.getenv("OPEIA_API_KEY") or "").strip(" '\"")
    base = (getattr(settings, "OPEIA_API_BASE", None) or os.getenv("OPEIA_API_BASE") or "https://api.openai.com").strip()
    model = (getattr(settings, "OPEIA_MODEL", None) or os.getenv("OPEIA_MODEL") or "gpt-4o-mini").strip()
    if not key:
        _LLMDebug.last_error = "Missing API key"
        return []
    messages = _messages_for_ai(context)
    out = _call_chat_completions(base, key, model, messages)
    if out:
        return out
    return _call_responses_api(base, key, model, messages)


class ProducerAnalyticsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        top3, all_preds = _seasonal_forecasts(request.user)
        suggestion = _stock_suggestion(top3)
        alert = _soon_expiring_alert(request.user, days=5)
        stock_alerts = _low_stock_alerts(request.user, low_threshold=5)
        clusters = _customer_clusters(request.user)

        recommendations = []
        if suggestion:
            recommendations.append(suggestion)
        if alert:
            recommendations.append(alert)
        recommendations.extend(stock_alerts)
        recommendations.extend(_ai_recommendations({"forecasts": all_preds, "clusters": {
            "loyal": {"count": clusters["loyal"]["count"]},
            "new": {"count": clusters["new"]["count"]},
            "occasional": {"count": clusters["occasional"]["count"]},
        }}))

        return Response({"seasonal_forecasts": top3, "recommendations": recommendations, "clusters": clusters})


class ProducerAIPreviewView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        _, all_preds = _seasonal_forecasts(request.user)
        clusters = _customer_clusters(request.user)
        ai_context = {
            "forecasts": all_preds,
            "clusters": {
                "loyal": {"count": clusters["loyal"]["count"]},
                "new": {"count": clusters["new"]["count"]},
                "occasional": {"count": clusters["occasional"]["count"]},
            },
        }
        ai_output = _ai_recommendations(ai_context)
        debug = bool(int(request.GET.get("debug", "0")))
        payload = {"context_sent": ai_context, "ai_output": ai_output}
        if debug:
            payload["ai_debug"] = {
                "last_status": _LLMDebug.last_status,
                "last_error": _LLMDebug.last_error,
                "last_body": _LLMDebug.last_body,
            }
        return Response(payload)
