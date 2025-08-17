# analytics_scope.py
from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from django.utils import timezone
from rest_framework.permissions import IsAuthenticated
from .models import Company

# ============================================================
# Constantes & utilitaires communs (autonomes) — PAS d'import vers analytics_endpoints
# ============================================================

# FR: Statuts “valides” pour considérer une commande dans l’analytics.
VALID_STATUSES = ("confirmed", "delivered")

def _company_ids(user) -> List[int]:
    """
    FR: Retourne les IDs des entreprises actives appartenant à l'utilisateur.
    Dépend uniquement du modèle Company pour éviter tout import circulaire.
    """
    if not user or not getattr(user, "id", None):
        return []
    return list(
        Company.objects.filter(owner=user, is_active=True).values_list("id", flat=True)
    )

# ============================================================
# Mixin de portée (scope) unifié pour toutes les vues analytics
# ============================================================

class AnalyticsScopeMixin:
    """
    FR: Mixin qui unifie le comportement “producer vs admin”.
    - initialize_scope(request, **kwargs): lit le scope depuis l'URL (kwargs["scope"])
      * "admin": vision globale
      * "producer": restreint aux entreprises du user
    - get_orders(request, date_from, date_to): renvoie la QuerySet d'Order filtrée par scope.
      (Les vues concrètes font leur propre .filter/.annotate ultérieurement.)
    - producer_names_from_snapshot(snapshot): extrait {producer_name, store_name} depuis les snapshots.
    - bucket_anchor_date(dt, bucket): date “ancre” (YYYY-MM-DD) pour un bucket day/week/month.
    - normalize_producer_meta(meta): normalise les métadonnées producteur pour réponses homogènes.
    """

    permission_classes = [IsAuthenticated]

    def initialize_scope(self, request, **kwargs):
        # FR: scope vient du paramètre passé dans urls.py {"scope": "producer" | "admin"}
        raw = (kwargs or {}).get("scope") or request.GET.get("scope") or "producer"
        self.scope = raw.lower()
        self.is_admin_scope = (self.scope == "admin")

    # ---- API attendue par les vues unifiées ----
    def get_orders(self, request, date_from=None, date_to=None):
        """
        FR: Filtres communs:
        - Statut ∈ VALID_STATUSES
        - bornes de date (création)
        - si producer: restreint aux bundles contenant au moins un produit des companies du user
        """
        from .models import Order  # import local pour éviter dépendances au chargement
        qs = Order.objects.filter(status__in=VALID_STATUSES)
        if date_from:
            qs = qs.filter(created_at__date__gte=date_from)
        if date_to:
            qs = qs.filter(created_at__date__lte=date_to)

        if not self.is_admin_scope:
            co_ids = _company_ids(request.user)
            if co_ids:
                qs = qs.filter(items__bundle__items__product__company_id__in=co_ids).distinct()
            else:
                qs = qs.none()
        return qs

    # ---- Métadonnées producteur (toujours présentes, admin ou producer) ----
    def producer_names_from_snapshot(self, snapshot: dict) -> Optional[List[Dict[str, Optional[str]]]]:
        """
        FR: Extrait une liste de producteurs présents dans le bundle snapshot.
        Retourne une liste d’objets: [{"producer_name": str|None, "store_name": str|None}, ...]
        """
        if not isinstance(snapshot, dict):
            return None
        prods = snapshot.get("products") or []
        out: List[Dict[str, Optional[str]]] = []
        seen = set()
        for p in prods:
            cid = p.get("company_id")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            out.append({
                "producer_name": p.get("company_owner_name") or p.get("company_name"),  # fallback si tu stockes owner_name
                "store_name": p.get("company_name"),
            })
        return out or None

    def bucket_anchor_date(self, dt, bucket: str) -> str:
        """
        FR: Retourne la date “ancre” (YYYY-MM-DD) pour un bucket.
        - day   -> date de l’évènement
        - week  -> lundi ISO de la semaine
        - month -> premier jour du mois
        """
        b = (bucket or "week").lower()
        if b == "day":
            try:
                return dt.date().isoformat()
            except Exception:
                return timezone.localdate().isoformat()
        if b == "month":
            return f"{dt.year:04d}-{dt.month:02d}-01"
        # week (ISO)
        iso = dt.isocalendar()
        from datetime import datetime as _dt
        return _dt.fromisocalendar(iso.year, iso.week, 1).date().isoformat()

    def normalize_producer_meta(self, names: Optional[List[Dict[str, Optional[str]]]]) -> List[Dict[str, Optional[str]]]:
        """
        FR: Garanti la présence de la structure [{producer_name, store_name}] même si aucune info.
        """
        if not names:
            return [{"producer_name": None, "store_name": None}]
        # nettoyage minimal: force les deux clés
        normalized: List[Dict[str, Optional[str]]] = []
        for n in names:
            normalized.append({
                "producer_name": (n or {}).get("producer_name"),
                "store_name": (n or {}).get("store_name"),
            })
        return normalized
