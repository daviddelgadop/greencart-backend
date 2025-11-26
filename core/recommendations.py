# core/recommendations.py
from collections import Counter, defaultdict
from typing import Iterable, Set, Dict, Tuple

"""
Simple item-to-item co-purchase logic:
- Start from bundles the user already bought (user_bundle_ids).
- Look at other users who bought any of those bundles; collect the other bundles they bought.
- Score candidates by co-purchase count, with optional tie-break boosts.
- Return a sorted list of (bundle_id, score).
"""

def rank_copurchased_candidates(
    user_bundle_ids: Set[int],
    other_users_purchases: Iterable[Iterable[int]],
) -> Dict[int, float]:
    candidate_counts = Counter()
    for purchases in other_users_purchases:
        s = set(purchases)
        if not s or not (s & user_bundle_ids):
            continue
        for b in (s - user_bundle_ids):
            candidate_counts[b] += 1

    # Basic normalized score = count; you can add extra light boosts later.
    # Return plain dict for easy annotation/ordering in ORM.
    return {bid: float(cnt) for bid, cnt in candidate_counts.items()}
