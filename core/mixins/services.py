from decimal import Decimal
from django.conf import settings
from core.models import Reward, RewardTier, RewardStatus, RewardBenefit, UserRewardProgress

def _grant_reward(user, tier: RewardTier):
    if Reward.objects.filter(user=user, tier=tier).exists():
        return
    status = RewardStatus.NONE
    payload = {}
    if tier.benefit_kind == RewardBenefit.COUPON:
        if not getattr(settings, 'REWARDS_ENABLE_COUPONS', False):
            status = RewardStatus.BLOCKED
        else:
            status = RewardStatus.FULFILLED
            payload = {}
    elif tier.benefit_kind == RewardBenefit.FREESHIP:
        status = RewardStatus.BLOCKED
    Reward.objects.create(
        user=user,
        tier=tier,
        title=tier.title,
        description=tier.description,
        benefit_status=status,
        benefit_payload=payload,
    )

def update_rewards_for_order(order):
    prog, _ = UserRewardProgress.objects.get_or_create(user=order.user)
    prog.total_orders += 1
    prog.total_waste_kg = (prog.total_waste_kg + Decimal(order.order_total_avoided_waste_kg or 0)).quantize(Decimal('0.01'))
    prog.total_co2_kg = (prog.total_co2_kg + Decimal(order.order_total_avoided_co2_kg or 0)).quantize(Decimal('0.01'))
    prog.total_savings_eur = (prog.total_savings_eur + Decimal(order.order_total_savings or 0)).quantize(Decimal('0.01'))
    prog.save()

    tiers = RewardTier.objects.filter(is_active=True)
    owned = set(Reward.objects.filter(user=order.user).values_list('tier_id', flat=True))
    for t in tiers:
        if t.id in owned:
            continue
        if (
            prog.total_orders >= t.min_orders and
            prog.total_waste_kg >= t.min_waste_kg and
            prog.total_co2_kg >= t.min_co2_kg and
            prog.producers_supported >= t.min_producers_supported and
            prog.total_savings_eur >= t.min_savings_eur
        ):
            _grant_reward(order.user, t)
