from django.core.management.base import BaseCommand
from decimal import Decimal
from core.models import (
    Order, UserRewardProgress, Reward, RewardTier, CustomUser
)

def collect_producer_ids_from_order(order):
    ids = set()
    for oi in order.items.all():
        for p in (oi.bundle_snapshot or {}).get("products", []):
            cid = p.get("company_id")
            if cid:
                ids.add(int(cid))
    return ids

def rebuild_progress_for_user(user):
    orders = Order.objects.filter(user=user, is_active=True).prefetch_related("items")
    prog, _ = UserRewardProgress.objects.get_or_create(user=user)

    total_orders = 0
    total_waste = Decimal("0.00")
    total_co2 = Decimal("0.00")
    total_savings = Decimal("0.00")
    producers = set()

    for o in orders:
        total_orders += 1
        total_waste += Decimal(o.order_total_avoided_waste_kg or 0)
        total_co2 += Decimal(o.order_total_avoided_co2_kg or 0)
        total_savings += Decimal(o.order_total_savings or 0)
        producers |= collect_producer_ids_from_order(o)

    prog.total_orders = total_orders
    prog.total_waste_kg = total_waste.quantize(Decimal("0.01"))
    prog.total_co2_kg = total_co2.quantize(Decimal("0.01"))
    prog.total_savings_eur = total_savings.quantize(Decimal("0.01"))
    prog.producers_supported = len(producers)
    prog.seen_producer_ids = sorted(producers)
    prog.save()

    Reward.objects.filter(user=user).delete()
    tiers = RewardTier.objects.filter(is_active=True)
    for t in tiers:
        if (
            prog.total_orders >= t.min_orders and
            prog.total_waste_kg >= t.min_waste_kg and
            prog.total_co2_kg >= t.min_co2_kg and
            prog.producers_supported >= t.min_producers_supported and
            prog.total_savings_eur >= t.min_savings_eur
        ):
            Reward.objects.create(user=user, title=t.title, description=t.description, tier=t)

class Command(BaseCommand):
    help = "Reassign all orders from old_email to new_email and rebuild progress for both users"

    def add_arguments(self, parser):
        parser.add_argument("old_email", type=str)
        parser.add_argument("new_email", type=str)

    def handle(self, *args, **options):
        old_email = options["old_email"]
        new_email = options["new_email"]

        old_user = CustomUser.objects.get(email=old_email)
        new_user = CustomUser.objects.get(email=new_email)

        # Reassign orders
        updated = Order.objects.filter(user=old_user).update(user=new_user)

        # Rebuild progress for both
        rebuild_progress_for_user(old_user)
        rebuild_progress_for_user(new_user)

        self.stdout.write(self.style.SUCCESS(
            f"✅ {updated} orders moved from {old_email} to {new_email}.\n"
            f"Progress rebuilt for both users."
        ))
