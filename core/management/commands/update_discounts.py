from decimal import Decimal, ROUND_HALF_UP
from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import ProductBundle, ProductBundleItem

Q0 = Decimal("0")
Q01 = Decimal("0.01")
HUP = ROUND_HALF_UP


def next_multiple_of_5_up(pct: int) -> int:
    """
    Move to the next multiple of 5 upwards.
    Examples: 5 -> 10, 34 -> 35, 99 -> 100, 100 -> 100, <=0 -> 0.
    """
    if pct <= 0:
        return 0
    if pct >= 100:
        return 100
    return min(100, ((pct // 5) + 1) * 5)


def ensure_original_price(bundle: ProductBundle) -> Decimal:
    """
    Ensure original_price is available. If missing or zero, compute from active items.
    """
    if bundle.original_price is not None and Decimal(bundle.original_price) > Q0:
        return Decimal(bundle.original_price)

    total = Q0
    items = ProductBundleItem.objects.filter(bundle=bundle, is_active=True).select_related("product")
    for it in items:
        total += (Decimal(it.product.original_price) * Decimal(it.quantity))
    return total.quantize(Q01, rounding=HUP)


class Command(BaseCommand):
    help = (
        "Bump discounted_percentage to the next multiple of 5 (if > 0) for all bundles, "
        "then recompute discounted_price from original_price. "
        "If discounted_percentage is 0, the bundle is skipped. "
        "Use --dry-run to preview; use --bundle-id to target a single bundle."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help="Preview changes without saving.",
        )
        parser.add_argument(
            "--bundle-id",
            type=int,
            dest="bundle_id",
            help="Restrict to a single bundle id.",
        )

    def handle(self, *args, **opts):
        dry = opts.get("dry_run", False)
        only_id = opts.get("bundle_id")

        qs = ProductBundle.objects.all()
        if only_id:
            qs = qs.filter(id=only_id)

        updated = 0
        skipped = 0

        for b in qs.iterator():
            old_pct = int(b.discounted_percentage or 0)
            if old_pct == 0:
                skipped += 1
                continue

            new_pct = next_multiple_of_5_up(old_pct)
            if new_pct == old_pct:
                # Already at an upward multiple of 5 (e.g., 10, 15, 20...) and not needing a bump
                skipped += 1
                continue

            orig_price = ensure_original_price(b)
            new_price = (orig_price * (Decimal("1") - Decimal(new_pct) / Decimal("100"))).quantize(Q01, rounding=HUP)

            if dry:
                self.stdout.write(
                    f"[DRY] Bundle {b.id} '{b.title}': "
                    f"discount {old_pct}% -> {new_pct}%, "
                    f"price {b.discounted_price} -> {new_price} (orig {orig_price})"
                )
                updated += 1
                continue

            with transaction.atomic():
                ProductBundle.objects.filter(pk=b.pk).update(
                    discounted_percentage=new_pct,
                    discounted_price=new_price,
                    original_price=orig_price,
                )
            updated += 1

        msg = f"Done. Updated={updated}, Skipped={skipped}, DryRun={dry}"

        # FIX: do not nest self.stdout.write() calls
        if dry:
            self.stdout.write(self.style.WARNING(msg))
        else:
            self.stdout.write(self.style.SUCCESS(msg))
