from decimal import Decimal, ROUND_HALF_UP
from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import ProductBundle, ProductBundleItem

class Command(BaseCommand):
    help = (
        "Recompute ProductBundle.total_avoided_waste_kg and total_avoided_co2_kg "
        "by summing ProductBundleItem.avoided_waste_kg and avoided_co2_kg. "
        "Does NOT modify items. "
        "Default: update only bundles whose totals differ from the items' sums. "
        "Use --all to scan all bundles. Use --dry-run to preview."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--all",
            action="store_true",
            dest="force_all",
            help="Scan all bundles (still only writes when the stored totals differ from computed sums).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help="Preview changes without saving.",
        )

    def handle(self, *args, **options):
        force_all = options.get("force_all", False)
        dry = options.get("dry_run", False)

        qs = ProductBundle.objects.all() if force_all else ProductBundle.objects.all()
        # Default and --all both scan all; we still only write when mismatch exists.

        updated = 0
        unchanged = 0
        empty_items = 0

        for bundle in qs.iterator():
            items = ProductBundleItem.objects.filter(bundle=bundle, is_active=True)

            if not items.exists():
                # No active items: computed totals are 0.00; update only if stored differs.
                sum_waste_q = Decimal("0.00")
                sum_co2_q = Decimal("0.00")
                if (bundle.total_avoided_waste_kg != sum_waste_q) or (bundle.total_avoided_co2_kg != sum_co2_q):
                    if dry:
                        self.stdout.write(
                            f"[DRY] Bundle {bundle.id} '{bundle.title}': "
                            f"{bundle.total_avoided_waste_kg}/{bundle.total_avoided_co2_kg} -> 0.00/0.00 (no active items)"
                        )
                    else:
                        with transaction.atomic():
                            ProductBundle.objects.filter(pk=bundle.pk).update(
                                total_avoided_waste_kg=sum_waste_q,
                                total_avoided_co2_kg=sum_co2_q,
                            )
                    updated += 1
                else:
                    unchanged += 1
                empty_items += 1
                continue

            sum_waste = Decimal("0.0")
            sum_co2 = Decimal("0.0")
            for it in items:
                sum_waste += (it.avoided_waste_kg or Decimal("0"))
                sum_co2 += (it.avoided_co2_kg or Decimal("0"))

            sum_waste_q = sum_waste.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            sum_co2_q = sum_co2.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            if (bundle.total_avoided_waste_kg != sum_waste_q) or (bundle.total_avoided_co2_kg != sum_co2_q):
                if dry:
                    self.stdout.write(
                        f"[DRY] Bundle {bundle.id} '{bundle.title}': "
                        f"{bundle.total_avoided_waste_kg}/{bundle.total_avoided_co2_kg} -> "
                        f"{sum_waste_q}/{sum_co2_q}"
                    )
                else:
                    with transaction.atomic():
                        ProductBundle.objects.filter(pk=bundle.pk).update(
                            total_avoided_waste_kg=sum_waste_q,
                            total_avoided_co2_kg=sum_co2_q,
                        )
                updated += 1
            else:
                unchanged += 1

        msg = f"Done. Updated={updated}, Unchanged={unchanged}, EmptyBundles={empty_items}, ForcedAll={force_all}, DryRun={dry}"
        if dry:
            self.stdout.write(self.style.WARNING(msg))
        else:
            self.stdout.write(self.style.SUCCESS(msg))
