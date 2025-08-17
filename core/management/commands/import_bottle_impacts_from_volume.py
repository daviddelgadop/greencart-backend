from django.core.management.base import BaseCommand
from django.db import transaction
from decimal import Decimal
from core.models import ProductCatalog, ProductImpact

# Edit this list to control which bottle units will be created
# Format: (unit_key, liters_equivalent)
BOTTLE_SPECS = [
    ("bouteille 75 cl", Decimal("0.75")),
    ("bouteille 50 cl", Decimal("0.50")),
    ("bouteille 25 cl", Decimal("0.25")),
    ("bouteille 1 l",  Decimal("1.0")),
]

# Legacy underscore keys to purge
LEGACY_UNITS = ("bouteille_75cl", "bouteille_50cl", "bouteille_25cl")


class Command(BaseCommand):
    help = "Purge old bottle impact units and recreate bottle impacts with flexible target units."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Compute and print changes without writing to the database.",
        )
        parser.add_argument(
            "--purge-legacy",
            action="store_true",
            help="Delete legacy underscore units (bouteille_75cl/50cl/25cl) before recreating.",
        )
        parser.add_argument(
            "--purge-existing",
            action="store_true",
            help="Delete any existing impacts for units listed in BOTTLE_SPECS before recreating.",
        )
        parser.add_argument(
            "--purge-all-bottles",
            action="store_true",
            help="Delete any impacts whose unit contains 'bouteille' (useful when normalizing keys).",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        purge_legacy = options["purge_legacy"]
        purge_existing = options["purge_existing"]
        purge_all_bottles = options["purge_all_bottles"]

        units_to_create = tuple(u for u, _ in BOTTLE_SPECS)
        created = 0
        skipped = 0
        deleted_legacy = 0
        deleted_existing = 0
        deleted_all_bottles = 0
        rows_preview = []

        with transaction.atomic():
            if purge_legacy:
                qs = ProductImpact.objects.filter(unit__in=LEGACY_UNITS)
                deleted_legacy = qs.count()
                if not dry_run:
                    qs.delete()

            if purge_existing:
                qs = ProductImpact.objects.filter(unit__in=units_to_create)
                deleted_existing = qs.count()
                if not dry_run:
                    qs.delete()

            if purge_all_bottles:
                candidates = ProductImpact.objects.filter(unit__icontains="bouteille")
                if dry_run:
                    deleted_all_bottles = candidates.count()
                else:
                    deleted_all_bottles = 0
                    for pi in candidates.iterator():
                        pi.delete()
                        deleted_all_bottles += 1

            for product in ProductCatalog.objects.all().iterator():
                base = (
                    ProductImpact.objects.filter(product=product, unit="l")
                    .order_by("-weight_equivalent_kg")
                    .first()
                )
                if base is None:
                    base = (
                        ProductImpact.objects.filter(product=product, unit="cl")
                        .order_by("-weight_equivalent_kg")
                        .first()
                    )

                if base is None or not base.weight_equivalent_kg or base.weight_equivalent_kg <= 0:
                    skipped += 1
                    continue

                liters_equiv = float(base.weight_equivalent_kg)
                if liters_equiv <= 0:
                    skipped += 1
                    continue

                waste_per_l = float(base.avoided_waste_kg) / liters_equiv
                co2_per_l = float(base.avoided_co2_kg) / liters_equiv

                for unit_key, liters in BOTTLE_SPECS:
                    liters_f = float(liters)
                    weight_equiv = round(liters_f, 3)
                    waste = round(waste_per_l * liters_f, 3)
                    co2 = round(co2_per_l * liters_f, 3)

                    rows_preview.append({
                        "product": product.name,
                        "based_on": f'{base.unit} (weight_kg={base.weight_equivalent_kg}, waste_kg={base.avoided_waste_kg}, co2_kg={base.avoided_co2_kg})',
                        "unit": unit_key,
                        "quantity": 1,
                        "weight_equivalent_kg": weight_equiv,
                        "avoided_waste_kg": waste,
                        "avoided_co2_kg": co2,
                    })

                    if dry_run:
                        continue

                    ProductImpact.objects.update_or_create(
                        product=product,
                        unit=unit_key,
                        quantity=Decimal("1"),
                        defaults={
                            "weight_equivalent_kg": Decimal(str(weight_equiv)),
                            "avoided_waste_kg": Decimal(str(waste)),
                            "avoided_co2_kg": Decimal(str(co2)),
                        },
                    )
                    created += 1

            if dry_run:
                transaction.set_rollback(True)

        self.stdout.write(self.style.SUCCESS(
            f"Deleted legacy: {deleted_legacy} | "
            f"Deleted existing (BOTTLE_SPECS): {deleted_existing} | "
            f"Deleted all 'bouteille': {deleted_all_bottles} | "
            f"Created: {created} | Skipped (no l/cl baseline): {skipped}"
        ))

        header = "Produit,Basé sur,Unité,Quantité,weight_equivalent_kg,avoided_waste_kg,avoided_co2_kg"
        print(header)
        for r in rows_preview:
            print(
                f"{r['product']},\"{r['based_on']}\",{r['unit']},{r['quantity']},{r['weight_equivalent_kg']},{r['avoided_waste_kg']},{r['avoided_co2_kg']}"
            )
