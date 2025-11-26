from decimal import Decimal
from django.core.management.base import BaseCommand
from core.models import Order, ProductImpact

class Command(BaseCommand):
    help = "Recompute avoided_waste/co2 for OrderItems (and Orders) from their bundles."

    def add_arguments(self, parser):
        parser.add_argument(
            "--bundle-id",
            type=int,
            dest="bundle_id",
            help="Restrict to a single bundle id.",
        )

    def handle(self, *args, **opts):
        only_bundle_id = opts.get("bundle_id")

        qs = Order.objects.all()
        if only_bundle_id:
            qs = qs.filter(items__bundle_id=only_bundle_id).distinct()

        updated_orders = 0
        updated_items = 0

        for order in qs:
            changed = False
            for item in order.items.all():
                bundle = item.bundle
                if only_bundle_id and bundle.id != only_bundle_id:
                    continue

                waste = Decimal("0.0")
                co2 = Decimal("0.0")
                for bi in bundle.items.all():
                    prod = bi.product
                    impact = ProductImpact.objects.filter(
                        product=prod.catalog_entry,
                        unit=prod.unit
                    ).order_by("quantity").first()
                    if impact:
                        factor = Decimal(bi.quantity) / Decimal(impact.quantity)
                        waste += factor * Decimal(impact.avoided_waste_kg)
                        co2 += factor * Decimal(impact.avoided_co2_kg)

                if item.order_item_total_avoided_waste_kg != waste or item.order_item_total_avoided_co2_kg != co2:
                    item.order_item_total_avoided_waste_kg = waste
                    item.order_item_total_avoided_co2_kg = co2
                    item.save(update_fields=[
                        "order_item_total_avoided_waste_kg",
                        "order_item_total_avoided_co2_kg"
                    ])
                    updated_items += 1
                    changed = True

            if changed:
                order.update_totals()
                order.save(update_fields=[
                    "order_total_avoided_waste_kg",
                    "order_total_avoided_co2_kg",
                    "order_total_savings"
                ])
                updated_orders += 1

        self.stdout.write(self.style.SUCCESS(
            f"Done. Orders updated={updated_orders}, items updated={updated_items}"
        ))
