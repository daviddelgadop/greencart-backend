from decimal import Decimal, ROUND_HALF_UP
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from core.models import Order, OrderItem, ProductImpact

QZERO = Decimal("0")
Q001 = Decimal("0.001")


def recompute_item_from_bundle(item: OrderItem):
    bundle = item.bundle
    waste = QZERO
    co2 = QZERO

    for bi in bundle.items.all():
        prod = bi.product
        impact = (
            ProductImpact.objects
            .filter(product=prod.catalog_entry, unit=prod.unit)
            .order_by("quantity")
            .first()
        )
        if not impact:
            continue

        base_qty = Decimal(impact.quantity or 0)
        if base_qty == QZERO:
            continue

        factor = Decimal(bi.quantity) / base_qty
        waste += factor * Decimal(impact.avoided_waste_kg)
        co2 += factor * Decimal(impact.avoided_co2_kg)

    return (
        waste.quantize(Q001, rounding=ROUND_HALF_UP),
        co2.quantize(Q001, rounding=ROUND_HALF_UP),
    )


class Command(BaseCommand):
    help = "Recompute orders with zero totals OR orders that contain items with zero impact. Accepts optional --bundle-id."

    def add_arguments(self, parser):
        parser.add_argument(
            "--bundle-id",
            type=int,
            dest="bundle_id",
            help="Restrict to a single bundle id.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            dest="dry_run",
            help="Preview without saving.",
        )

    def handle(self, *args, **opts):
        only_bundle_id = opts.get("bundle_id")
        dry = opts.get("dry_run", False)

        orders_q = Order.objects.filter(
            Q(order_total_avoided_waste_kg=0, order_total_avoided_co2_kg=0)
            | Q(items__order_item_total_avoided_waste_kg=0)
            | Q(items__order_item_total_avoided_co2_kg=0)
        ).distinct()

        if only_bundle_id:
            orders_q = orders_q.filter(items__bundle_id=only_bundle_id).distinct()

        updated_orders = 0
        updated_items = 0

        for order in orders_q.iterator():
            changed = False

            # If order totals are zero, recompute all its items.
            # Else recompute only items that are zero.
            if order.order_total_avoided_waste_kg == 0 and order.order_total_avoided_co2_kg == 0:
                items_q = order.items.all()
            else:
                items_q = order.items.filter(
                    Q(order_item_total_avoided_waste_kg=0) | Q(order_item_total_avoided_co2_kg=0)
                )

            if only_bundle_id:
                items_q = items_q.filter(bundle_id=only_bundle_id)

            for it in items_q:
                new_waste, new_co2 = recompute_item_from_bundle(it)

                if (it.order_item_total_avoided_waste_kg == new_waste and
                    it.order_item_total_avoided_co2_kg == new_co2):
                    continue

                if dry:
                    self.stdout.write(
                        f"[DRY] OrderItem {it.id} (order={it.order_id}, bundle={it.bundle_id}) "
                        f"{it.order_item_total_avoided_waste_kg}/{it.order_item_total_avoided_co2_kg} -> "
                        f"{new_waste}/{new_co2}"
                    )
                    updated_items += 1
                    changed = True
                    continue

                with transaction.atomic():
                    OrderItem.objects.filter(pk=it.pk).update(
                        order_item_total_avoided_waste_kg=new_waste,
                        order_item_total_avoided_co2_kg=new_co2,
                    )
                updated_items += 1
                changed = True

            if changed:
                if dry:
                    self.stdout.write(f"[DRY] Would refresh Order {order.id} totals")
                    updated_orders += 1
                    continue

                with transaction.atomic():
                    order.update_totals()
                    order.save(update_fields=[
                        "order_total_avoided_waste_kg",
                        "order_total_avoided_co2_kg",
                        "order_total_savings",
                    ])
                updated_orders += 1

        self.stdout.write(self.style.SUCCESS(
            f"Done. Orders updated={updated_orders}, items updated={updated_items}"
        ))
