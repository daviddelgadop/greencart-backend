from decimal import Decimal
from core.models import ProductBundle, ProductImpact, Order

def fix_bundle_snapshots(bundle_id: int):
    try:
        bundle = ProductBundle.objects.get(id=bundle_id)
    except ProductBundle.DoesNotExist:
        print(f"❌ Le bundle {bundle_id} n’existe pas")
        return

    orders = Order.objects.filter(items__bundle=bundle).distinct()
    print(f"🔎 {orders.count()} commandes trouvées avec le bundle « {bundle.title} ».")

    for order in orders:
        changed = False
        for item in order.items.filter(bundle=bundle):
            print(f"Avant => OrderItem {item.id}: gaspillage={item.order_item_total_avoided_waste_kg}, CO₂={item.order_item_total_avoided_co2_kg}")

            waste = Decimal("0.0")
            co2 = Decimal("0.0")

            for bi in bundle.items.all():
                prod = bi.product
                impact_entry = ProductImpact.objects.filter(
                    product=prod.catalog_entry,
                    unit=prod.unit
                ).order_by("quantity").first()
                if impact_entry:
                    multiplier = Decimal(bi.quantity) / impact_entry.quantity
                    waste += multiplier * impact_entry.avoided_waste_kg
                    co2 += multiplier * impact_entry.avoided_co2_kg
                else:
                    print(f"⚠️ Aucun impact trouvé pour {prod.title} ({prod.unit})")

            item.order_item_total_avoided_waste_kg = waste
            item.order_item_total_avoided_co2_kg = co2
            item.save(update_fields=["order_item_total_avoided_waste_kg", "order_item_total_avoided_co2_kg"])

            if hasattr(item, "snapshot") and item.snapshot:
                snap = item.snapshot
                snap.order_item_total_avoided_waste_kg = waste
                snap.order_item_total_avoided_co2_kg = co2
                snap.save(update_fields=["order_item_total_avoided_waste_kg", "order_item_total_avoided_co2_kg"])
                print(f"   ↳ Snapshot de l’item {item.id} mis à jour")

            changed = True
            print(f"Après => OrderItem {item.id}: gaspillage={waste}, CO₂={co2}")

        if changed:
            order.update_totals()
            order.save(update_fields=[
                "order_total_avoided_waste_kg",
                "order_total_avoided_co2_kg",
                "order_total_savings"
            ])

            if hasattr(order, "snapshot") and order.snapshot:
                order_snap = order.snapshot
                order_snap.order_total_avoided_waste_kg = order.order_total_avoided_waste_kg
                order_snap.order_total_avoided_co2_kg = order.order_total_avoided_co2_kg
                order_snap.order_total_savings = order.order_total_savings
                order_snap.save(update_fields=[
                    "order_total_avoided_waste_kg",
                    "order_total_avoided_co2_kg",
                    "order_total_savings"
                ])
                print(f"✔ Snapshot de la commande {order.id} mis à jour")

            print(f"✔ Totaux de la commande {order.id} recalculés\n")

fix_bundle_snapshots(48)
