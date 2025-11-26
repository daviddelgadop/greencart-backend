import random
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple, List

import requests
from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from django.utils.text import slugify

from core.models import (
    CustomUser,
    Company,
    Certification,
    ProductCatalog,
    ProductImpact,
    Product,
    ProductImage,
    ProductBundle,
    ProductBundleItem,
)

HEADERS = {"User-Agent": "GreenCart Seeder/1.0"}

ALLOWED_UNITS_PRIORITY: Tuple[str, ...] = (
    "kg",
    "l",
    "bouteille 1 l",
    "bouteille 75 cl",
    "bouteille 50 cl",
    "bouteille 25 cl",
    "pièce",
    "boîte",
    "botte",
)
FORBIDDEN_UNITS: Tuple[str, ...] = ("g", "cl", "ml", "centilitre", "millilitre")


def q(val_min: float, val_max: float) -> Decimal:
    v = random.uniform(val_min, val_max)
    return Decimal(v).quantize(Decimal("0.10"), rounding=ROUND_HALF_UP)


def suggest_price(unit: str, catalog_name: str) -> Decimal:
    name = (catalog_name or "").lower()
    u = (unit or "").lower()

    if u == "kg":
        if any(k in name for k in ["fromage", "cheese"]):
            return q(12, 26)
        if any(k in name for k in ["viande", "bœuf", "boeuf", "porc", "volaille"]):
            return q(10, 25)
        if any(k in name for k in ["poisson", "saumon", "truite", "bar"]):
            return q(14, 28)
        return q(2.0, 9.0)
    if u == "l":
        if "lait" in name:
            return q(0.9, 2.0)
        if any(k in name for k in ["jus", "kéfir", "kefir"]):
            return q(2.0, 5.0)
        return q(1.0, 6.0)
    if u.startswith("bouteille"):
        if any(k in name for k in ["vin", "cidre", "bière", "biere"]):
            return q(4.0, 14.0)
        return q(3.0, 10.0)
    if u in ("pièce", "boîte", "botte"):
        if any(k in name for k in ["œuf", "oeuf"]):
            return q(2.0, 6.0)
        return q(1.2, 8.0)
    return q(2.0, 10.0)


def unit_quantity_label(unit: str) -> str:
    u = (unit or "").lower()
    if u == "kg":
        return "1kg"
    if u == "l":
        return "1L"
    if u == "bouteille 1 l":
        return "1L"
    if u == "bouteille 75 cl":
        return "75cl"
    if u == "bouteille 50 cl":
        return "50cl"
    if u == "bouteille 25 cl":
        return "25cl"
    if u == "pièce":
        return "1pc"
    if u in ("boîte", "botte"):
        return "1"
    return "1"


def pick_unit_from_impacts(catalog: ProductCatalog) -> Optional[str]:
    units = list(
        ProductImpact.objects.filter(product=catalog)
        .values_list("unit", flat=True)
        .distinct()
    )
    normalized = [u for u in units if u and u.lower() not in FORBIDDEN_UNITS]
    for pref in ALLOWED_UNITS_PRIORITY:
        for u in normalized:
            if u.lower() == pref:
                return u
    return normalized[0] if normalized else None


def pick_storage_token(catalog: ProductCatalog) -> str:
    # Model expects short tokens: "réfrigéré", "frais", "surgelé", "temp_ambiante", "sec"
    name = (getattr(catalog, "name", "") or "").lower()
    if any(k in name for k in ["fromage", "yaourt", "lait", "crème", "creme"]):
        return "réfrigéré"
    if any(k in name for k in ["viande", "poisson", "saumon", "truite", "bar"]):
        return "réfrigéré"
    if any(k in name for k in ["surgelé", "congelé", "surgele", "congele"]):
        return "surgelé"
    if any(k in name for k in ["pomme", "poire", "carotte", "tomate", "salade", "légume", "legume", "fruit"]):
        return "frais"
    return "temp_ambiante"


def description_for(catalog: ProductCatalog, unit: str) -> str:
    storage = pick_storage_token(catalog)
    return f"Sélection locale de {catalog.name}. Conditionnement: {unit_quantity_label(unit)}. Conservation: {storage}."


def variety_for(catalog: ProductCatalog) -> str:
    name = (catalog.name or "").lower()
    mapping = {
        "pomme": ["Gala", "Golden", "Fuji", "Pink Lady", "Reinette"],
        "poire": ["Conférence", "Comice", "Williams"],
        "tomate": ["Cœur de bœuf", "Cerise", "Roma"],
        "carotte": ["Nantaise", "Touchon"],
        "salade": ["Batavia", "Romaine", "Feuille de chêne"],
        "fraise": ["Gariguette", "Mara des bois"],
        "yaourt": ["Nature", "Vanille", "Fraise"],
        "fromage": ["Tomme", "Comté", "Bleu"],
        "vin": ["Rouge", "Blanc", "Rosé"],
        "bière": ["Blonde", "Ambrée", "Brune"],
        "pain": ["Campagne", "Complet", "Seigle"],
        "jus": ["Pomme", "Raisin"],
    }
    for k, opts in mapping.items():
        if k in name:
            return random.choice(opts)
    return catalog.name


def themed_image_url(catalog_name: str, seed: int) -> str:
    keywords = [
        ("pomme", "apple,fruit"),
        ("poire", "pear,fruit"),
        ("fraise", "strawberry,fruit"),
        ("framboise", "raspberry,fruit"),
        ("myrtille", "blueberry,fruit"),
        ("raisin", "grapes,fruit"),
        ("tomate", "tomato,vegetable"),
        ("carotte", "carrot,vegetable"),
        ("salade", "lettuce,vegetable"),
        ("lait", "milk,dairy"),
        ("fromage", "cheese,dairy"),
        ("yaourt", "yogurt,dairy"),
        ("miel", "honey,food"),
        ("confiture", "jam,food"),
        ("jus", "juice,drink"),
        ("vin", "wine,bottle"),
        ("bière", "beer,bottle"),
        ("pain", "bread,bakery"),
        ("œuf", "eggs,egg"),
        ("oeuf", "eggs,egg"),
    ]
    name_l = (catalog_name or "").lower()
    for key, tag in keywords:
        if key in name_l:
            return f"https://loremflickr.com/1280/960/{tag}?lock={seed}"
    return f"https://picsum.photos/seed/{slugify(catalog_name) or seed}/1280/960"


def fetch_image_bytes(url: str) -> Optional[bytes]:
    try:
        r = requests.get(url, timeout=12, headers=HEADERS)
        if r.status_code == 200 and r.content:
            return r.content
    except Exception:
        pass
    try:
        r = requests.get("https://picsum.photos/1280/960", timeout=12, headers=HEADERS)
        if r.status_code == 200 and r.content:
            return r.content
    except Exception:
        pass
    return None


def pick_company(producer: CustomUser) -> Optional[Company]:
    return Company.objects.filter(owner=producer, is_active=True).order_by("id").first()


def pick_cert(company: Company) -> Optional[Certification]:
    return Certification.objects.filter(company=company, is_active=True).order_by("?").first()


class Command(BaseCommand):
    help = "Create Products (default 5 per producer, max 200) and one Bundle per new Product."

    def add_arguments(self, parser):
        parser.add_argument("--per-producer", dest="per_producer", type=int, default=5)
        parser.add_argument("--max-products", dest="max_products", type=int, default=200)
        parser.add_argument("--publish-bundles", dest="publish_bundles", action="store_true")
        parser.add_argument("--no-images", dest="no_images", action="store_true")
        parser.add_argument("--seed", dest="seed", type=int, default=12345)
        parser.add_argument("--dry-run", dest="dry_run", action="store_true")

    def handle(self, *args, **opts):
        random.seed(int(opts.get("seed", 12345)))
        per_producer = int(opts.get("per_producer", 5))
        cap = int(opts.get("max_products", 200))
        publish_bundles = bool(opts.get("publish_bundles", False))
        skip_images = bool(opts.get("no_images", False))
        dry = bool(opts.get("dry_run", False))

        producers = list(CustomUser.objects.filter(type="producer", is_active=True).order_by("id"))
        if not producers:
            self.stdout.write(self.style.WARNING("No active producers found."))
            return

        catalogs = list(
            ProductCatalog.objects.filter(is_active=True)
            .exclude(Q(name__istartswith="Autre") | Q(name__istartswith="Autres"))
            .order_by("id")
        )
        if not catalogs:
            self.stdout.write(self.style.WARNING("No valid ProductCatalog rows found."))
            return

        created_products: List[Product] = []
        created_bundles: List[ProductBundle] = []

        @transaction.atomic
        def create_all():
            total = 0
            for producer in producers:
                if total >= cap:
                    break
                company = pick_company(producer)
                if not company:
                    continue

                cert = pick_cert(company)

                random.shuffle(catalogs)
                picked = catalogs[: per_producer]

                for catalog in picked:
                    if total >= cap:
                        break

                    unit = pick_unit_from_impacts(catalog)
                    if not unit:
                        continue

                    stock = random.randint(10, 40)
                    variet = variety_for(catalog)
                    desc = description_for(catalog, unit)
                    price_orig = suggest_price(unit, catalog.name)
                    storage_token = pick_storage_token(catalog)

                    product = Product(
                        company=company,
                        title=catalog.name,
                        variety=variet,
                        description=desc,
                        catalog_entry=catalog,
                        original_price=price_orig,
                        stock=stock,
                        unit=unit,
                        storage_instructions=storage_token,
                        eco_score=getattr(catalog, "eco_score", None),
                        is_active=True,
                    )
                    product.save()
                    if cert:
                        product.certifications.add(cert)
                    created_products.append(product)

                    if not skip_images:
                        url = themed_image_url(catalog.name, seed=hash((producer.id, catalog.id)) % 10000)
                        img_bytes = fetch_image_bytes(url)
                        if img_bytes:
                            ProductImage.objects.create(
                                product=product,
                                image=ContentFile(img_bytes, name=f"{slugify(product.title)}-{product.id}.jpg"),
                                alt_text=product.title,
                            )

                    discount_pct = random.randint(5, 10)
                    qty_label = unit_quantity_label(unit)
                    bundle_title = f"{catalog.name} {qty_label}".strip()

                    base_price = price_orig
                    discounted = (base_price * (Decimal(100) - Decimal(discount_pct))) / Decimal(100)
                    discounted = discounted.quantize(Decimal("0.10"), rounding=ROUND_HALF_UP)

                    bundle = ProductBundle.objects.create(
                        title=bundle_title,
                        stock=stock,
                        discounted_percentage=discount_pct,
                        discounted_price=discounted,
                        original_price=base_price,
                        status="published" if publish_bundles else "draft",
                    )
                    ProductBundleItem.objects.create(bundle=bundle, product=product, quantity=1)
                    created_bundles.append(bundle)

                    total += 1

            if dry:
                raise transaction.TransactionManagementError("Dry-run: rolling back all changes.")

        try:
            create_all()
        except transaction.TransactionManagementError as e:
            if "Dry-run" in str(e):
                self.stdout.write(self.style.WARNING("Dry-run complete, no changes persisted."))
                self.stdout.write(self.style.WARNING(f"Planned products: {len(created_products)}, bundles: {len(created_bundles)}"))
                return
            raise

        self.stdout.write(self.style.SUCCESS(f"Created products: {len(created_products)}"))
        self.stdout.write(self.style.SUCCESS(f"Created bundles: {len(created_bundles)}"))
