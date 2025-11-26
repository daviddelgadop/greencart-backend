# core/management/commands/seed_producers.py
import csv
import random
import unicodedata
from pathlib import Path
from collections import defaultdict
from datetime import date, timedelta
from io import BytesIO
from urllib.parse import quote_plus

import requests
from django.contrib.auth import get_user_model
from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import City, Address, Company, Certification

# ============================== Configuration ==============================

COMMON_PASSWORD = "Prod#2025!"
USE_RANDOMUSER_FOR_GENDER_PHOTOS = True
TIMEOUT = 25
random.seed(20250818)

SEED_TAG = "[SEED-PROD-2025]"

CREDENTIALS_TXT = Path("seed_producers_credentials.txt")
CREDENTIALS_CSV = Path("seed_producers_credentials.csv")

EMAIL_DOMAINS = ["gmail.com", "outlook.com", "hotmail.com", "yahoo.fr", "proton.me"]

# Postal-code fallbacks (adjust to match existing rows in your DB)
REPLACEMENTS = {
    "38000": "01000",
    "42000": "69001",
    "06000": "13001",
    "83000": "05000",
    "84000": "26100",
    "20000": "20200",
}

REGION_ALIASES = {
    "ARA": "Auvergne-Rhône-Alpes",
    "IDF": "Île-de-France",
    "BFC": "Bourgogne-Franche-Comté",
    "PACA": "Provence-Alpes-Côte d’Azur",
}

# ============================== Helpers ==============================

def fr_phone_digits() -> str:
    prefix = random.choice(["06", "07"])
    rest = "".join(random.choices("0123456789", k=8))
    return prefix + rest  # 10 digits, no spaces

def slugify(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-."
    s = s.replace("’", "").replace("'", "").replace(" ", "-").replace("/", "-")
    s = "".join(ch for ch in s if ch in allowed)
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-.")

def randomuser_avatar_url(gender: str, idx: int) -> str:
    img = idx % 100
    return f"https://randomuser.me/api/portraits/{'women' if gender=='female' else 'men'}/{img}.jpg"

def download_avatar_content(gender: str, idx: int):
    if not USE_RANDOMUSER_FOR_GENDER_PHOTOS:
        return None
    url = randomuser_avatar_url(gender, idx)
    resp = requests.get(url, timeout=TIMEOUT)
    resp.raise_for_status()
    return ContentFile(resp.content, name=f"avatar_{idx}.jpg")

def _http_get_image(url: str, timeout: int = TIMEOUT):
    headers = {"User-Agent": "SeedScript/1.0 (+https://example.com)"}
    resp = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
    resp.raise_for_status()
    ctype = resp.headers.get("Content-Type", "")
    if not ctype.startswith("image/"):
        raise ValueError(f"Unexpected content type: {ctype}")
    ext = ".jpg"
    if "png" in ctype:
        ext = ".png"
    elif "jpeg" in ctype or "jpg" in ctype:
        ext = ".jpg"
    elif "webp" in ctype:
        ext = ".webp"
    return resp.content, ext

def _generate_placeholder_png(text: str, size=(800, 600), bg=(32, 92, 56), fg=(255, 255, 255)) -> bytes:
    try:
        from PIL import Image, ImageDraw, ImageFont
        img = Image.new("RGB", size, bg)
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype("DejaVuSans.ttf", 40)
        except Exception:
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), text, font=font)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        x = max(20, (size[0] - w) // 2)
        y = max(20, (size[1] - h) // 2)
        draw.text((x, y), text, fill=fg, font=font)
        bio = BytesIO()
        img.save(bio, format="PNG")
        return bio.getvalue()
    except Exception:
        # 1×1 transparent PNG fallback
        return (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
            b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\x0bIDATx\x9cc``\x00\x00\x00\x02\x00\x01"
            b"\x0d\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
        )

def get_company_logo_content(company_name: str, idx: int) -> ContentFile:
    sources = [
        f"https://placehold.co/800x600/png?text={quote_plus(company_name)}",
        f"https://picsum.photos/seed/agri-{idx}/800/600",
    ]
    for s in sources:
        try:
            data, ext = _http_get_image(s)
            return ContentFile(data, name=f"company_{idx}{ext}")
        except Exception:
            continue
    png = _generate_placeholder_png(company_name)
    return ContentFile(png, name=f"company_{idx}.png")

def fake_pdf_bytes(title: str) -> bytes:
    body = f"Certification: {title}".encode("utf-8")
    return b"%PDF-1.4\n%Fake\n" + body + b"\n%%EOF"

# ============================== Seed pools ==============================

FEMALE_FIRST = [
    "Emma","Chloé","Camille","Léa","Manon","Inès","Jade","Zoé","Sarah","Eva",
    "Lucie","Anna","Lou","Léna","Julia","Alice","Romane","Maëlys","Elise","Noémie"
]
MALE_FIRST = [
    "Lucas","Hugo","Gabriel","Louis","Arthur","Nathan","Enzo","Paul","Jules","Tom",
    "Raphaël","Noah","Leo","Maxime","Théo","Ethan","Baptiste","Antoine","Sacha","Alexandre"
]
LAST_NAMES = [
    "Dubois","Durand","Lefevre","Moreau","Laurent","Simon","Michel","Leroy","Roux","David",
    "Bertrand","Morel","Fournier","Girard","Bonnet","Dupont","Lambert","Fontaine","Rousseau","Vincent",
    "Muller","Blanc","Guerin","Henry","Roussel","Nicolas","Perrin","Morin","Mathieu","Clement",
    "Gauthier","Dumont","Lopez","Garnier","Chevalier","Francois","Legrand","Gautier","Garcia","Fernandez"
]
STREETS = [
    "Rue de la République","Avenue Victor Hugo","Rue des Écoles","Rue Nationale","Boulevard Gambetta",
    "Rue du Commerce","Rue Pasteur","Rue Voltaire","Rue du Général de Gaulle","Rue de Paris",
    "Rue Jean Jaurès","Avenue de la Liberté","Rue de l'Église","Rue des Lilas","Rue des Fleurs",
    "Rue des Jardins","Rue du Stade","Rue de la Gare","Rue du Moulin","Rue de la Paix",
    "Rue des Cerisiers","Rue des Acacias","Rue des Peupliers","Chemin des Érables","Allée des Tilleuls",
    "Rue des Rosiers","Impasse des Sources","Rue des Vignes","Quai des Célestins","Rue des Artisans",
    "Rue des Bouleaux","Rue du Levant","Rue du Soleil","Rue du Pont","Rue des Champs",
    "Rue des Primevères","Rue des Amandiers","Rue des Alouettes","Rue des Glycines","Rue des Myosotis"
]

ARA = [
    {"region":"Auvergne-Rhône-Alpes","dept_code":"01","dept":"Ain","postal_code":"01000","city":"Bourg-en-Bresse"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"03","dept":"Allier","postal_code":"03000","city":"Moulins"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"07","dept":"Ardèche","postal_code":"07000","city":"Privas"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"15","dept":"Cantal","postal_code":"15000","city":"Aurillac"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"26","dept":"Drôme","postal_code":"26000","city":"Valence"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"43","dept":"Haute-Loire","postal_code":"43000","city":"Le Puy-en-Velay"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"74","dept":"Haute-Savoie","postal_code":"74000","city":"Annecy"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"38","dept":"Isère","postal_code":"38000","city":"Grenoble"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"42","dept":"Loire","postal_code":"42000","city":"Saint-Étienne"},
    {"region":"Auvergne-Rhône-Alpes","dept_code":"69","dept":"Rhône","postal_code":"69001","city":"Lyon"},
]
IDF = [
    {"region":"Île-de-France","dept_code":"75","dept":"Paris","postal_code":"75011","city":"Paris"},
    {"region":"Île-de-France","dept_code":"92","dept":"Hauts-de-Seine","postal_code":"92000","city":"Nanterre"},
    {"region":"Île-de-France","dept_code":"93","dept":"Seine-Saint-Denis","postal_code":"93000","city":"Bobigny"},
    {"region":"Île-de-France","dept_code":"94","dept":"Val-de-Marne","postal_code":"94000","city":"Créteil"},
    {"region":"Île-de-France","dept_code":"78","dept":"Yvelines","postal_code":"78000","city":"Versailles"},
    {"region":"Île-de-France","dept_code":"91","dept":"Essonne","postal_code":"91000","city":"Évry-Courcouronnes"},
    {"region":"Île-de-France","dept_code":"77","dept":"Seine-et-Marne","postal_code":"77000","city":"Melun"},
    {"region":"Île-de-France","dept_code":"95","dept":"Val-d’Oise","postal_code":"95000","city":"Cergy"},
    {"region":"Île-de-France","dept_code":"60","dept":"Oise","postal_code":"60000","city":"Beauvais"},
    {"region":"Île-de-France","dept_code":"28","dept":"Eure-et-Loir","postal_code":"28000","city":"Chartres"},
]
BFC = [
    {"region":"Bourgogne-Franche-Comté","dept_code":"21","dept":"Côte-d’Or","postal_code":"21000","city":"Dijon"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"25","dept":"Doubs","postal_code":"25000","city":"Besançon"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"70","dept":"Haute-Saône","postal_code":"70000","city":"Vesoul"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"39","dept":"Jura","postal_code":"39000","city":"Lons-le-Saunier"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"58","dept":"Nièvre","postal_code":"58000","city":"Nevers"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"71","dept":"Saône-et-Loire","postal_code":"71000","city":"Mâcon"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"90","dept":"Territoire de Belfort","postal_code":"90000","city":"Belfort"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"89","dept":"Yonne","postal_code":"89000","city":"Auxerre"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"10","dept":"Aube","postal_code":"10000","city":"Troyes"},
    {"region":"Bourgogne-Franche-Comté","dept_code":"03","dept":"Allier","postal_code":"03100","city":"Montluçon"},
]
PACA = [
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"04","dept":"Alpes-de-Haute-Provence","postal_code":"04000","city":"Digne-les-Bains"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"05","dept":"Hautes-Alpes","postal_code":"05000","city":"Gap"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"06","dept":"Alpes-Maritimes","postal_code":"06000","city":"Nice"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"13","dept":"Bouches-du-Rhône","postal_code":"13001","city":"Marseille"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"83","dept":"Var","postal_code":"83000","city":"Toulon"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"84","dept":"Vaucluse","postal_code":"84000","city":"Avignon"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"26","dept":"Drôme","postal_code":"26100","city":"Romans-sur-Isère"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"07","dept":"Ardèche","postal_code":"07100","city":"Annonay"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"2A","dept":"Corse-du-Sud","postal_code":"20000","city":"Ajaccio"},
    {"region":"Provence-Alpes-Côte d’Azur","dept_code":"2B","dept":"Haute-Corse","postal_code":"20200","city":"Bastia"},
]

TARGET_ROWS = ARA + IDF + BFC + PACA

COMPANY_PATTERNS = [
    "Maison {city}","Atelier {city}","Collectif {city}","Coopérative {city}","Le Comptoir {city}",
    "Maison du Quartier {city}","Atelier du Bourg {city}","Pavillon {city}","Le Cercle {city}","La Fabrique {city}",
    "Le Passage {city}","Le Quai {city}","La Cour {city}","Le Patio {city}","Le Belvédère {city}",
    "L’Escalier {city}","La Canopée {city}","L’Horizon {city}","Le Comptoir du Centre {city}","La Rencontre {city}",
    "Le 12 {city}","La Grange {city}","Le Forum {city}","Le Carré {city}","Le Clos {city}",
    "La Maison Commune {city}","Le Lieu-Dit {city}","La Traverse {city}","Le Parvis {city}","La Promenade {city}",
    "Le Passage du Bourg {city}","La Terrasse {city}","Le Grand Pré {city}","L’Atelier du Centre {city}",
    "La Parenthèse {city}","Le Moulin {city}","La Source {city}","Le Refuge {city}","La Clairière {city}"
]

# Gendered bio openers (no epicene forms)
BIO_OPENERS_M = ["Installé à", "Présent à", "Ancré à", "Basé à", "Actif à", "Engagé à", "Implanté à", "Situé à"]
BIO_OPENERS_F = ["Installée à", "Présente à", "Ancrée à", "Basée à", "Active à", "Engagée à", "Implantée à", "Située à"]

BIO_MIDDLES  = [
    "nous privilégions un fonctionnement lisible et humain",
    "nous cultivons des liens solides avec le quartier",
    "nous avançons avec une organisation claire et accessible",
    "nous accordons de l’importance à la régularité et à la parole donnée",
    "nous collaborons avec sérieux et simplicité",
    "nous améliorons nos façons de faire semaine après semaine",
    "nous donnons la priorité à la proximité et à la clarté",
    "nous maintenons un rythme adapté et respectueux",
    "nous favorisons la transparence et l’accueil",
    "nous veillons à la ponctualité et à la communication"
]
BIO_CLOSERS = [
    "N’hésitez pas à nous contacter.",
    "Vos retours sont les bienvenus.",
    "Échanges simples et réactifs.",
    "À votre écoute pour toute précision.",
    "Toujours disponibles sur rendez-vous.",
    "Au plaisir d’échanger.",
    "Contactez-nous pour en savoir plus.",
    "On vous répond rapidement.",
    "Un message suffit pour démarrer.",
    "Discutons de vos besoins."
]

DESC_OPENERS = [
    "Lieu de proximité à","Adresse conviviale à","Espace de rencontre à","Point d’appui local à","Maison ouverte à",
    "Cadre chaleureux à","Repère du quartier à","Collectif établi à","Organisation locale à","Pavillon de quartier à"
]
DESC_MIDDLES  = [
    "nous coordonnons sereinement les échanges","nous assurons des rendez-vous fiables","nous misons sur des relations suivies",
    "nous privilégions une logistique claire","nous travaillons à taille humaine","nous posons des repères simples et utiles",
    "nous avançons avec méthode et constance","nous construisons la confiance dans la durée","nous restons disponibles et attentifs","nous tenons nos engagements"
]
DESC_CLOSERS  = [
    "Passez nous voir ou écrivez-nous.","On prend le temps de répondre.","Un accueil simple et direct.",
    "Nous restons joignables facilement.","Au plaisir de vous rencontrer.","Un message, et on s’organise.",
    "On vous répond sans délai.","La porte est ouverte.","On trouve une solution ensemble.","À très bientôt sur place."
]

# ============================== Dataset builders ==============================

def unique_company_name(city: str, used: set, idx: int) -> str:
    base = COMPANY_PATTERNS[idx % len(COMPANY_PATTERNS)].format(city=city)
    name, tweak = base, 0
    while name in used:
        tweak += 1
        suffix = [" — Centre"," — Bourg"," — Halte"," — Nord",f" — {tweak}"][tweak % 5]
        name = base + suffix
    used.add(name)
    return name

def build_people_rows():
    rows = []
    male = MALE_FIRST[:]
    female = FEMALE_FIRST[:]
    random.shuffle(male); random.shuffle(female)
    last = LAST_NAMES[:]
    random.shuffle(last)

    genders = ["female"]*20 + ["male"]*20
    for i, r in enumerate(TARGET_ROWS):
        g = genders[i]
        fn = (female if g == "female" else male)[i % 20]
        ln = last[i]
        rows.append({**r, "gender": g, "first_name": fn, "last_name": ln, "public_display_name": f"{fn} {ln}"})
    return rows

def attach_company_and_text(rows):
    used_names = set()
    streets = STREETS[:]
    random.shuffle(streets)
    out = []
    for i, r in enumerate(rows):
        name = unique_company_name(r["city"], used_names, i)
        opener = random.choice(BIO_OPENERS_F if r["gender"] == "female" else BIO_OPENERS_M)
        user_bio_tpl  = (
            f"{opener} {r['city']} ({r['dept_code']} – {r['region']}), "
            f"{random.choice(BIO_MIDDLES)} {SEED_TAG} "
            f"Contact : {{email}} / {{phone}}."
        )
        company_desc_tpl = (
            f"{random.choice(DESC_OPENERS)} {r['city']}, "
            f"{random.choice(DESC_MIDDLES)} {SEED_TAG} "
            f"Contact : {{email}} / {{phone}}. {random.choice(DESC_CLOSERS)}"
        )
        out.append({
            **r,
            "company_name": name,
            "user_bio_template": user_bio_tpl,
            "company_desc_template": company_desc_tpl,
            "street_number": str(random.randint(2, 199)),
            "street_name": streets[i],
            "years_of_experience": random.randint(2, 18),
        })
    return out

def build_emails(rows):
    used = set()
    for r in rows:
        domain = random.choice(EMAIL_DOMAINS)
        base = slugify(r["company_name"]) or slugify(f"{r['first_name']}.{r['last_name']}.{r['city']}")
        email = f"{base}@{domain}"
        n = 2
        while email in used or not base:
            email = f"{base}{n}@{domain}"
            n += 1
        used.add(email)
        r["email"] = email
        r["phone"] = fr_phone_digits()
        r["password"] = COMMON_PASSWORD
        r["user_bio"] = r["user_bio_template"].replace("{email}", r["email"]).replace("{phone}", r["phone"])
        r["company_desc"] = r["company_desc_template"].replace("{email}", r["email"]).replace("{phone}", r["phone"])
    return rows

def dataset():
    rows = build_people_rows()
    rows = attach_company_and_text(rows)
    rows = build_emails(rows)
    return rows

# ============================== Certifications ==============================

def next_valid_until() -> date:
    return date.today() + timedelta(days=random.randint(365, 3*365))

def next_cert_number(code: str) -> str:
    return f"{code}-{random.randint(100000, 999999)}"

def create_company_certifications(company: Company, min_count: int = 3):
    choices = [c[0] for c in Certification.CERTIFICATION_CHOICES]
    random.shuffle(choices)
    count = max(min_count, 3)
    for code in choices[:count]:
        number = next_cert_number(code)
        content = ContentFile(
            fake_pdf_bytes(f"{company.name} / {code} / {number}"),
            name=f"{slugify(company.name)}_{code}.pdf"
        )
        Certification.objects.get_or_create(
            company=company,
            code=code,
            certification_number=number,
            defaults=dict(
                valid_until=next_valid_until(),
                file=content,
                verified=random.choice([True, False]),
            )
        )

# ============================== Purge ==============================

def purge_seed_records(UserModel):
    qs = UserModel.objects.filter(type="producer", description_utilisateur__icontains=SEED_TAG)
    count = qs.count()
    for u in qs:
        u.delete()
    return count

# ============================== Command ==============================

class Command(BaseCommand):
    help = "Seed 40 producteurs locaux (utilisateurs, adresses, entreprises, ≥3 certifications) avec purge sécurisée et logos. Écrit TXT/CSV des identifiants."

    def add_arguments(self, parser):
        parser.add_argument(
            "--regions",
            type=str,
            default="",
            help="Liste séparée par des virgules. Alias: ARA,IDF,BFC,PACA ou noms complets."
        )
        parser.add_argument(
            "--no-purge",
            action="store_true",
            help="Ne pas purger avant d’insérer."
        )
        parser.add_argument(
            "--purge-only",
            action="store_true",
            help="Ne rien créer, uniquement purger."
        )

    def handle(self, *args, **options):
        User = get_user_model()

        if options.get("purge_only"):
            deleted = purge_seed_records(User)
            self.stdout.write(self.style.SUCCESS(f"Purge: {deleted} comptes supprimés."))
            return

        if not options.get("no_purge"):
            deleted = purge_seed_records(User)
            self.stdout.write(self.style.WARNING(f"Purge initiale: {deleted} comptes supprimés."))

        rows = dataset()
        selected = set()
        if options.get("regions"):
            for token in [x.strip() for x in options["regions"].split(",") if x.strip()]:
                selected.add(REGION_ALIASES.get(token, token))
            rows = [r for r in rows if r["region"] in selected]

        created = []
        creds = []
        used_by_region = defaultdict(list)

        for idx, r in enumerate(rows):
            with transaction.atomic():
                # Resolve City with fallbacks
                target_cp = REPLACEMENTS.get(r["postal_code"], r["postal_code"])
                city = City.objects.filter(postal_code=target_cp).first()

                if not city and used_by_region[r["region"]]:
                    city = random.choice(used_by_region[r["region"]])
                    self.stdout.write(self.style.WARNING(
                        f"[FALLBACK] {r['postal_code']} {r['city']} -> reuse {city.postal_code} {getattr(city, 'name', '')} in {r['region']}"
                    ))

                if not city:
                    city = City.objects.filter(department__code=r["dept_code"]).order_by("id").first()
                    if city:
                        self.stdout.write(self.style.WARNING(
                            f"[FALLBACK] {r['postal_code']} {r['city']} -> {city.postal_code} {getattr(city, 'name', '')} (same department {r['dept_code']})"
                        ))

                if not city:
                    city = City.objects.filter(department__region__name=r["region"]).order_by("id").first()
                    if city:
                        self.stdout.write(self.style.WARNING(
                            f"[FALLBACK] {r['postal_code']} {r['city']} -> {city.postal_code} {getattr(city, 'name', '')} (same region {r['region']})"
                        ))

                if not city:
                    self.stdout.write(self.style.WARNING(
                        f"[SKIP] {r['postal_code']} {r['city']} not found and no fallback available."
                    ))
                    continue

                # User
                user, created_user = User.objects.get_or_create(
                    email=r["email"],
                    defaults=dict(
                        type="producer",
                        first_name=r["first_name"],
                        last_name=r["last_name"],
                        public_display_name=r["public_display_name"],
                        description_utilisateur=r["user_bio"],
                        years_of_experience=r["years_of_experience"],
                        phone=r["phone"],
                    ),
                )
                if created_user:
                    user.set_password(r["password"])
                    user.save()
                else:
                    user.first_name = r["first_name"]
                    user.last_name = r["last_name"]
                    user.public_display_name = r["public_display_name"]
                    user.description_utilisateur = r["user_bio"]
                    user.years_of_experience = r["years_of_experience"]
                    user.phone = r["phone"]
                    user.save()

                # Avatar
                if USE_RANDOMUSER_FOR_GENDER_PHOTOS and not getattr(user, "avatar", None):
                    try:
                        content = download_avatar_content(r["gender"], idx)
                        if content:
                            user.avatar.save(content.name, content, save=True)
                    except Exception as e:
                        self.stdout.write(self.style.WARNING(f"[AVATAR] {user.email}: {e}"))

                # Address
                addr = Address.objects.create(
                    user=user,
                    title="Siège",
                    street_number=r["street_number"],
                    street_name=r["street_name"],
                    city=city,
                    complement=SEED_TAG,
                    is_primary=True
                )
                if not user.main_address:
                    user.main_address = addr
                    user.save(update_fields=["main_address"])

                # Company
                company = Company.objects.create(
                    owner=user,
                    name=r["company_name"],
                    siret_number="".join(random.choices("0123456789", k=14)),
                    address=addr,
                    description=r["company_desc"]
                )

                # Company logo (robust multi-source + offline fallback)
                try:
                    if hasattr(company, "logo"):
                        logo_content = get_company_logo_content(company.name, idx)
                        company.logo.save(logo_content.name, logo_content, save=True)
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"[LOGO] {company.name}: {e}"))

                # Certifications (≥3)
                create_company_certifications(company, min_count=3)

                used_by_region[r["region"]].append(city)

                created.append((user.email, user.public_display_name, company.name, r["postal_code"], r["city"]))
                creds.append((user.email, r["password"], user.public_display_name, company.name, r["postal_code"], r["city"], r["phone"]))

        # Credentials output
        lines = [
            "=== Comptes producteurs ===",
            f"Mot de passe commun: {COMMON_PASSWORD}",
            ""
        ]
        for row in creds:
            lines.append(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} {row[5]} | {row[6]}")
        CREDENTIALS_TXT.write_text("\n".join(lines), encoding="utf-8")

        with CREDENTIALS_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["email","password","name","company","postal_code","city","phone"])
            w.writerows(creds)

        self.stdout.write(self.style.SUCCESS(f"Credentials: {CREDENTIALS_TXT.resolve()}"))
        self.stdout.write(self.style.SUCCESS(f"CSV:         {CREDENTIALS_CSV.resolve()}"))
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("== CREATED =="))
        for e, n, c, cp, ci in created:
            self.stdout.write(f"{e} | {n} | {c} | {cp} {ci}")
