import os
from decouple import config, Csv  # Csv permet de lire des listes depuis .env
from pathlib import Path
from corsheaders.defaults import default_headers
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

OPEIA_API_KEY = os.getenv("OPEIA_API_KEY")  # do not hardcode
OPEIA_API_BASE = os.getenv("OPEIA_API_BASE", "https://api.openai.com")  
OPEIA_MODEL = os.getenv("OPEIA_MODEL", "gpt-4o-mini")


# Detection des variables d'environnement mal encodees
with open(".env", "rb") as f:
    content = f.read()
    try:
        content.decode("utf-8")
        print(".env est bien encodé en UTF-8.")
    except UnicodeDecodeError as e:
        print("Erreur d'encodage dans .env :", e)

# === Repertoires de base ===
BASE_DIR = Path(__file__).resolve().parent.parent
# BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# === Cle secrete et mode debug (via .env) ===
SECRET_KEY = config('SECRET_KEY') 
DEBUG = config('DEBUG', default=False, cast=bool)

# === Domaines autorises (separes par des virgules dans .env)
ALLOWED_HOSTS = config('ALLOWED_HOSTS', default='127.0.0.1,localhost', cast=Csv())

# === Applications installees ===
INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",       # API REST
    "core",                 # App principale
    "corsheaders",          # Gestion du CORS
]

# === Middleware ===
MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

CORS_ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

# === URLs principales ===
ROOT_URLCONF = "core.urls"

# === Templates Django ===
TEMPLATES = [{
    "BACKEND": "django.template.backends.django.DjangoTemplates",
    "DIRS": [],
    "APP_DIRS": True,
    "OPTIONS": {
        "context_processors": [
            "django.template.context_processors.debug",
            "django.template.context_processors.request",
            "django.contrib.auth.context_processors.auth",
            "django.contrib.messages.context_processors.messages",
        ],
    },
}]

# === Application WSGI ===
WSGI_APPLICATION = "core.wsgi.application"

# === Base de donnees PostgreSQL ===
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": config("POSTGRES_DB"),
        "USER": config("POSTGRES_USER"),
        "PASSWORD": config("POSTGRES_PASSWORD"),
        "HOST": config("POSTGRES_HOST"),
        "PORT": config("POSTGRES_PORT", default="5432"),
        "OPTIONS": {
            "client_encoding": "UTF8",  
        }
    }
}

# === Modele utilisateur personnalise ===
AUTH_USER_MODEL = "core.CustomUser"

# === REST Framework ===
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
    ),
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
        'rest_framework.parsers.FormParser',
        'rest_framework.parsers.MultiPartParser',
    ],
}

# === Internationalisation ===
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# === Fichiers statiques et medias ===
STATIC_URL = "/static/"
MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

# === Champ auto par defaut ===
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# === CORS autorise ===
CORS_ALLOW_ALL_ORIGINS = config("CORS_ALLOW_ALL_ORIGINS", default=True, cast=bool)

# === Configuration PayPal ===
PAYPAL_CLIENT_ID = config("PAYPAL_CLIENT_ID")
PAYPAL_SECRET = config("PAYPAL_SECRET")
PAYPAL_ENV = config("PAYPAL_ENV", default="sandbox")

from datetime import timedelta

SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': timedelta(days=7),  # durée du token d'accès
    'REFRESH_TOKEN_LIFETIME': timedelta(days=30),     # durée du token de rafraîchissement
    'ROTATE_REFRESH_TOKENS': False,
    'BLACKLIST_AFTER_ROTATION': False,
    'ALGORITHM': 'HS256',
    'AUTH_HEADER_TYPES': ('Bearer',),
}

CORS_ALLOW_HEADERS = list(default_headers) + [
    "x-session-key", 
]

# (optionel) cookies/session cross-site
# CORS_ALLOW_CREDENTIALS = True