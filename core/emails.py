import os
import requests
from django.conf import settings
from django.core.mail import send_mail
from urllib.parse import urlencode, urlparse


def _api_base():
    region = str(getattr(settings, "MAILGUN_REGION", "EU")).upper()
    return "https://api.eu.mailgun.net/v3" if region == "EU" else "https://api.mailgun.net/v3"


def _mailgun_auth():
    api_key = getattr(settings, "MAILGUN_API_KEY", "")
    if not api_key:
        raise RuntimeError("MAILGUN_API_KEY is not configured.")
    return ("api", api_key)


def _normalize_domain(value: str) -> str:
    if not value:
        raise RuntimeError("MAILGUN_DOMAIN is not configured.")
    v = value if value.startswith("http") else f"https://{value}"
    parsed = urlparse(v)
    host = parsed.hostname
    if not host:
        raise RuntimeError("MAILGUN_DOMAIN is invalid.")
    return host


def _mailgun_domain():
    raw = getattr(settings, "MAILGUN_DOMAIN", "")
    return _normalize_domain(raw)


def _from_header():
    if getattr(settings, "DEFAULT_FROM_EMAIL", None):
        return settings.DEFAULT_FROM_EMAIL

    domain = str(getattr(settings, "MAILGUN_DOMAIN", "")).strip()
    if domain:
        try:
            host = _normalize_domain(domain)
            local = "postmaster" if host.startswith("sandbox") else "no-reply"
            name = getattr(settings, "MAILGUN_FROM_NAME", "Greencart")
            return f"{name} <{local}@{host}>"
        except Exception:
            pass

    user = getattr(settings, "EMAIL_HOST_USER", None)
    if user:
        return f"Greencart <{user}>"

    return "Greencart <no-reply@example.com>"


def _ensure_list(recipient):
    if isinstance(recipient, (list, tuple, set)):
        return list(recipient)
    return [recipient]


def _provider():
    prov = os.getenv("EMAIL_PROVIDER", "").strip().lower()
    if prov:
        return prov
    backend = str(getattr(settings, "EMAIL_BACKEND", "")).lower()
    if "smtp" in backend:
        return "smtp"
    if getattr(settings, "MAILGUN_DOMAIN", None) and getattr(settings, "MAILGUN_API_KEY", None):
        return "mailgun_api"
    return "smtp"


def send_app_email(to, subject, text, html=None):
    provider = _provider()

    if provider in ("smtp", "gmail", "mailgun_smtp"):
        try:
            send_mail(
                subject=subject,
                message=text or "",
                from_email=_from_header(),
                recipient_list=_ensure_list(to),
                html_message=html,
                fail_silently=False,
            )
            return True, {"message": f"Sent via SMTP ({provider})"}
        except Exception as e:
            return False, str(e)

    if provider in ("mailgun_api", "mailgun"):
        try:
            domain = _mailgun_domain()
            auth = _mailgun_auth()
            api_base = _api_base()
            data = {
                "from": _from_header(),
                "to": _ensure_list(to),
                "subject": subject,
                "text": text or "",
            }
            if html:
                data["html"] = html
            resp = requests.post(f"{api_base}/{domain}/messages", auth=auth, data=data, timeout=10)
            if 200 <= resp.status_code < 300:
                return True, resp.json()
            return False, {"status_code": resp.status_code, "text": resp.text}
        except Exception as e:
            return False, str(e)

    try:
        send_mail(
            subject=subject,
            message=text or "",
            from_email=_from_header(),
            recipient_list=_ensure_list(to),
            html_message=html,
            fail_silently=False,
        )
        return True, {"message": "Sent via SMTP (fallback)"}
    except Exception as e:
        return False, str(e)


def send_mailgun_email(to, subject, text, html=None):
    return send_app_email(to, subject, text, html)


def build_frontend_url(path, params=None):
    base = getattr(settings, "FRONTEND_URL", "http://localhost:5173")
    if not path.startswith("/"):
        path = "/" + path
    if params:
        return f"{base}{path}?{urlencode(params)}"
    return f"{base}{path}"
