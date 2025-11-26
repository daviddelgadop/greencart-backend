from django.apps import AppConfig

class GreencartConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "core"

    def ready(self):
        # Import signals so they are registered
        from . import signals  # noqa: F401
