from django.core.management.base import BaseCommand
from django.utils import timezone
from core.models import BlogCategory 

class Command(BaseCommand):
    help = "Réinitialise les catégories de blog par défaut"

    def handle(self, *args, **options):
        # Borra todas las categorías primero
        BlogCategory.objects.all().delete()
        self.stdout.write(self.style.WARNING("Toutes les catégories existantes ont été supprimées."))

        categories = [
            {
                "name": "Anti-gaspillage",
                "slug": "anti-gaspillage",
                "description": "Articles sur la réduction du gaspillage alimentaire",
                "color": "#16a34a",
                "icon": "Leaf",
                "order": 1,
            },
            {
                "name": "Recettes",
                "slug": "recettes",
                "description": "Idées et inspirations culinaires",
                "color": "#dc2626",
                "icon": "Utensils",
                "order": 2,
            },
            {
                "name": "Producteurs",
                "slug": "producteurs",
                "description": "Portraits et interviews de producteurs locaux",
                "color": "#2563eb",
                "icon": "Factory",
                "order": 3,
            },
            {
                "name": "Conseils",
                "slug": "conseils",
                "description": "Astuces pratiques et guides du quotidien",
                "color": "#d97706",
                "icon": "Lightbulb",
                "order": 4,
            },
            {
                "name": "Environnement",
                "slug": "environnement",
                "description": "Actualités et réflexions sur l’environnement",
                "color": "#0d9488",
                "icon": "Globe",
                "order": 5,
            },
        ]

        for cat in categories:
            obj = BlogCategory.objects.create(
                **cat,
                is_active=True,
                created_at=timezone.now(),
                updated_at=timezone.now(),
            )
            self.stdout.write(self.style.SUCCESS(f"Catégorie créée: {obj.name}"))

        self.stdout.write(self.style.SUCCESS("Toutes les catégories par défaut ont été recréées."))
