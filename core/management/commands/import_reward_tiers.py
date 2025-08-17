from decimal import Decimal
from django.core.management.base import BaseCommand
from core.models import RewardTier

WASTE = [
    ('eco-debutant','Éco-Débutant','Plus de 1 kg de gaspillage évité','1'),
    ('eco-heros','Éco-Héros','Plus de 10 kg de gaspillage évité','10'),
    ('sauveur-gourmet','Sauveur Gourmet','Plus de 25 kg de gaspillage évité','25'),
    ('gardien-du-gout','Gardien du Goût','Plus de 50 kg de gaspillage évité','50'),
    ('zero-gachis','Zéro Gâchis','Plus de 100 kg de gaspillage évité','100'),
]
PROD = [
    ('decouverte','Découverte','Soutenir 1 producteur différent',1),
    ('ami-des-fermes','Ami des Fermes','Soutenir 5 producteurs différents',5),
    ('voisin-solidaire','Voisin Solidaire','Soutenir 10 producteurs différents',10),
    ('champion-local','Champion Local','Soutenir 20 producteurs différents',20),
    ('ambassadeur-terroir','Ambassadeur Terroir','Soutenir 35 producteurs différents',35),
]

class Command(BaseCommand):
    help = 'Seed reward tiers'

    def handle(self, *args, **options):
        for code, title, desc, kg in WASTE:
            RewardTier.objects.update_or_create(
                code=code,
                defaults=dict(
                    title=title, description=desc,
                    min_waste_kg=Decimal(kg),
                    min_producers_supported=0, min_orders=0,
                    min_co2_kg=Decimal('0'), min_savings_eur=Decimal('0'),
                    benefit_kind='none', benefit_config={}, is_active=True
                )
            )
        for code, title, desc, prod in PROD:
            RewardTier.objects.update_or_create(
                code=code,
                defaults=dict(
                    title=title, description=desc,
                    min_producers_supported=prod,
                    min_waste_kg=Decimal('0'), min_orders=0,
                    min_co2_kg=Decimal('0'), min_savings_eur=Decimal('0'),
                    benefit_kind='none', benefit_config={}, is_active=True
                )
            )
        self.stdout.write(self.style.SUCCESS('Done'))
