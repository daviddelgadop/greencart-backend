import csv
import sys
import os
from decimal import Decimal
from django.core.management.base import BaseCommand
from core.models import Region, Department, City


class Command(BaseCommand):
    help = "Importe les régions, départements et communes depuis les fichiers CSV dans /data."

    def handle(self, *args, **options):
        csv.field_size_limit(sys.maxsize)

        base_path = os.path.join('data')
        regions_file = os.path.join(base_path, 'regions-france.csv')
        departments_file = os.path.join(base_path, 'departements-france.csv')
        communes_file = os.path.join(base_path, 'communes-departement-region.csv')

        self.stdout.write("🧹 Suppression des données existantes...")
        City.objects.all().delete()
        Department.objects.all().delete()
        Region.objects.all().delete()

        self.stdout.write("📥 Import des régions...")
        regions = {}
        with open(regions_file, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            print(f"Clés du fichier regions: {reader.fieldnames}")
            for row in reader:
                region = Region.objects.create(code=row['code_region'], name=row['nom_region'])
                regions[row['code_region']] = region

        self.stdout.write("📥 Import des départements...")
        departments = {}
        with open(departments_file, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            print(f"Clés du fichier departements: {reader.fieldnames}")
            for row in reader:
                region = regions.get(row['code_region'])
                if region:
                    dept = Department.objects.create(
                        code=row['code_departement'],
                        name=row['nom_departement'],
                        region=region
                    )
                    departments[row['code_departement']] = dept

        self.stdout.write("📥 Import des communes...")
        count = 0
        with open(communes_file, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=',')
            print(f"Clés du fichier communes: {reader.fieldnames}")
            for row in reader:
                dept = departments.get(row['code_departement'])
                if not dept:
                    continue

                try:
                    latitude = Decimal(row['latitude']) if row['latitude'] else None
                    longitude = Decimal(row['longitude']) if row['longitude'] else None
                except:
                    latitude = None
                    longitude = None

                raw_cp = row.get('code_postal', '').strip()
                postal_code = raw_cp.zfill(5) if raw_cp.isdigit() else ''

                City.objects.update_or_create(
                    commune_code=row['code_commune_INSEE'],
                    defaults={
                        'postal_code': postal_code,
                        'name': row['nom_commune_postal'],
                        'department': dept,
                        'latitude': latitude,
                        'longitude': longitude,
                        'country_name': 'FRANCE'
                    }
                )
                count += 1

        self.stdout.write(self.style.SUCCESS(f"✅ {count} communes importées avec succès."))
