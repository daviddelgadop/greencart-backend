import os
import psycopg2
from django.core.management.base import BaseCommand
from dotenv import load_dotenv

class Command(BaseCommand):
    help = "Test connection to Supabase database"

    def handle(self, *args, **options):
        load_dotenv()

        try:
            conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                sslmode="require"
            )
            cur = conn.cursor()
            cur.execute("SELECT current_database(), current_user, inet_server_addr(), NOW();")
            row = cur.fetchone()
            self.stdout.write(self.style.SUCCESS(f"✅ Connected: {row}"))

            cur.close()
            conn.close()
            self.stdout.write(self.style.SUCCESS("Connection closed."))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Connection failed: {e}"))
