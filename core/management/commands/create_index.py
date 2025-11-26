# core/management/commands/create_index.py
from django.core.management.base import BaseCommand
from django.db import connections
from django.conf import settings

GROUPS = {
    # Public producers feed
    "producers": [
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customuser_producer_active_created
        ON core_customuser (created_at DESC)
        WHERE (type = 'producer' AND is_active = true);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customuser_type_active
        ON core_customuser (type, is_active);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_company_owner_active_created
        ON core_company (owner_id, created_at DESC)
        WHERE (is_active = true);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_company_owner_active
        ON core_company (owner_id)
        WHERE (is_active = true);
        """,
    ],
    # Public bundles + favorites (reviews by bundle)
    "bundles": [
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bundle_active_published_stock
        ON core_productbundle (id DESC)
        WHERE (is_active = true AND status = 'published' AND stock > 0);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pbi_bundle_id__id
        ON core_productbundleitem (bundle_id, id);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orderitem_bundle_recent_rated
        ON core_orderitem (bundle_id, rated_at DESC)
        WHERE (is_active = true AND customer_rating IS NOT NULL);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prod_cert_product
        ON core_product_certifications (product_id, certification_id);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prod_cert_certification
        ON core_product_certifications (certification_id, product_id);
        """,
    ],
    # Orders (global and "my orders")
    "orders": [
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_user_active_created
        ON core_order (user_id, created_at DESC)
        WHERE (is_active = true);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_status_active_created
        ON core_order (status, created_at DESC)
        WHERE (is_active = true);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_active_created
        ON core_order (created_at DESC)
        WHERE (is_active = true);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orderitem_order_active
        ON core_orderitem (order_id, id)
        WHERE (is_active = true);
        """,
    ],
    # Product catalogs and products
    "catalogs": [
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_productcatalog_active_name
        ON core_productcatalog (name ASC)
        WHERE (is_active = true);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_productcatalog_category_id
        ON core_productcatalog (category_id);
        """,
    ],
    "products": [
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_product_company_id
        ON core_product (company_id);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_productimage_product_id
        ON core_productimage (product_id);
        """,
        # Only create these if your schema predates Django's automatic FK indexes.
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_city_id
        ON core_address (city_id);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_city_department_id
        ON core_city (department_id);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_department_region_id
        ON core_department (region_id);
        """,
    ],
    # Producer analytics (EXISTS with bundle->product->company)
    "analytics": [
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pbi_bundle_product
        ON core_productbundleitem (bundle_id, product_id);
        """,
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pbi_bundle_product_active
        ON core_productbundleitem (bundle_id, product_id)
        WHERE (is_active = true);
        """,
    ],
}

DROP_GROUPS = {
    "producers": [
        "DROP INDEX CONCURRENTLY IF EXISTS idx_company_owner_active;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_company_owner_active_created;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_customuser_type_active;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_customuser_producer_active_created;",
    ],
    "bundles": [
        "DROP INDEX CONCURRENTLY IF EXISTS idx_orderitem_bundle_recent_rated;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_pbi_bundle_id__id;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_bundle_active_published_stock;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_prod_cert_certification;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_prod_cert_product;",
    ],
    "orders": [
        "DROP INDEX CONCURRENTLY IF EXISTS idx_orderitem_order_active;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_order_active_created;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_order_status_active_created;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_order_user_active_created;",
    ],
    "catalogs": [
        "DROP INDEX CONCURRENTLY IF EXISTS idx_productcatalog_category_id;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_productcatalog_active_name;",
    ],
    "products": [
        "DROP INDEX CONCURRENTLY IF EXISTS idx_department_region_id;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_city_department_id;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_address_city_id;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_productimage_product_id;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_product_company_id;",
    ],
    "analytics": [
        "DROP INDEX CONCURRENTLY IF EXISTS idx_pbi_bundle_product_active;",
        "DROP INDEX CONCURRENTLY IF EXISTS idx_pbi_bundle_product;",
    ],
}

ALL_GROUP_NAMES = list(GROUPS.keys())

class Command(BaseCommand):
    help = (
        "Create or drop PostgreSQL indexes (CONCURRENTLY) related to API hotspots.\n"
        "Usage:\n"
        "  python manage.py create_index [--group producers bundles ...]\n"
        "  python manage.py create_index --drop [--group orders]\n"
        "  python manage.py create_index --dry-run\n"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            default="default",
            help="Database alias (default: default).",
        )
        parser.add_argument(
            "--group",
            nargs="*",
            choices=ALL_GROUP_NAMES,
            help=f"Limit to one or more groups: {', '.join(ALL_GROUP_NAMES)}. Default: all groups.",
        )
        parser.add_argument(
            "--drop",
            action="store_true",
            help="Drop indexes instead of creating them.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print SQL but do not execute.",
        )

    def handle(self, *args, **opts):
        alias = opts["database"]
        drop = opts["drop"]
        dry = opts["dry_run"]
        groups = opts["group"] or ALL_GROUP_NAMES

        conn = connections[alias]
        vendor = getattr(conn, "vendor", "")
        if vendor != "postgresql":
            self.stderr.write(self.style.ERROR("This command only supports PostgreSQL backends."))
            return

        # Build SQL list
        if drop:
            sql_list = []
            for g in groups:
                sql_list.extend(DROP_GROUPS[g])
        else:
            sql_list = []
            for g in groups:
                sql_list.extend([s.strip() for s in GROUPS[g]])

        # Show plan
        self.stdout.write(self.style.NOTICE(f"Database: {alias}"))
        self.stdout.write(self.style.NOTICE(f"Action: {'DROP' if drop else 'CREATE'}"))
        self.stdout.write(self.style.NOTICE(f"Groups: {', '.join(groups)}"))
        self.stdout.write(self.style.NOTICE(f"Statements: {len(sql_list)}"))
        if dry:
            self.stdout.write(self.style.WARNING("DRY RUN (no SQL executed)."))
            for s in sql_list:
                self.stdout.write(s.strip() + ("\n" if not s.strip().endswith(";") else ""))
            return

        # CREATE INDEX CONCURRENTLY cannot run inside a transaction
        # Ensure autocommit True for the duration
        prior_autocommit = conn.get_autocommit()
        try:
            if not prior_autocommit:
                conn.set_autocommit(True)

            with conn.cursor() as cur:
                for stmt in sql_list:
                    sql = stmt.strip()
                    if not sql:
                        continue
                    self.stdout.write(self.style.SQL_COLTYPE(sql))
                    try:
                        cur.execute(sql)
                        self.stdout.write(self.style.SUCCESS("OK"))
                    except Exception as e:
                        # Keep going; some IF NOT EXISTS might still throw under certain states
                        self.stderr.write(self.style.WARNING(f"WARN: {e}"))
        finally:
            # Restore original autocommit
            if conn.get_autocommit() != prior_autocommit:
                conn.set_autocommit(prior_autocommit)

        self.stdout.write(self.style.SUCCESS("Done."))
