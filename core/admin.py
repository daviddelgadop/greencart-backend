from django.contrib import admin
from django.utils.html import format_html
from django.contrib.auth.admin import UserAdmin

from .models import (
    CustomUser,
    Department,
    Region,
    City,
    Address,
    Company,
    Certification,
    ProductImpact,
    ProductCategory,
    ProductCatalog,
    Product,
    ProductImage,
    ProductBundle,
    ProductBundleImage,
    ProductBundleItem,
    Order,
    OrderItem,
    Favorite,
    RewardBenefit,
    RewardTier,
    RewardStatus,
    Reward,
    UserRewardProgress,
    PaymentMethod,
    UserSetting,
    Cart,
    CartItem,
    BlogPost,
    BlogCategory,
    AboutSection,
    CoreValue,
    TeamMember,
    LegalInformation,
    ContactMessage,
    SiteSetting,
)


@admin.register(CustomUser)
class CustomUserAdmin(UserAdmin):
    model = CustomUser
    list_display = ("email", "first_name", "last_name", "type", "is_staff")
    list_filter = ("type", "is_staff", "is_active")
    fieldsets = (
        (None, {"fields": ("email", "password")}),
        (
            "Informations personnelles",
            {
                "fields": (
                    "first_name",
                    "last_name",
                    "phone",
                    "date_of_birth",
                    "type",
                    "public_display_name",
                    "description_utilisateur",
                    "years_of_experience",
                    "avatar",
                    "avatar_preview",
                )
            },
        ),
        (
            "Permissions",
            {"fields": ("is_staff", "is_active", "is_superuser", "groups", "user_permissions")},
        ),
    )
    add_fieldsets = (
        (
            None,
            {
                "fields": (
                    "email",
                    "password1",
                    "password2",
                    "type",
                    "first_name",
                    "last_name",
                    "date_of_birth",
                )
            },
        ),
    )
    readonly_fields = ("avatar_preview",)
    search_fields = ("email", "first_name", "last_name")
    ordering = ("email",)

    def avatar_preview(self, obj):
        if obj and obj.avatar:
            return format_html('<img src="{}" style="height:80px;border-radius:8px;" />', obj.avatar.url)
        return "-"

    avatar_preview.short_description = "Avatar preview"


class ProductImageInline(admin.TabularInline):
    model = ProductImage
    extra = 1
    fields = ("image", "alt_text", "image_preview")
    readonly_fields = ("image_preview",)

    def image_preview(self, obj):
        if obj and getattr(obj, "image", None):
            return format_html('<img src="{}" style="height:60px;border-radius:6px;" />', obj.image.url)
        return "-"

    image_preview.short_description = "Preview"


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("id", "title", "company", "original_price", "stock", "is_active")
    list_filter = ("company", "is_active", "unit", "eco_score")
    search_fields = ("title", "variety", "company__name")
    inlines = [ProductImageInline]


class ProductBundleImageInline(admin.TabularInline):
    model = ProductBundleImage
    extra = 1
    fields = ("image", "alt_text", "image_preview")
    readonly_fields = ("image_preview",)

    def image_preview(self, obj):
        if obj and getattr(obj, "image", None):
            return format_html('<img src="{}" style="height:60px;border-radius:6px;" />', obj.image.url)
        return "-"

    image_preview.short_description = "Preview"


@admin.register(ProductBundle)
class ProductBundleAdmin(admin.ModelAdmin):
    list_display = ("id", "title", "get_company_name", "stock", "discounted_price", "is_active", "status")
    readonly_fields = ("id",)
    search_fields = ("title", "items__product__company__name")
    list_filter = ("status", "is_active")
    inlines = [ProductBundleImageInline]
    actions = ["make_draft", "make_published", "make_archived", "make_out_of_stock"]

    def get_company_name(self, obj):
        first_item = obj.items.first()
        if first_item:
            return first_item.product.company.name
        return "-"

    get_company_name.short_description = "Entreprise"

    def make_draft(self, request, queryset):
        updated = queryset.update(status="draft")
        self.message_user(request, f"{updated} bundle(s) set to Draft.")

    make_draft.short_description = "Mark selected bundles as Draft"

    def make_published(self, request, queryset):
        updated = queryset.update(status="published")
        self.message_user(request, f"{updated} bundle(s) set to Published.")

    make_published.short_description = "Mark selected bundles as Published"

    def make_archived(self, request, queryset):
        updated = queryset.update(status="archived")
        self.message_user(request, f"{updated} bundle(s) set to Archived.")

    make_archived.short_description = "Mark selected bundles as Archived"

    def make_out_of_stock(self, request, queryset):
        updated = queryset.update(status="out_of_stock")
        self.message_user(request, f"{updated} bundle(s) set to Out of Stock.")

    make_out_of_stock.short_description = "Mark selected bundles as Out of Stock"


@admin.register(ProductBundleItem)
class ProductBundleItemAdmin(admin.ModelAdmin):
    list_display = ("id", "bundle", "product", "quantity", "is_active")
    list_filter = ("is_active", "bundle")
    search_fields = ("bundle__title", "product__title")


@admin.register(Cart)
class CartAdmin(admin.ModelAdmin):
    list_display = ("id", "user", "session_key", "is_active", "created_at", "updated_at", "deactivated_at")
    list_filter = ("is_active", "created_at", "updated_at")
    search_fields = ("user__email", "session_key")
    ordering = ("-created_at",)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.model.all_objects.all()


@admin.register(CartItem)
class CartItemAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "cart",
        "bundle",
        "title_snapshot",
        "price_snapshot",
        "quantity",
        "is_active",
        "created_at",
        "updated_at",
        "deactivated_at",
    )
    list_filter = ("is_active", "created_at", "updated_at")
    search_fields = ("title_snapshot", "bundle__title", "cart__user__email")
    ordering = ("-created_at",)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.model.all_objects.all()


admin.site.register(Department)
admin.site.register(Region)
admin.site.register(City)
admin.site.register(Address)
admin.site.register(PaymentMethod)
admin.site.register(Company)
admin.site.register(Certification)
admin.site.register(ProductCategory)
admin.site.register(ProductCatalog)
admin.site.register(ProductImpact)
admin.site.register(Order)
admin.site.register(OrderItem)
admin.site.register(Favorite)
admin.site.register(Reward)
admin.site.register(RewardTier)
admin.site.register(UserRewardProgress)
admin.site.register(BlogPost)
admin.site.register(BlogCategory)
admin.site.register(AboutSection)
admin.site.register(CoreValue)
admin.site.register(TeamMember)
admin.site.register(LegalInformation)
admin.site.register(ContactMessage)
admin.site.register(SiteSetting)
