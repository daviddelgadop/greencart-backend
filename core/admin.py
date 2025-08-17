from django.contrib import admin
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
    ProductBundle,
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
    SiteSetting
)
from django.contrib.auth.admin import UserAdmin

@admin.register(CustomUser)
class CustomUserAdmin(UserAdmin):
    model = CustomUser
    list_display = ('email', 'first_name', 'last_name', 'type', 'is_staff')
    list_filter = ('type', 'is_staff', 'is_active')
    fieldsets = (
        (None, {'fields': ('email', 'password')}),
        ('Informations personnelles', {'fields': ('first_name', 'last_name', 'phone', 'date_of_birth', 'type')}),
        ('Permissions', {'fields': ('is_staff', 'is_active', 'is_superuser', 'groups', 'user_permissions')}),
    )
    add_fieldsets = (
        (None, {
            'fields': ('email', 'password1', 'password2', 'type', 'first_name', 'last_name', 'date_of_birth')}
        ),
    )
    search_fields = ('email', 'first_name', 'last_name')
    ordering = ('email',)

@admin.register(ProductBundle)
class ProductBundleAdmin(admin.ModelAdmin):
    list_display = ('id', 'title', 'get_company_name', 'stock', 'discounted_price', 'is_active', 'status')
    readonly_fields = ('id',)

    def get_company_name(self, obj):
        first_item = obj.items.first()
        if first_item:
            return first_item.product.company.name
        return "-"
    get_company_name.short_description = 'Entreprise'

@admin.register(ProductBundleItem)
class ProductBundleItemAdmin(admin.ModelAdmin):
    list_display = ('id', 'bundle', 'product', 'quantity', 'is_active')

admin.register(Cart)
class CartAdmin(admin.ModelAdmin):
    list_display = ('id', 'user', 'session_key', 'is_active', 'created_at', 'updated_at', 'deactivated_at')
    list_filter = ('is_active', 'created_at', 'updated_at')
    search_fields = ('user__email', 'session_key')
    ordering = ('-created_at',)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.model.all_objects.all()  

@admin.register(CartItem)
class CartItemAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'cart',
        'bundle',
        'title_snapshot',
        'price_snapshot',
        'quantity',
        'is_active',
        'created_at',
        'updated_at',
        'deactivated_at'
    )
    list_filter = ('is_active', 'created_at', 'updated_at')
    search_fields = ('title_snapshot', 'bundle__title', 'cart__user__email')
    ordering = ('-created_at',)

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.model.all_objects.all()  
    

# Enregistrement des autres modèles
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
admin.site.register(Product)
admin.site.register(Order)
admin.site.register(OrderItem)
admin.site.register(Favorite)
admin.site.register(Reward)
admin.site.register(RewardTier)
admin.site.register(UserRewardProgress)
admin.site.register(Cart)

admin.site.register(BlogPost)
admin.site.register(BlogCategory)
admin.site.register(AboutSection)
admin.site.register(CoreValue)
admin.site.register(TeamMember)
admin.site.register(LegalInformation)
admin.site.register(ContactMessage)
admin.site.register(SiteSetting)

