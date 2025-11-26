from django.contrib import admin
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from django.conf import settings
from django.conf.urls.static import static

from .views import (
    AdminUsersView,
    AdminDeletionRequestedCustomersView,
    AdminUserUpdateView,
    AdminUserDeactivateView,
    AdminUserActivateView,
    AdminUserHardDeleteView,
    MeView,
    UserDetailViewSet,
    VerifyPasswordView,
    PostalCodesListAPIView,
    PostalInfoAPIView,
    AddressViewSet,
    CompanyViewSet,
    CertificationViewSet,
    ProductCatalogViewSet,
    ProductCategoryViewSet,
    ProductViewSet,
    ProductBundleViewSet,
    ProductBundleItemViewSet,
    PublicProductBundleListView,
    PublicProductBundleDetailView,
    PaymentMethodViewSet,
    UserSettingView,
    DownloadUserDataView,
    AccountDeletionRequestView,
    OrderViewSet,
    FavoriteViewSet,
    RewardTierViewSet,
    RewardViewSet,
    ProducerDashboardView,
    CartViewSet,
    PublicProducerDetailView,
    PublicProducerListView,
    ProducerOrdersView,
    BlogCategoryViewSet,
    AdminBlogPostViewSet,
    PublicBlogPostViewSet,
    PublicBlogPostListView,
    RecommendationsView,
    PublicBundlesView,

    AboutSectionViewSet,
    CoreValueViewSet,
    LegalInformationViewSet,
)

from .general_analytics_views import ProducerAnalyticsView, ProducerAIPreviewView

from .analytics_endpoints import (
    SalesTimeseriesView,
    OrdersDeepView,
    CustomersDeepView,
    CartsAbandonedDeepView,
    CatalogDeepView,
    ProductsHealthView,
    ImpactView,
    SalesByCategoryDeepView,
    PaymentsDeepView,
    CohortsMonthlyView,
    GeoDeepView,
    EvaluationsDeepView,
    ReviewsKeywordsView,
    SalesVsRatingsView,
)

from .analytics_cross import (
    ImpactVsRevenueView,
    DiscountVsConversionView,
    ExpiryVsVelocityView,
    PaymentsAovRatingsGeoView,
    GeoRevenueRatingImpactView,
    CategorySavingsImpactView,
    CertificationsPerformanceView,
    EcoScorePerformanceView,
    FavoritesToPurchaseView,
    CohortsImpactView,
    RfmRatingsView,
    ProducerShareInOrdersView,
    DiscountVsRatingView,
    InventoryEfficiencyView,
)

from .auth_views import (
    CustomTokenObtainPairView,
    RegisterUserView,
    VerifyEmailView,
    ResendVerificationEmailView,
    PasswordResetRequestView,
    PasswordResetConfirmView,
    ChangePasswordView,
)


router = DefaultRouter()

router.register(r'users', UserDetailViewSet, basename='user')
router.register(r'addresses', AddressViewSet, basename='address')
router.register(r'companies', CompanyViewSet, basename='company')
router.register(r'certifications', CertificationViewSet, basename='certification')
router.register(r'product-catalogs', ProductCatalogViewSet, basename='product-catalog')
router.register(r'product-categories', ProductCategoryViewSet, basename='product-category')
router.register(r'products', ProductViewSet, basename='product')
router.register(r'product-bundles', ProductBundleViewSet, basename='product-bundle')
router.register(r'product-bundle-items', ProductBundleItemViewSet, basename='product-bundle-item')
router.register(r'payment-methods', PaymentMethodViewSet, basename='payment')
router.register(r'orders', OrderViewSet)
router.register(r'reward-tiers', RewardTierViewSet, basename='reward-tier')
router.register(r'rewards', RewardViewSet, basename='reward')
router.register(r'cart', CartViewSet, basename='cart')
router.register(r'favorites', FavoriteViewSet, basename='favorite')
router.register(r'about', AboutSectionViewSet)
router.register(r'core-values', CoreValueViewSet)
router.register(r'legal', LegalInformationViewSet)

# Blog
router.register(r'blog/categories', BlogCategoryViewSet, basename='blog-category')
router.register(r'blog/admin/posts', AdminBlogPostViewSet, basename='blog-admin')
router.register(r'blog/posts', PublicBlogPostViewSet, basename='blog-public')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/', include(router.urls)),

    path("api/recommendations/", RecommendationsView.as_view(), name="recommendations"),


    # Auth
    path('api/auth/verify-password/', VerifyPasswordView.as_view(), name='verify-password'),
    path('api/me/', MeView.as_view(), name='me'),

    path('api/admin/users/', AdminUsersView.as_view(), name='admin-users'),
    path('api/admin/users/deletion-requests/', AdminDeletionRequestedCustomersView.as_view(), name='admin-users-deletion-requests'),
    path('api/admin/users/<int:pk>/', AdminUserUpdateView.as_view(), name='admin-users-update'),
    path('api/admin/users/<int:pk>/deactivate/', AdminUserDeactivateView.as_view(), name='admin-users-deactivate'),
    path('api/admin/users/<int:pk>/activate/', AdminUserActivateView.as_view(), name='admin-users-activate'),
    path("api/admin/users/<int:pk>/hard-delete/", AdminUserHardDeleteView.as_view(), name="admin-user-hard-delete"),

    path('api/auth/register/', RegisterUserView.as_view(), name='register'),
    path('api/auth/token/', CustomTokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/auth/verify-email/', VerifyEmailView.as_view(), name='verify-email'),
    path('api/auth/resend-verification/', ResendVerificationEmailView.as_view(), name='resend-verification'),
    path('api/auth/password-reset/', PasswordResetRequestView.as_view(), name='password-reset'),
    path('api/auth/password-reset/confirm/', PasswordResetConfirmView.as_view(), name='password-reset-confirm'),
    path('api/auth/change-password/', ChangePasswordView.as_view(), name='change-password'),

    path('api/postal-codes/', PostalCodesListAPIView.as_view()),
    path('api/postal-codes/<str:code_postal>/', PostalInfoAPIView.as_view()),
    path('api/public-bundles/<int:id>/', PublicProductBundleDetailView.as_view()),
    path('api/public-bundles/', PublicProductBundleListView.as_view()),
    #path("api/public-bundles/", PublicBundlesView.as_view(), name="public-bundles"),
    path('api/download-user-data/', DownloadUserDataView.as_view(), name='download-user-data'),
    path('account-deletion-request/', AccountDeletionRequestView.as_view(), name='account-deletion-request'),
    path('api/user-settings/', UserSettingView.as_view(), name='user-settings'),

    # Dashboards
    path('api/producer/dashboard/', ProducerDashboardView.as_view(), name='producer-dashboard'),
    path('api/producer/analytics/', ProducerAnalyticsView.as_view(), name='producer-analytics'),
    path('api/producer/ai-preview/', ProducerAIPreviewView.as_view(), name='producer-ai-preview'),
    path("api/producer/orders/", ProducerOrdersView.as_view(), name="producer-orders"),

    # Public producers
    path("api/public/producers/<int:pk>/", PublicProducerDetailView.as_view(), name="public-producer-detail"),
    path("api/public/producers/", PublicProducerListView.as_view(), name="public-producer-list"),

    # Analytics endpoints
    path("api/producer/analytics/sales/timeseries/", SalesTimeseriesView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/sales/timeseries/", SalesTimeseriesView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/orders/deep/", OrdersDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/orders/deep/", OrdersDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/customers/deep/", CustomersDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/customers/deep/", CustomersDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/carts/abandoned/deep/", CartsAbandonedDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/carts/abandoned/deep/", CartsAbandonedDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/catalog/deep/", CatalogDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/catalog/deep/", CatalogDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/products/health/", ProductsHealthView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/products/health/", ProductsHealthView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/impact/", ImpactView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/impact/", ImpactView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/sales/by-category/deep/", SalesByCategoryDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/sales/by-category/deep/", SalesByCategoryDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/payments/deep/", PaymentsDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/payments/deep/", PaymentsDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cohorts/monthly/", CohortsMonthlyView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cohorts/monthly/", CohortsMonthlyView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/geo/deep/", GeoDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/geo/deep/", GeoDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/evaluations/deep/", EvaluationsDeepView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/evaluations/deep/", EvaluationsDeepView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/reviews/keywords/", ReviewsKeywordsView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/reviews/keywords/", ReviewsKeywordsView.as_view(), {"scope": "admin"}),

    # Cross analytics
    path("api/producer/analytics/cross/sales-vs-ratings/", SalesVsRatingsView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/sales-vs-ratings/", SalesVsRatingsView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/impact-vs-revenue/", ImpactVsRevenueView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/impact-vs-revenue/", ImpactVsRevenueView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/discount-vs-conversion/", DiscountVsConversionView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/discount-vs-conversion/", DiscountVsConversionView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/expiry-vs-velocity/", ExpiryVsVelocityView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/expiry-vs-velocity/", ExpiryVsVelocityView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/payments-aov-ratings-geo/", PaymentsAovRatingsGeoView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/payments-aov-ratings-geo/", PaymentsAovRatingsGeoView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/geo-revenue-rating-impact/", GeoRevenueRatingImpactView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/geo-revenue-rating-impact/", GeoRevenueRatingImpactView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/category-savings-impact/", CategorySavingsImpactView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/category-savings-impact/", CategorySavingsImpactView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/certifications-performance/", CertificationsPerformanceView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/certifications-performance/", CertificationsPerformanceView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/ecoscore-performance/", EcoScorePerformanceView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/ecoscore-performance/", EcoScorePerformanceView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/favorites-to-purchase/", FavoritesToPurchaseView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/favorites-to-purchase/", FavoritesToPurchaseView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/cohorts-impact/", CohortsImpactView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/cohorts-impact/", CohortsImpactView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/rfm-ratings/", RfmRatingsView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/rfm-ratings/", RfmRatingsView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/producer-share-in-orders/", ProducerShareInOrdersView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/producer-share-in-orders/", ProducerShareInOrdersView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/discount-vs-rating/", DiscountVsRatingView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/discount-vs-rating/", DiscountVsRatingView.as_view(), {"scope": "admin"}),

    path("api/producer/analytics/cross/inventory-efficiency/", InventoryEfficiencyView.as_view(), {"scope": "producer"}),
    path("api/admin/analytics/cross/inventory-efficiency/", InventoryEfficiencyView.as_view(), {"scope": "admin"}),

    # Blog public list
    path("api/blog/posts/", PublicBlogPostListView.as_view(), name="blog-post-list"),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
