import json
import requests
import os
from django.db import transaction
from django.db.models import F, Sum, Count, Exists, OuterRef, Q, Prefetch, Value, FloatField, IntegerField
from django.db.models.functions import Coalesce
from datetime import datetime, date
from django.db.models import ImageField, Prefetch
from rest_framework.pagination import LimitOffsetPagination

from decimal import Decimal, ROUND_HALF_UP
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db.models.deletion import ProtectedError
from django.http import FileResponse
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.utils.timezone import now
from rest_framework import viewsets, status, filters, generics, permissions
from rest_framework.decorators import action
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page


from rest_framework.generics import ListAPIView, RetrieveAPIView
from rest_framework.permissions import IsAuthenticated, AllowAny, IsAdminUser
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.exceptions import PermissionDenied
from core.mixins.responses import StandardResponseMixin
from .utils import (
    export_user_data, 
    get_or_create_cart, 
    recompute_after_bundle,
    hard_delete_user_and_related)
from dateutil.relativedelta import relativedelta



import logging, traceback
logger = logging.getLogger(__name__)

from .models import (
    City,
    Address, 
    Company,
    Certification,
    ProductCategory, 
    ProductCatalog,
    Product,
    ProductImpact,
    ProductImage,
    ProductBundleImage,
    ProductBundle,
    ProductBundleItem, 
    Order, 
    OrderItem,
    PaymentMethod, 
    UserSetting, 
    Favorite, 
    Reward,
    RewardTier,
    UserRewardProgress, 
    CartItem,
    Cart,
    CustomUser, 
    BlogCategory,
    BlogPost,

    AboutSection, 
    CoreValue, 
    LegalInformation,
    SiteSetting
)

from .serializers import (
    UserSerializer,
    CustomUserSerializer, 
    AddressSerializer, 
    CompanySerializer,
    CertificationSerializer,
    ProductImpactSerializer,
    ProductCategorySerializer,
    ProductCatalogSerializer,
    ProductSerializer, 
    ProductBundleSerializer,
    ProductBundleItemSerializer,
    OrderSerializer,
    OrderItemSerializer,
    PaymentMethodSerializer, 
    UserSettingSerializer,  
    FavoriteSerializer,
    RewardTierSerializer, 
    RewardSerializer, 
    CartItemSerializer,
    CartSerializer,
    RateOnlySerializer,
    BlogCategorySerializer,
    BlogPostReadSerializer,
    BlogPostWriteSerializer,
    BlogPostPublicSerializer,
    ProductBundleSummarySerializer,
    OrderListSerializer,
    PublicBundleListSerializer,

    AboutSectionSerializer, 
    CoreValueSerializer, 
    LegalInformationSerializer,
    SiteSettingSerializer
)


from typing import List, Set, Dict
from core.recommendations import rank_copurchased_candidates


def cached_first(iterable_or_manager):
    """Return the first element from a prefetched relation without hitting the DB."""
    try:
        it = iter(iterable_or_manager) 
    except TypeError:
        it = iter(iterable_or_manager.all()) 
    return next(it, None)



def calculate_bundle_impact(bundle):
    total_waste = Decimal("0.0")
    total_co2 = Decimal("0.0")

    for item in bundle.items.all():
        catalog_entry = item.product.catalog_entry
        unit = item.product.unit
        quantity = Decimal(item.quantity) * bundle.stock

        impact_entry = cached_first(ProductImpact.objects.filter(
            product=catalog_entry,
            unit=unit,
            quantity=Decimal("1.0")
        ))

        if impact_entry:
            total_waste += quantity * impact_entry.avoided_waste_kg
            total_co2 += quantity * impact_entry.avoided_co2_kg

    bundle.total_avoided_waste_kg = total_waste
    bundle.total_avoided_co2_kg = total_co2
    bundle.save()


def _collect_producer_ids_from_order(order):
    ids = set()
    for oi in order.items.all():
        for p in (oi.bundle_snapshot or {}).get("products", []):
            cid = p.get("company_id")
            if cid:
                ids.add(int(cid))
    return ids

def update_rewards_for_order(order):
    prog, _ = UserRewardProgress.objects.get_or_create(user=order.user)

    # Sum total
    prog.total_orders += 1
    prog.total_waste_kg = (prog.total_waste_kg + (order.order_total_avoided_waste_kg or 0)).quantize(Decimal('0.01'))
    prog.total_co2_kg = (prog.total_co2_kg + (order.order_total_avoided_co2_kg or 0)).quantize(Decimal('0.01'))
    prog.total_savings_eur = (prog.total_savings_eur + (order.order_total_savings or 0)).quantize(Decimal('0.01'))

    # Uniques producers
    new_ids = _collect_producer_ids_from_order(order)
    seen = set(prog.seen_producer_ids or [])
    merged = sorted(seen.union(new_ids))
    prog.seen_producer_ids = merged
    prog.producers_supported = len(merged)
    prog.save()

    # Give rewards
    tiers = RewardTier.objects.filter(is_active=True)
    owned = set(Reward.objects.filter(user=order.user).values_list('title', flat=True))

    for t in tiers:
        if t.title in owned:
            continue
        if (
            prog.total_orders >= t.min_orders and
            prog.total_waste_kg >= t.min_waste_kg and
            prog.total_co2_kg >= t.min_co2_kg and
            prog.producers_supported >= t.min_producers_supported and
            prog.total_savings_eur >= t.min_savings_eur
        ):
            Reward.objects.create(user=order.user, title=t.title, description=t.description)



User = get_user_model()

# === Authentification JWT personnalisée ===

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    username_field = User.EMAIL_FIELD

    def validate(self, attrs):
        data = super().validate(attrs)
        data['user'] = {
            'id': self.user.id,
            'email': self.user.email,
            'first_name': self.user.first_name,
            'last_name': self.user.last_name,
            'full_name': f"{self.user.first_name} {self.user.last_name}",
            'type': self.user.type,
            'phone': self.user.phone,
            'date_of_birth': self.user.date_of_birth
        }
        return data

class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer



# === Gestion des utilisateurs ===

class RegisterUserView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = CustomUserSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response({"message": "Utilisateur créé avec succès"}, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class MeView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        serializer = UserSerializer(request.user)
        return Response(serializer.data)

    def patch(self, request):
        serializer = CustomUserSerializer(request.user, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=400)


class AdminUsersView(StandardResponseMixin, APIView):
    permission_classes = [IsAdminUser]
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def get(self, request):
        if not request.user.is_superuser:
            return self.standard_response(False, "Only superusers can list users.", status.HTTP_403_FORBIDDEN)

        qs = (User.objects
            .all()
            .select_related('usersetting')
            .order_by("-date_joined" if hasattr(User, "date_joined") else "-id"))
    
        user_type = request.query_params.get("type")
        if user_type in ("producer", "customer"):
            qs = qs.filter(type=user_type)

        is_active = request.query_params.get("is_active")
        if is_active in ("true", "false"):
            qs = qs.filter(is_active=(is_active == "true"))

        q = request.query_params.get("q")
        if q:
            qs = qs.filter(
                Q(email__icontains=q) |
                Q(first_name__icontains=q) |
                Q(last_name__icontains=q) |
                Q(public_display_name__icontains=q)
            )

        data = CustomUserSerializer(qs, many=True, context={"request": request}).data

        # Option A: return wrapped (keep StandardResponse style)
        return self.standard_response(True, "Users fetched.", data=data, status_code=status.HTTP_200_OK)

        # Option B (if you prefer raw array, uncomment next line and remove standard_response above):
        # return Response(data, status=status.HTTP_200_OK)

    def post(self, request):
        if not request.user.is_superuser:
            return self.standard_response(False, "Only superusers can create users.", status.HTTP_403_FORBIDDEN)

        data = request.data
        for f in ("type", "first_name", "last_name", "email", "password"):
            if not data.get(f):
                return self.standard_response(False, f"Missing required field: {f}", status.HTTP_400_BAD_REQUEST)

        user_type = data.get("type")
        if user_type not in ("producer", "customer"):
            return self.standard_response(False, "Invalid type. Must be 'producer' or 'customer'.", status.HTTP_400_BAD_REQUEST)

        dob = None
        dob_str = data.get("date_of_birth")
        if dob_str:
            try:
                dob = date.fromisoformat(dob_str)
            except ValueError:
                return self.standard_response(False, "Invalid date_of_birth. Use YYYY-MM-DD.", status.HTTP_400_BAD_REQUEST)

        years = data.get("years_of_experience")
        try:
            years = int(years) if years not in (None, "",) else 0
        except ValueError:
            return self.standard_response(False, "Invalid years_of_experience. Must be an integer.", status.HTTP_400_BAD_REQUEST)

        avatar_file = request.FILES.get("avatar")

        user = User.objects.create_user(
            email=data.get("email"),
            password=data.get("password"),
            first_name=data.get("first_name"),
            last_name=data.get("last_name"),
            type=user_type,
            phone=data.get("phone") or "",
            date_of_birth=dob,
            public_display_name=data.get("public_display_name") or "",
            years_of_experience=years,
            description_utilisateur=data.get("description") or "",
            is_active=True,
        )

        if avatar_file:
            user.avatar = avatar_file
            user.save(update_fields=["avatar"])

        serialized = CustomUserSerializer(user, context={"request": request}).data
        return self.standard_response(True, "User created successfully.", data=serialized, status_code=status.HTTP_201_CREATED)


class AdminDeletionRequestedCustomersView(StandardResponseMixin, APIView):
    permission_classes = [IsAdminUser]

    def get(self, request):
        if not request.user.is_superuser:
            return self.standard_response(False, "Only superusers can list users.", status.HTTP_403_FORBIDDEN)

        qs = (User.objects
              .filter(type="customer")
              .select_related('usersetting')
              .filter(usersetting__account_deletion_requested__isnull=False)
              .order_by("-usersetting__account_deletion_requested", "-id"))

        data = CustomUserSerializer(qs, many=True, context={"request": request}).data
        return self.standard_response(True, "Customers with deletion requested fetched.", data=data, status_code=status.HTTP_200_OK)
    

class AdminUserUpdateView(StandardResponseMixin, APIView):
    permission_classes = [IsAdminUser]
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def patch(self, request, pk):
        if not request.user.is_superuser:
            return self.standard_response(False, "Only superusers can edit users.", status.HTTP_403_FORBIDDEN)

        user = get_object_or_404(User, pk=pk)

        # Allowed fields to update logically (profile fields)
        allowed_fields = {
            "type", "first_name", "last_name", "email", "phone",
            "date_of_birth", "public_display_name", "years_of_experience",
            "description_utilisateur"
        }

        # Copy only allowed fields
        data = {k: v for k, v in request.data.items() if k in allowed_fields}

        # Avatar optional in multipart
        avatar_file = request.FILES.get("avatar")
        if avatar_file:
            data["avatar"] = avatar_file

        serializer = CustomUserSerializer(user, data=data, partial=True, context={"request": request})
        if serializer.is_valid():
            serializer.save()
            return self.standard_response(True, "User updated.", data=serializer.data, status_code=status.HTTP_200_OK)

        return self.standard_response(False, "Validation error.", errors=serializer.errors, status_code=status.HTTP_400_BAD_REQUEST)


class AdminUserDeactivateView(StandardResponseMixin, APIView):
    permission_classes = [IsAdminUser]

    def post(self, request, pk):
        if not request.user.is_superuser:
            return self.standard_response(False, "Only superusers can deactivate users.", status.HTTP_403_FORBIDDEN)

        user = get_object_or_404(User, pk=pk)
        if not user.is_active:
            return self.standard_response(True, "User already inactive.", data={"id": user.pk, "is_active": user.is_active}, status_code=status.HTTP_200_OK)

        user.is_active = False
        user.save(update_fields=["is_active"])
        return self.standard_response(True, "User deactivated.", data={"id": user.pk, "is_active": user.is_active}, status_code=status.HTTP_200_OK)


class AdminUserActivateView(StandardResponseMixin, APIView):
    permission_classes = [IsAdminUser]

    def post(self, request, pk):
        if not request.user.is_superuser:
            return self.standard_response(False, "Only superusers can activate users.", status.HTTP_403_FORBIDDEN)

        user = get_object_or_404(User, pk=pk)
        if user.is_active:
            return self.standard_response(True, "User already active.", data={"id": user.pk, "is_active": user.is_active}, status_code=status.HTTP_200_OK)

        user.is_active = True
        user.save(update_fields=["is_active"])
        return self.standard_response(True, "User activated.", data={"id": user.pk, "is_active": user.is_active}, status_code=status.HTTP_200_OK)


class AdminUserHardDeleteView(StandardResponseMixin, APIView):
    permission_classes = [IsAdminUser]

    def post(self, request, pk):
        if not request.user.is_superuser:
            return self.standard_response(
                False, "Only superusers can hard-delete users.",
                status_code=status.HTTP_403_FORBIDDEN
            )

        user = get_object_or_404(User, pk=pk)
        if getattr(user, "is_superuser", False):
            return self.standard_response(
                False, "Refusing to hard-delete a superuser.",
                status_code=status.HTTP_400_BAD_REQUEST
            )

        try:
            logger.info("Hard delete START user_id=%s admin_id=%s", user.id, request.user.id)
            metrics = hard_delete_user_and_related(user.id)
            logger.info("Hard delete OK user_id=%s metrics=%s", user.id, metrics)
        except User.DoesNotExist:
            logger.warning("Hard delete: user already deleted user_id=%s", pk)
            return self.standard_response(True, "User already deleted.", status_code=status.HTTP_200_OK)
        except ProtectedError as e:
            logger.exception("Hard delete PROTECT conflict user_id=%s", pk)
            return self.standard_response(
                False, f"Deletion blocked by PROTECT: {str(e)}",
                status_code=status.HTTP_409_CONFLICT
            )
        except Exception as e:
            logger.exception("Hard delete FAILED user_id=%s", pk)
            if request.user.is_superuser and request.query_params.get("debug") == "1":
                tb = traceback.format_exc()
                return self.standard_response(
                    False, {"message": f"Deletion failed: {str(e)}", "traceback": tb},
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            return self.standard_response(
                False, f"Deletion failed: {str(e)}",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        # Verificación final
        if User.objects.filter(pk=pk).exists():
            logger.error("Hard delete post-check: user STILL EXISTS user_id=%s", pk)
            return self.standard_response(
                False,
                "Deletion executed but user still exists (check FK on_delete or DB constraints).",
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return self.standard_response(
            True,
            {"message": "User and related data permanently deleted.", "metrics": metrics},
            status_code=status.HTTP_200_OK,
        )
    

class VerifyPasswordView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        password = request.data.get('password')
        if not password:
            return Response({'detail': 'Mot de passe requis.'}, status=status.HTTP_400_BAD_REQUEST)

        if not request.user.check_password(password):
            return Response({'detail': 'Mot de passe incorrect.'}, status=status.HTTP_401_UNAUTHORIZED)

        return Response({'detail': 'Mot de passe confirmé.'}, status=status.HTTP_200_OK)
    

class UserDetailViewSet(StandardResponseMixin, viewsets.ViewSet):
    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    def retrieve(self, request, pk=None):
        user = get_object_or_404(User, pk=pk)
        serializer = CustomUserSerializer(user, context={'request': request})
        return self.standard_response(
            success=True,
            message="Données de l'utilisateur récupérées.",
            data=serializer.data,
            status_code=status.HTTP_200_OK
        )

    def partial_update(self, request, pk=None):
        user = get_object_or_404(User, pk=pk)
        if request.user.pk != user.pk and not request.user.is_staff:
            return self.standard_response(success=False, message='Non autorisé.', status_code=status.HTTP_403_FORBIDDEN)

        serializer = CustomUserSerializer(user, data=request.data, partial=True, context={'request': request})
        if serializer.is_valid():
            serializer.save()
            return self.standard_response(
                success=True,
                message='Profil mis à jour avec succès.',
                data=serializer.data,
                status_code=status.HTTP_200_OK
            )
        else:
            return self.standard_response(
                success=False,
                message='Erreur de validation.',
                errors=serializer.errors,
                status_code=status.HTTP_400_BAD_REQUEST
            )



# === ViewSets pour les ressources principales ===

class PostalCodesListAPIView(APIView):
    def get(self, request):
        cities = City.objects \
            .filter(postal_code__regex=r'^\d{5}$') \
            .values('id', 'postal_code', 'name', 'country_name') \
            .order_by('postal_code') \
            .distinct('postal_code')

        data = [
            {
                'id': city['id'],
                'postal_code': city['postal_code'],
                'ville': city['name'],
                'country_name': city['country_name']
            }
            for city in cities
        ]
        return Response(data)
    

class PostalInfoAPIView(APIView):
    def get(self, request, postal_code):
        city = cached_first(City.objects.filter(postal_code=postal_code))
        if not city:
            return Response({'detail': 'Code postal non trouvé'}, status=404)

        data = {
            'postal_code': city.postal_code,
            'ville': city.name,
            'code_departement': city.department.code,
            'nom_departement': city.department.name,
            'code_region': city.department.region.code,
            'nom_region': city.department.region.name,
            'latitude': city.latitude,
            'longitude': city.longitude,
            'country_name': city.country_name
        }
        return Response(data)

class AddressViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    serializer_class = AddressSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return Address.objects.filter(user=self.request.user, is_active=True) \
            .order_by('-is_primary', '-updated_at', '-created_at')

    def perform_create(self, serializer):
        user = self.request.user
        is_primary = serializer.validated_data.get('is_primary', False)

        if is_primary:
            Address.objects.filter(user=user, is_primary=True).update(is_primary=False)

        is_first = not Address.objects.filter(user=user).exists()
        serializer.save(user=user, is_primary=is_primary or is_first)

    def perform_destroy(self, instance):
        if instance.user != self.request.user:
            raise PermissionDenied("Vous ne pouvez supprimer que vos propres adresses.")
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save()
        return self.standard_response(
            success=True,
            message="Adresse désactivée avec succès.",
            status_code=status.HTTP_200_OK
        )

class CompanyViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated]
    serializer_class = CompanySerializer

    def get_queryset(self):
        return self.request.user.companies.filter(is_active=True).order_by('-updated_at')

    def perform_create(self, serializer):
        serializer.save(owner=self.request.user, is_active=True)
    
    def perform_destroy(self, instance):
        if instance.owner != self.request.user:
            raise PermissionDenied("Vous ne pouvez supprimer que vos propres entreprises.")
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save()
        return self.standard_response(
            success=True,
            message="Entreprise désactivée avec succès.",
            status_code=status.HTTP_200_OK
        )
    
    @action(detail=True, methods=['get'], permission_classes=[IsAuthenticated])
    def certifications(self, request, pk=None):
        company = self.get_object()
        if company.owner != request.user:
            raise PermissionDenied("Vous ne pouvez voir que vos propres entreprises.")
        certs = Certification.objects.filter(company=company, is_active=True)
        serializer = CertificationSerializer(certs, many=True)
        return Response(serializer.data) 
    

class CertificationViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    serializer_class = CertificationSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return Certification.objects.filter(company__owner=self.request.user, is_active=True)

    def perform_create(self, serializer):
        company = serializer.validated_data.get('company')
        if company.owner != self.request.user:
            raise PermissionDenied("Vous ne pouvez ajouter des certifications qu'à vos propres entreprises.")
        serializer.save(is_active=True)

    def perform_update(self, serializer):
        if serializer.instance.company.owner != self.request.user:
            raise PermissionDenied("Modification non autorisée.")
        serializer.save()

    def perform_destroy(self, instance):
        if instance.company.owner != self.request.user:
            raise PermissionDenied("Vous ne pouvez supprimer que vos propres certifications.")
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save()
        return self.standard_response(
            success=True,
            message="Certification désactivée avec succès.",
            status_code=status.HTTP_200_OK
        )


class ProductCategoryViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    serializer_class = ProductCategorySerializer
    #permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return ProductCategory.objects.filter(is_active=True).order_by('label')

    def perform_destroy(self, instance):
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save()
        return self.standard_response(
            success=True,
            message="Catégorie désactivée avec succès.",
            status_code=status.HTTP_200_OK
        )

class ProductCatalogViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    serializer_class = ProductCatalogSerializer  
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return ProductCatalog.objects.filter(is_active=True).order_by('name')

    def perform_destroy(self, instance):
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save()
        return self.standard_response(
            success=True,
            message="Produit du catalogue désactivé avec succès.",
            status_code=status.HTTP_200_OK
        )

    @action(detail=True, methods=['get'], permission_classes=[IsAuthenticated])
    def units(self, request, pk=None):
        catalog = self.get_object()

        unit_values = (
            ProductImpact.objects
            .filter(product=catalog)
            .values_list('unit', flat=True)
            .distinct()
        )

        unit_choices = dict(Product._meta.get_field('unit').choices)
        data = [
            {"value": u, "label": unit_choices.get(u, u)}
            for u in unit_values
        ]
        return Response(data)

    @action(detail=True, methods=['get'], permission_classes=[IsAuthenticated])
    def impacts(self, request, pk=None):
        catalog = self.get_object()
        qs = ProductImpact.objects.filter(product=catalog).order_by('unit', 'quantity')
        serializer = ProductImpactSerializer(qs, many=True)
        return Response(serializer.data)


class ProductViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    serializer_class = ProductSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        queryset = Product.objects.filter(company__owner=self.request.user, is_active=True)
        company_id = self.request.query_params.get('company')
        if company_id:
            queryset = queryset.filter(company_id=company_id)
        return queryset

    def perform_create(self, serializer):
        if 'catalog_entry' not in serializer.validated_data:
            raise ValidationError("Le champ 'catalog_entry' est requis.")
        company = serializer.validated_data.get('company')
        if company is None or company.owner != self.request.user:
            raise PermissionDenied("Vous ne pouvez créer des produits que pour vos propres entreprises.")
        
        product = serializer.save(is_active=True)
        self.handle_images(product)

    def perform_update(self, serializer):
        if not serializer.partial and 'catalog_entry' not in serializer.validated_data:
            raise ValidationError("Le champ 'catalog_entry' est requis.")
        if serializer.instance.company.owner != self.request.user:
            raise PermissionDenied("Vous ne pouvez modifier que vos propres produits.")
        product = serializer.save()

        keep_ids = self.request.data.get("keep_image_ids", "")
        keep_ids = [int(i) for i in keep_ids.split(",") if i.isdigit()]
        product.images.exclude(id__in=keep_ids).delete()
        self.handle_images(product)

    def perform_destroy(self, instance):
        if instance.user != self.request.user:
            raise PermissionDenied("Vous ne pouvez supprimer que vos propres produits.")
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        return self.standard_response(
            success=True,
            message="Produit désactivé avec succès.",
            status_code=status.HTTP_200_OK
        )

    def handle_images(self, product):
        images = self.request.FILES.getlist('images')
        for img in images:
            ProductImage.objects.create(product=product, image=img)


class ProductBundleViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    serializer_class = ProductBundleSerializer
    permission_classes = [IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def create(self, request, *args, **kwargs):
        data = request.data.copy()

        items_raw = data.get('items')
        if items_raw and isinstance(items_raw, str):
            try:
                items_list = json.loads(items_raw)
            except json.JSONDecodeError:
                return Response({"items": ["Format JSON invalide."]}, status=400)
            data.setlist('items', items_list) 

            serializer = ProductBundleSerializer(data={
                "title": data.get("title"),
                "stock": data.get("stock"),
                "discounted_percentage": data.get("discounted_percentage"),
                "company": data.get("company"),
                "items": items_list
            }, context={"request": request})

            if serializer.is_valid():
                bundle = serializer.save()

                for image_file in request.FILES.getlist("bundle_images"):
                    ProductBundleImage.objects.create(bundle=bundle, image=image_file)

                return self.standard_response(
                    success=True,
                    message="Lot de produits créé avec succès.",
                    data=serializer.data,
                    status_code=status.HTTP_201_CREATED
                )
            else:
                return Response(serializer.errors, status=400)

        return Response({"items": ["Ce champ est requis."]}, status=400)
    
    def get_queryset(self):
        return ProductBundle.objects.filter(
            is_active=True,
            items__product__company__owner=self.request.user
        ).distinct()

    def perform_create(self, serializer):
        bundle = serializer.save()
        try:
            self._validate_and_calculate(bundle)
        except ValidationError as e:
            raise PermissionDenied(str(e))

    def perform_update(self, serializer):
        bundle = serializer.save()
        try:
            self._validate_and_calculate(bundle)
        except ValidationError as e:
            raise PermissionDenied(str(e))

        keep_ids = self.request.data.get("keep_image_ids", "")
        keep_ids = [int(i) for i in keep_ids.split(",") if i.isdigit()]
        bundle.images.exclude(id__in=keep_ids).delete()
        self.handle_images(bundle)

    def perform_destroy(self, instance):
        print(">>>>> Eliminar bundle:", instance.id)
        print(">>>>> Usuario:", self.request.user)
        print(">>>>> Data enviada:", self.request.data)
        companies = {item.product.company.owner_id for item in instance.items.all()}
        if len(companies) != 1 or self.request.user.id not in companies:
            raise PermissionDenied("Vous ne pouvez supprimer que vos propres lots.")

        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save()
        return self.standard_response(
            success=True,
            message="Lot désactivé avec succès.",
            status_code=status.HTTP_200_OK
        )

    def _validate_and_calculate(self, bundle):
        items = bundle.items.all()
        if not items.exists():
            raise ValidationError("Le lot doit contenir au moins un produit.")

        companies = {item.product.company for item in items}
        if len(companies) != 1:
            raise ValidationError("Tous les produits doivent appartenir à la même entreprise.")

        total_price = Decimal('0.0')
        for item in items:
            product = item.product
            if item.quantity * bundle.stock > product.stock:
                raise ValidationError(f"Stock insuffisant pour le produit '{product.title}'.")

            total_price += Decimal(product.original_price) * item.quantity

        bundle.original_price = total_price

        discount_percentage = Decimal(str(bundle.discounted_percentage or 0))
        reduction = Decimal('1') - (discount_percentage / Decimal('100'))
        bundle.discounted_price = (total_price * reduction).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        bundle.save()

    def handle_images(self, product):
        images = self.request.FILES.getlist('images')
        for img in images:
            ProductImage.objects.create(product=product, image=img)
    
            
class ProductBundleItemViewSet(StandardResponseMixin, viewsets.ModelViewSet):
    serializer_class = ProductBundleItemSerializer
    #permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return ProductBundleItem.objects.filter(bundle__company__owner=self.request.user, is_active=True)

    def perform_create(self, serializer):
        bundle = serializer.validated_data.get('bundle')
        product = serializer.validated_data.get('product')

        if bundle.company.owner != self.request.user or product.company.owner != self.request.user:
            raise PermissionDenied("Vous ne pouvez ajouter que vos propres produits à vos propres lots.")

        if not bundle.company.is_active:
            raise PermissionDenied("Vous ne pouvez pas ajouter de produit à une entreprise inactive.")
                
        serializer.save()

    def perform_update(self, serializer):
        if serializer.instance.bundle.company.owner != self.request.user:
            raise PermissionDenied("Modification non autorisée.")
        serializer.save()

    def perform_destroy(self, instance):
        if instance.user != self.request.user:
            raise PermissionDenied("Vous ne pouvez supprimer que vos propres produits.")
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save()
        return self.standard_response(
            success=True,
            message="Produit désactivé avec succès.",
            status_code=status.HTTP_200_OK
        )


class PublicProductBundleDetailView(RetrieveAPIView):
    queryset = ProductBundle.objects.filter(is_active=True)
    serializer_class = ProductBundleSerializer
    permission_classes = [AllowAny]
    lookup_field = 'id'


#class PublicProductBundleListView(ListAPIView):
#    queryset = ProductBundle.objects.filter(is_active=True, status='published')
#    serializer_class = ProductBundleSerializer
#    permission_classes = [AllowAny]


class PaymentMethodViewSet(viewsets.ModelViewSet):
    serializer_class = PaymentMethodSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [filters.OrderingFilter]
    ordering_fields = ['type', 'provider_name', 'digits']
    ordering = ['type']  # orden por defecto

    def get_queryset(self):
        return PaymentMethod.objects.filter(user=self.request.user)

    def perform_create(self, serializer):
        user = self.request.user
        is_default = serializer.validated_data.get('is_default', False)

        if is_default:
            PaymentMethod.objects.filter(user=user, is_default=True).update(is_default=False)
        serializer.save(user=user)

    def perform_update(self, serializer):
        payment_method = self.get_object()

        if payment_method.user != self.request.user:
            raise PermissionDenied("Cette méthode de paiement ne vous appartient pas.")

        is_default = serializer.validated_data.get('is_default', False)
        if is_default:
            PaymentMethod.objects.filter(user=self.request.user, is_default=True).exclude(id=payment_method.id).update(is_default=False)

        serializer.save()


class UserSettingView(StandardResponseMixin, APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        settings, _ = UserSetting.objects.get_or_create(user=request.user)
        serializer = UserSettingSerializer(settings)
        return self.standard_response(
            success=True,
            message="Préférences récupérées avec succès.",
            data=serializer.data,
            status_code=status.HTTP_200_OK
        )

    def patch(self, request):
        settings, _ = UserSetting.objects.get_or_create(user=request.user)
        serializer = UserSettingSerializer(settings, data=request.data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return self.standard_response(
                success=True,
                message="Préférences mises à jour avec succès.",
                data=serializer.data,
                status_code=status.HTTP_200_OK
            )
        return self.standard_response(
            success=False,
            message="Erreur de validation.",
            errors=serializer.errors,
            status_code=status.HTTP_400_BAD_REQUEST
        )


class ExportUserDataView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user_data = export_user_data(request.user)
        return Response(user_data)


class DownloadUserDataView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        settings, _ = UserSetting.objects.get_or_create(user=request.user)
        settings.download_data_requested = now()
        settings.save()

        file_path = export_user_data(request.user)
        if not os.path.exists(file_path):
            return Response({'detail': 'Erreur lors de la génération du fichier.'}, status=500)

        filename = os.path.basename(file_path)
        response = FileResponse(open(file_path, 'rb'), as_attachment=True, filename=filename)
        return response
    
    
class AccountDeletionRequestView(StandardResponseMixin, APIView):
    permission_classes = [IsAuthenticated]

    def patch(self, request):
        settings, _ = UserSetting.objects.get_or_create(user=request.user)
        settings.account_deletion_requested = now()
        settings.save()

        return self.standard_response(
            success=True,
            message="Demande de suppression enregistrée.",
            status_code=status.HTTP_200_OK
        )



class OrderViewSet(viewsets.ModelViewSet):
    queryset = Order.objects.all()
    serializer_class = OrderSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        from django.db.models import Prefetch

        user = self.request.user

        bundle_items_qs = (
            ProductBundleItem.objects
            .select_related("product__company__address__city__department__region",
                            "product__catalog_entry")
            .prefetch_related("product__images")
            .order_by("id")
        )
        items_qs = (
            OrderItem.objects
            .filter(is_active=True)
            .select_related("bundle")
            .prefetch_related(Prefetch("bundle__items", queryset=bundle_items_qs))
        )

        base = (
            Order.objects
            .order_by("-created_at")
            .select_related(
                "shipping_address__city__department__region",
                "billing_address__city__department__region",
                "payment_method",
            )
            .prefetch_related(Prefetch("items", queryset=items_qs))
        )
    
        return base if user.is_staff else base.filter(user=user)

    # ---------- helpers snapshots ----------
    def _address_snapshot(self, addr: Address) -> dict:
        """
        Snapshot PLANO coherente con analytics:
        - line1, complement
        - city, postal_code
        - department_code, department
        - region_code, region
        - country
        + ids opcionales (útiles para debug)
        """
        if not addr:
            return None
        city = getattr(addr, "city", None)
        dep = getattr(city, "department", None) if city else None
        reg = getattr(dep, "region", None) if dep else None
        return {
            "line1": " ".join([str(getattr(addr, "street_number", "") or "").strip(),
                               (getattr(addr, "street_name", "") or "").strip()]).strip() or None,
            "complement": getattr(addr, "complement", None),
            "city": getattr(city, "name", None),
            "city_id": getattr(city, "id", None),
            "postal_code": getattr(city, "postal_code", None),
            "department_code": getattr(dep, "code", None),
            "department": getattr(dep, "name", None),
            "department_id": getattr(dep, "id", None),
            "region_code": getattr(reg, "code", None),
            "region": getattr(reg, "name", None),
            "region_id": getattr(reg, "id", None),
            "country": getattr(city, "country_name", None),
        }

    def _producer_from_company(self, company):
        """
        Devuelve (producer_id, producer_name_mostrable)
        """
        if not company:
            return None, None
        producer = getattr(company, "owner", None)
        if not producer:
            return None, None
        display = getattr(producer, "public_display_name", None)
        if not display:
            first = (producer.first_name or "").strip()
            last = (producer.last_name or "").strip()
            display = " ".join([first, last]).strip() or producer.email
        return producer.id, display

    # -----------------------------------------------------

    def create(self, request, *args, **kwargs):
        with transaction.atomic():
            user = request.user
            data = request.data

            shipping_address = get_object_or_404(Address, id=data.get('shipping_address_id'), user=user)
            billing_address = get_object_or_404(Address, id=data.get('billing_address_id'), user=user)
            payment_method = get_object_or_404(PaymentMethod, id=data.get('payment_method_id'), user=user)

            items_data = data.get('items', [])
            if not items_data:
                return Response({"detail": "Aucun article fourni."}, status=status.HTTP_400_BAD_REQUEST)

            order_total_waste_kg = Decimal(str(data.get('order_total_avoided_waste_kg', 0)))
            order_total_co2_kg   = Decimal(str(data.get('order_total_avoided_co2_kg', 0)))

            order_subtotal = Decimal('0.00')
            order_shipping = Decimal(str(data.get('shipping_cost', 0)))
            order_total_savings = Decimal('0.00')

            # Snapshots de dirección (FLAT, coherente con analytics)
            shipping_snapshot = self._address_snapshot(shipping_address)
            billing_snapshot  = self._address_snapshot(billing_address)

            order = Order.objects.create(
                user=user,
                subtotal=Decimal('0.00'),
                shipping_cost=order_shipping,
                total_price=Decimal('0.00'),
                shipping_address=shipping_address,
                billing_address=billing_address,
                payment_method=payment_method,

                # 👇 snapshots normalizados:
                shipping_address_snapshot=shipping_snapshot,
                billing_address_snapshot=billing_snapshot,

                payment_method_snapshot={
                    "type": payment_method.type,
                    "provider": payment_method.provider_name,
                    "digits": f"•••• {payment_method.digits[-4:]}" if payment_method.digits else None,
                    "paypal_email": payment_method.paypal_email
                },
                order_total_avoided_waste_kg=order_total_waste_kg,
                order_total_avoided_co2_kg=order_total_co2_kg,
            )

            for item in items_data:
                bundle = ProductBundle.objects.select_for_update().get(id=item['bundle_id'])
                quantity = int(item['quantity'])

                if bundle.stock < quantity:
                    return Response(
                        {"detail": f"Stock insuffisant pour le lot '{bundle.title}'."},
                        status=status.HTTP_409_CONFLICT
                    )

                bundle_items = (
                    ProductBundleItem.objects
                    .select_related('product__company__address__city__department__region')
                    .filter(bundle=bundle)
                )
                product_ids = [bi.product_id for bi in bundle_items]

                products_stock = {
                    p.id: p.stock
                    for p in Product.objects.filter(id__in=product_ids).only('id', 'stock')
                }

                insuff = []
                for bi in bundle_items:
                    required_units = bi.quantity * quantity
                    current_stock = int(products_stock.get(bi.product_id, 0))
                    if current_stock < required_units:
                        insuff.append({
                            "product_id": bi.product_id,
                            "title": bi.product.title,
                            "stock_before": current_stock,
                            "stock_after": int(current_stock - required_units),
                            "per_bundle_quantity": int(bi.quantity)
                        })

                if insuff:
                    return Response(
                        {"detail": "Stock insuffisant pour certains produits du lot.", "products": insuff},
                        status=status.HTTP_409_CONFLICT
                    )

                # Descontar stock de productos componentes
                for bi in bundle_items:
                    required_units = bi.quantity * quantity
                    Product.objects.filter(id=bi.product_id).update(
                        stock=F('stock') - required_units,
                        sold_units=F('sold_units') + required_units
                    )

                bundle_stock_before = int(bundle.stock)
                ProductBundle.objects.filter(id=bundle.id).update(
                    stock=F('stock') - quantity,
                    sold_bundles=F('sold_bundles') + quantity
                )
                bundle_stock_after = bundle_stock_before - quantity

                discounted = Decimal(bundle.discounted_price or 0)
                line_total = (discounted * quantity).quantize(Decimal('0.01'))
                order_subtotal += line_total

                item_savings = (Decimal(bundle.original_price) - discounted) * quantity
                order_total_savings += item_savings

                # products snapshot (incluye categoría)
                products_snapshot = []
                for bi in bundle_items:
                    prod = bi.product
                    cat_id, cat_label = None, None
                    try:
                        cat = prod.catalog_entry.category
                        cat_id = cat.id
                        cat_label = cat.label
                    except Exception:
                        pass
                    products_snapshot.append({
                        "product_id": prod.id,
                        "product_title": prod.title,
                        "per_bundle_quantity": int(bi.quantity),
                        "category_id": cat_id,
                        "category_name": cat_label,
                    })

                #  snapshot del bundle
                first_item = cached_first(bundle_items)
                company_id = None
                company_name = None
                department_payload = None
                region_payload = None
                producer_id = None
                producer_name = None

                if first_item and first_item.product and first_item.product.company:
                    company = first_item.product.company
                    company_id = company.id
                    company_name = company.name

                    # productor
                    producer_id, producer_name = self._producer_from_company(company)

                    try:
                        dept = company.address.city.department
                        reg = dept.region
                        department_payload = {"code": dept.code, "name": dept.name}
                        region_payload = {"code": reg.code, "name": reg.name}
                    except Exception:
                        pass

                bundle_snapshot = {
                    "id": bundle.id,
                    "title": bundle.title,
                    "original_price": str(bundle.original_price),
                    "discounted_price": str(bundle.discounted_price),
                    "stock_before": bundle_stock_before,
                    "stock_after": bundle_stock_after,
                    "created_at": getattr(bundle, "created_at", None).isoformat() if getattr(bundle, "created_at", None) else None,
                    "region": region_payload,
                    "department": department_payload,

                    # 👇 empresa + productor
                    "company_id": company_id,
                    "company_name": company_name,
                    "producer_id": producer_id,
                    "producer_name": producer_name,

                    "products": products_snapshot
                }

                OrderItem.objects.create(
                    order=order,
                    bundle=bundle,
                    quantity=quantity,
                    total_price=line_total,
                    bundle_snapshot=bundle_snapshot,
                    order_item_total_avoided_waste_kg=Decimal(str(item.get('order_item_total_avoided_waste_kg', 0))),
                    order_item_total_avoided_co2_kg=Decimal(str(item.get('order_item_total_avoided_co2_kg', 0))),
                    order_item_savings=item_savings
                )

            order.subtotal = order_subtotal.quantize(Decimal('0.01'))
            order.total_price = (order_subtotal + order_shipping).quantize(Decimal('0.01'))
            order.order_total_savings = order_total_savings.quantize(Decimal('0.01'))
            order.save()

            # Rewards, limpieza de carrito
            update_rewards_for_order(order)

            purchased_bundle_ids = [int(it['bundle_id']) for it in items_data]
            cart = get_or_create_cart(request)

            CartItem.all_objects.filter(
                cart=cart,
                bundle_id__in=purchased_bundle_ids,
                is_active=True
            ).update(
                is_active=False,
                deactivated_at=timezone.now()
            )

            if cart and cart.is_active:
                cart.is_active = False
                cart.deactivated_at = timezone.now()
                cart.save(update_fields=['is_active', 'deactivated_at', 'updated_at'])

            serializer = self.get_serializer(order)
            return Response(serializer.data, status=status.HTTP_201_CREATED)

    @action(detail=True, methods=['post'], url_path='rate')
    def rate_order(self, request, pk=None):
        order = self.get_object()
        if order.user != request.user and not request.user.is_staff:
            raise PermissionDenied("Not allowed.")
        if getattr(order, "status", "").lower() not in ("delivered", "livré", "livree", "livrée"):
            raise ValidationError("Only delivered orders can be rated.")
        if order.customer_rating is not None:
            raise ValidationError("Order already rated.")

        payload = RateOnlySerializer(data=request.data)
        payload.is_valid(raise_exception=True)

        rating = payload.validated_data["rating"]
        note = (payload.validated_data.get("note") or "").strip()

        order.customer_rating = rating
        order.customer_note = note
        order.rated_at = timezone.now()
        order.save(update_fields=["customer_rating", "customer_note", "rated_at", "updated_at"])

        return Response(self.get_serializer(order).data, status=200)

    @action(detail=True, methods=['post'], url_path='items/(?P<item_id>[^/.]+)/rate')
    def rate_order_item(self, request, pk=None, item_id=None):
        order = self.get_object()
        if order.user != request.user and not request.user.is_staff:
            raise PermissionDenied("Not allowed.")
        if getattr(order, "status", "").lower() not in ("delivered", "livré", "livree", "livrée"):
            raise ValidationError("Only delivered orders can be rated.")

        item = get_object_or_404(OrderItem, pk=item_id, order=order, is_active=True)
        if item.customer_rating is not None:
            raise ValidationError("Item already rated.")

        payload = RateOnlySerializer(data=request.data)
        payload.is_valid(raise_exception=True)

        rating = payload.validated_data["rating"]
        note = (payload.validated_data.get("note") or "").strip()

        item.customer_rating = rating
        item.customer_note = note
        item.rated_at = timezone.now()
        item.save(update_fields=["customer_rating", "customer_note", "rated_at", "updated_at"])

        if item.bundle_id:
            recompute_after_bundle(item.bundle_id)

        return Response(OrderItemSerializer(item, context={'request': request}).data, status=200)
    



class FavoriteViewSet(viewsets.ModelViewSet):
    serializer_class = FavoriteSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return Favorite.objects.filter(user=self.request.user, is_active=True)

    def create(self, request, *args, **kwargs):
        bundle_id = request.data.get('bundle_id')
        if not bundle_id:
            return Response({"bundle_id": ["Ce champ est obligatoire."]}, status=400)

        try:
            bundle = ProductBundle.objects.get(pk=bundle_id)
        except ProductBundle.DoesNotExist:
            return Response({"bundle_id": ["Lot introuvable."]}, status=404)

        fav_qs = Favorite.objects.filter(user=request.user, bundle=bundle)
        if fav_qs.filter(is_active=True).exists():
            return Response({"detail": "Ce lot est déjà dans vos favoris."}, status=400)

        fav = cached_first(fav_qs)
        if fav:
            fav.is_active = True
            fav.deactivated_at = None
            fav.save(update_fields=['is_active', 'deactivated_at', 'updated_at'])
            serializer = self.get_serializer(fav)
            return Response(serializer.data, status=200)

        serializer = self.get_serializer(data={'bundle_id': bundle_id})
        serializer.is_valid(raise_exception=True)
        serializer.save(user=request.user)
        headers = self.get_success_headers(serializer.data)
        return Response(serializer.data, status=201, headers=headers)

    def perform_destroy(self, instance):
        # Supprime logiquement un favori (désactive sans effacer)
        if instance.user != self.request.user:
            raise PermissionDenied("Vous ne pouvez pas supprimer un favori d'un autre utilisateur.")
        instance.is_active = False
        instance.deactivated_at = timezone.now()
        instance.save(update_fields=['is_active', 'deactivated_at', 'updated_at'])


class RewardTierViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = RewardTier.objects.filter(is_active=True).order_by('id')
    serializer_class = RewardTierSerializer
    permission_classes = [AllowAny]


class RewardViewSet(viewsets.ReadOnlyModelViewSet):
    serializer_class = RewardSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return Reward.objects.filter(user=self.request.user).order_by('-earned_on')

    @action(detail=False, methods=['get'])
    def progress(self, request):
        prog, _ = UserRewardProgress.objects.get_or_create(user=request.user)
        data = {
            "total_orders": prog.total_orders,
            "total_waste_kg": str(prog.total_waste_kg),
            "total_co2_kg": str(prog.total_co2_kg),
            "total_savings_eur": str(prog.total_savings_eur),
            "producers_supported": prog.producers_supported,
        }
        return Response(data)



VALID_STATUSES = ('confirmed', 'delivered')

def month_range(dt: datetime):
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    next_start = (start + relativedelta(months=1))
    return start, next_start

class ProducerDashboardView(APIView):
    permission_classes = [IsAuthenticated]

    def get_company_ids_for_user(self, user):
        # Adjust this to your ownership model if needed
        return list(Company.objects.filter(owner=user).values_list('id', flat=True))

    def get(self, request):
        tz_now = timezone.localtime()
        cur_start, cur_end = month_range(tz_now)
        prev_start, prev_end = month_range(cur_start - relativedelta(days=1))

        company_ids = self.get_company_ids_for_user(request.user)
        if not company_ids:
            return Response({
                "period": {
                    "current": {"start": cur_start, "end": cur_end},
                    "previous": {"start": prev_start, "end": prev_end}
                },
                "metrics": {
                    "sales": {"current": "0.00", "previous": "0.00", "change_pct": 0.0},
                    "bundles_sold": {"current": 0, "previous": 0, "change_pct": 0.0},
                    "new_customers": {"current": 0, "previous": 0, "change_pct": 0.0},
                    "waste_kg": {"current": "0.00", "previous": "0.00", "change_pct": 0.0},
                },
                "recent": []
            })

        # Subquery: does this OrderItem's bundle contain any product from producer companies?
        exists_company_item = ProductBundleItem.objects.filter(
            bundle=OuterRef('bundle'),
            product__company_id__in=company_ids
        )

        base = OrderItem.objects.annotate(has_company=Exists(exists_company_item))

        cur_items = base.filter(
            has_company=True,
            order__status__in=VALID_STATUSES,
            order__created_at__gte=cur_start,
            order__created_at__lt=cur_end,
        )

        prev_items = base.filter(
            has_company=True,
            order__status__in=VALID_STATUSES,
            order__created_at__gte=prev_start,
            order__created_at__lt=prev_end,
        )

        def agg_block(qs):
            return {
                "sales": qs.aggregate(x=Coalesce(Sum('total_price'), Decimal('0.00')))["x"],
                "bundles": qs.aggregate(x=Coalesce(Sum('quantity'), 0))["x"],
                "waste": qs.aggregate(x=Coalesce(Sum('order_item_total_avoided_waste_kg'), Decimal('0.00')))["x"],
                "customers": qs.values("order__user_id").distinct().count(),
            }

        cur = agg_block(cur_items)
        prev = agg_block(prev_items)

        def pct(cur_val, prev_val):
            try:
                cur_d = Decimal(str(cur_val or 0))
                prev_d = Decimal(str(prev_val or 0))
                if prev_d == 0:
                    return 100.0 if cur_d > 0 else 0.0
                return float(((cur_d - prev_d) / prev_d) * 100)
            except Exception:
                return 0.0

        metrics = {
            "sales": {
                "current": str(Decimal(cur["sales"]).quantize(Decimal('0.01'))),
                "previous": str(Decimal(prev["sales"]).quantize(Decimal('0.01'))),
                "change_pct": pct(cur["sales"], prev["sales"]),
            },
            "bundles_sold": {
                "current": int(cur["bundles"] or 0),
                "previous": int(prev["bundles"] or 0),
                "change_pct": pct(cur["bundles"], prev["bundles"]),
            },
            "new_customers": {
                "current": int(cur["customers"] or 0),
                "previous": int(prev["customers"] or 0),
                "change_pct": pct(cur["customers"], prev["customers"]),
            },
            "waste_kg": {
                "current": str(Decimal(cur["waste"]).quantize(Decimal('0.01'))),
                "previous": str(Decimal(prev["waste"]).quantize(Decimal('0.01'))),
                "change_pct": pct(cur["waste"], prev["waste"]),
            },
        }

        # Recent activity (last 10 orders that include producer items), grouped per order
        recent_orders = (
            base.filter(has_company=True, order__status__in=VALID_STATUSES)
                .values('order_id', 'order__order_code', 'order__created_at',
                        'order__user__first_name', 'order__user__last_name', 'order__user__email')
                .annotate(amount=Coalesce(Sum('total_price'), Decimal('0.00')),
                        items=Coalesce(Sum('quantity'), 0))
                .order_by('-order__created_at')[:10]
        )

        recent = []
        for r in recent_orders:
            first = (r.get('order__user__first_name') or '').strip()
            last  = (r.get('order__user__last_name') or '').strip()
            email = (r.get('order__user__email') or '').strip()
            # “Marie D.”, y si no hay nombre, cae al user antes de la @
            if first or last:
                customer_name = f"{first} {last[:1].upper()+'.' if last else ''}".strip()
            else:
                customer_name = email.split('@')[0] if email else ''

            recent.append({
                "type": "order",
                "order_id": r["order_id"],
                "order_code": r["order__order_code"],
                "amount": str(Decimal(r["amount"]).quantize(Decimal('0.01'))),
                "items": int(r["items"] or 0),
                "customer_name": customer_name,
                "created_at": r["order__created_at"],
            })

        return Response({
            "period": {
                "current": {"start": cur_start, "end": cur_end},
                "previous": {"start": prev_start, "end": prev_end}
            },
            "metrics": metrics,
            "recent": recent
        })



class CartView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        cart = get_or_create_cart(request)
        ser = CartSerializer(cart, context={"request": request})
        return Response(ser.data)


class CartViewSet(viewsets.ViewSet):
    permission_classes = [AllowAny]

    # -------- helpers --------
    def _require_guest_key(self, request):
        key = request.headers.get('X-Session-Key') or request.COOKIES.get('sessionid')
        if not key:
            raise ValueError("Missing X-Session-Key")
        return key

    def _abs_media_url(self, request, value):
        if not value:
            return None
        if hasattr(value, "url"):
            url = getattr(value, "url", None) or ""
        else:
            url = str(value or "")
        if not url:
            return None
        if url.startswith("http://") or url.startswith("https://"):
            return url.replace("/media/media/", "/media/")
        if not url.startswith("/"):
            url = "/" + url
        url = url.replace("/media/media/", "/media/")
        base = request.build_absolute_uri("/")
        if base.endswith("/"):
            base = base[:-1]
        return f"{base}{url}"

    def _get_or_create_cart(self, request):
        user = request.user if request.user.is_authenticated else None
        qs = Cart.objects.filter(is_active=True)

        if user:
            cart = cached_first(qs.filter(user=user).order_by('-updated_at'))
            if not cart:
                cart = Cart.objects.create(user=user, session_key=None)
            return cart

        session_key = self._require_guest_key(request)
        cart = cached_first(qs.filter(session_key=session_key, user__isnull=True).order_by('-updated_at'))
        if not cart:
            cart = Cart.objects.create(user=None, session_key=session_key)
        return cart

    # -------- endpoints --------
    def list(self, request):
        try:
            cart = self._get_or_create_cart(request)
        except ValueError:
            return Response({"detail": "Missing X-Session-Key."}, status=400)
        ser = CartSerializer(cart, context={"request": request})
        return Response(ser.data)

    @action(detail=False, methods=['post'])
    def items(self, request):
        try:
            cart = self._get_or_create_cart(request)
        except ValueError:
            return Response({"detail": "Missing X-Session-Key."}, status=400)

        try:
            bundle_id = int(request.data.get('bundle', 0))
        except (TypeError, ValueError):
            return Response({"detail": "Invalid bundle."}, status=400)

        try:
            qty = max(1, int(request.data.get('quantity', 1)))
        except (TypeError, ValueError):
            return Response({"detail": "Invalid quantity."}, status=400)

        add_waste = Decimal(str(request.data.get('avoided_waste_kg', 0)))
        add_co2 = Decimal(str(request.data.get('avoided_co2_kg', 0)))

        try:
            bundle = ProductBundle.objects.get(pk=bundle_id, is_active=True)
        except ProductBundle.DoesNotExist:
            return Response({"detail": "Bundle introuvable."}, status=404)

        price = bundle.discounted_price or bundle.original_price

        first_img = None
        first_bundle_item = cached_first(bundle.items.select_related('product'))
        if first_bundle_item and hasattr(first_bundle_item.product, 'images'):
            pimg = cached_first(first_bundle_item.product.images.order_by('id'))
            first_img = getattr(pimg, 'image', None)
        bundle_image_value = self._abs_media_url(request, first_img)

        company_id_value = getattr(bundle, 'company_id', None)
        company_name_value = getattr(getattr(bundle, 'company', None), 'name', None)
        if (company_id_value is None or company_name_value is None) and first_bundle_item:
            pco = getattr(first_bundle_item.product, 'company', None)
            if company_id_value is None:
                company_id_value = getattr(pco, 'id', None)
            if company_name_value is None:
                company_name_value = getattr(pco, 'name', None)

        if request.data.get('producer_name'):
            company_name_value = str(request.data.get('producer_name'))
        if request.data.get('company_id') not in (None, ''):
            try:
                company_id_value = int(request.data.get('company_id'))
            except Exception:
                pass

        from datetime import date as _Date
        best_dates = []
        for bi in bundle.items.all():
            d = getattr(bi, 'best_before_date', None)
            if isinstance(d, _Date):
                best_dates.append(d)
        dluo_text_value = max(best_dates).isoformat() if best_dates else ''

        with transaction.atomic():
            existing = cached_first(CartItem.objects.filter(
                cart=cart, bundle=bundle, is_active=True
            ).select_for_update())

            if existing:
                existing.quantity = int(existing.quantity) + qty
                existing.price_snapshot = price
                existing.title_snapshot = bundle.title
                if not existing.bundle_image and bundle_image_value:
                    existing.bundle_image = bundle_image_value
                if not getattr(existing, 'company_id_snapshot', None):
                    existing.company_id_snapshot = company_id_value
                if not getattr(existing, 'company_name_snapshot', None):
                    existing.company_name_snapshot = company_name_value
                if not getattr(existing, 'dluo_snapshot', None):
                    existing.dluo_snapshot = dluo_text_value

                existing.avoided_waste_kg = (existing.avoided_waste_kg or Decimal('0')) + add_waste
                existing.avoided_co2_kg = (existing.avoided_co2_kg or Decimal('0')) + add_co2

                existing.save(update_fields=[
                    'quantity', 'price_snapshot', 'title_snapshot',
                    'bundle_image',
                    'company_id_snapshot', 'company_name_snapshot', 'dluo_snapshot',
                    'avoided_waste_kg', 'avoided_co2_kg',
                    'updated_at'
                ])
                created = False
            else:
                CartItem.objects.create(
                    cart=cart,
                    bundle=bundle,
                    quantity=qty,
                    price_snapshot=price,
                    title_snapshot=bundle.title,
                    bundle_image=bundle_image_value,
                    company_id_snapshot=company_id_value,
                    company_name_snapshot=company_name_value,
                    dluo_snapshot=dluo_text_value,
                    avoided_waste_kg=add_waste,
                    avoided_co2_kg=add_co2
                )
                created = True

        cart.refresh_from_db()
        ser = CartSerializer(cart, context={"request": request})
        return Response(ser.data, status=201 if created else 200)

    @action(detail=False, methods=['get', 'patch', 'delete'], url_path=r'item/(?P<item_id>\d+)')
    def item(self, request, item_id=None):
        try:
            cart = self._get_or_create_cart(request)
        except ValueError:
            return Response({"detail": "Missing X-Session-Key."}, status=400)

        if request.method.lower() == 'get':
            try:
                item = cart.items.get(pk=item_id)
            except CartItem.DoesNotExist:
                return Response({"detail": "Item not found."}, status=404)
            return Response(CartItemSerializer(item, context={"request": request}).data)

        if request.method.lower() == 'delete':
            try:
                item = cart.items.get(pk=item_id)
            except CartItem.DoesNotExist:
                return Response({"detail": "Item not found."}, status=404)
            if item.is_active:
                CartItem.all_objects.filter(pk=item.pk).update(
                    is_active=False, deactivated_at=timezone.now()
                )
            cart.refresh_from_db()
            return Response(CartSerializer(cart, context={"request": request}).data)

        # PATCH
        try:
            item = cart.items.get(pk=item_id)
        except CartItem.DoesNotExist:
            return Response({"detail": "Item not found."}, status=404)

        qty_raw = request.data.get('quantity', None)
        if qty_raw is not None:
            try:
                qty = int(qty_raw)
            except (TypeError, ValueError):
                return Response({"detail": "Invalid quantity."}, status=400)
            if qty <= 0:
                CartItem.all_objects.filter(pk=item.pk).update(
                    is_active=False, deactivated_at=timezone.now()
                )
                cart.refresh_from_db()
                return Response(CartSerializer(cart, context={"request": request}).data)
            item.quantity = qty

        if 'avoided_waste_kg' in request.data:
            try:
                item.avoided_waste_kg = Decimal(str(request.data.get('avoided_waste_kg')))
            except Exception:
                return Response({"detail": "Invalid avoided_waste_kg."}, status=400)

        if 'avoided_co2_kg' in request.data:
            try:
                item.avoided_co2_kg = Decimal(str(request.data.get('avoided_co2_kg')))
            except Exception:
                return Response({"detail": "Invalid avoided_co2_kg."}, status=400)

        if 'producer_name' in request.data and request.data.get('producer_name'):
            item.company_name_snapshot = str(request.data.get('producer_name'))
        if 'company_id' in request.data and request.data.get('company_id') not in (None, ''):
            try:
                item.company_id_snapshot = int(request.data.get('company_id'))
            except Exception:
                pass

        if request.data.get('refresh_image', False) or not item.bundle_image:
            first_img = None
            first_bundle_item = cached_first(item.bundle.items.select_related('product'))
            if first_bundle_item and hasattr(first_bundle_item.product, 'images'):
                pimg = cached_first(first_bundle_item.product.images.order_by('id'))
                first_img = getattr(pimg, 'image', None)
            item.bundle_image = self._abs_media_url(request, first_img)

        if request.data.get('refresh_dluo', False) or not item.dluo_snapshot:
            from datetime import date as _Date
            best_dates = []
            for bi in item.bundle.items.all():
                d = getattr(bi, 'best_before_date', None)
                if isinstance(d, _Date):
                    best_dates.append(d)
            item.dluo_snapshot = max(best_dates).isoformat() if best_dates else ''

        item.save(update_fields=[
            'quantity', 'avoided_waste_kg', 'avoided_co2_kg',
            'bundle_image',
            'company_id_snapshot', 'company_name_snapshot', 'dluo_snapshot',
            'updated_at'
        ])
        cart.refresh_from_db()
        return Response(CartSerializer(cart, context={"request": request}).data)

    @action(detail=False, methods=['delete'], url_path='clear')
    def clear(self, request):
        try:
            cart = self._get_or_create_cart(request)
        except ValueError:
            return Response({"detail": "Missing X-Session-Key."}, status=400)
        CartItem.all_objects.filter(cart=cart, is_active=True).update(
            is_active=False, deactivated_at=timezone.now()
        )
        cart.refresh_from_db()
        return Response(CartSerializer(cart, context={"request": request}).data)

    @action(detail=False, methods=['post'], url_path='merge', permission_classes=[IsAuthenticated])
    def merge(self, request):
        session_key = request.headers.get('X-Session-Key') or request.data.get('session_key')
        user = request.user
        user_cart = cached_first(Cart.objects.filter(is_active=True, user=user).order_by('-updated_at'))

        if not session_key:
            if not user_cart:
                user_cart = Cart.objects.create(user=user, session_key=None, is_active=True)
            ser = CartSerializer(user_cart, context={"request": request})
            return Response(ser.data)

        guest_cart = (
            cached_first(Cart.objects.filter(is_active=True, user__isnull=True, session_key=session_key)
            .order_by('-updated_at')
            )
        )

        if not guest_cart:
            if not user_cart:
                user_cart = Cart.objects.create(user=user, session_key=None, is_active=True)
            ser = CartSerializer(user_cart, context={"request": request})
            return Response(ser.data)

        with transaction.atomic():
            if not user_cart:
                user_cart = Cart.objects.create(user=user, session_key=None, is_active=True)

            guest_items = CartItem.all_objects.select_for_update().filter(
                cart=guest_cart, is_active=True
            )
            for gi in guest_items:
                existing = cached_first(CartItem.objects.select_for_update().filter(
                    cart=user_cart, bundle=gi.bundle, is_active=True
                ))

                if existing:
                    existing.quantity = int(existing.quantity) + int(gi.quantity or 0)
                    existing.price_snapshot = gi.price_snapshot or existing.price_snapshot
                    existing.title_snapshot = gi.title_snapshot or existing.title_snapshot

                    if not existing.bundle_image and getattr(gi, 'bundle_image', None):
                        existing.bundle_image = gi.bundle_image
                    if not getattr(existing, 'company_id_snapshot', None):
                        existing.company_id_snapshot = getattr(gi, 'company_id_snapshot', None)
                    if not getattr(existing, 'company_name_snapshot', None):
                        existing.company_name_snapshot = getattr(gi, 'company_name_snapshot', None)
                    if not getattr(existing, 'dluo_snapshot', None):
                        existing.dluo_snapshot = getattr(gi, 'dluo_snapshot', None)

                    existing.avoided_waste_kg = (existing.avoided_waste_kg or Decimal('0')) + (gi.avoided_waste_kg or Decimal('0'))
                    existing.avoided_co2_kg = (existing.avoided_co2_kg or Decimal('0')) + (gi.avoided_co2_kg or Decimal('0'))

                    existing.save(update_fields=[
                        'quantity', 'price_snapshot', 'title_snapshot',
                        'bundle_image',
                        'company_id_snapshot', 'company_name_snapshot', 'dluo_snapshot',
                        'avoided_waste_kg', 'avoided_co2_kg',
                        'updated_at'
                    ])
                else:
                    CartItem.objects.create(
                        cart=user_cart,
                        bundle=gi.bundle,
                        quantity=int(gi.quantity or 1),
                        price_snapshot=gi.price_snapshot,
                        title_snapshot=gi.title_snapshot,
                        bundle_image=getattr(gi, 'bundle_image', None),
                        company_id_snapshot=getattr(gi, 'company_id_snapshot', None),
                        company_name_snapshot=getattr(gi, 'company_name_snapshot', None),
                        dluo_snapshot=getattr(gi, 'dluo_snapshot', None),
                        avoided_waste_kg=(gi.avoided_waste_kg or Decimal('0')),
                        avoided_co2_kg=(gi.avoided_co2_kg or Decimal('0')),
                    )

            CartItem.all_objects.filter(cart=guest_cart, is_active=True).update(
                is_active=False, deactivated_at=timezone.now()
            )
            guest_cart.is_active = False
            guest_cart.deactivated_at = timezone.now()
            guest_cart.save(update_fields=['is_active', 'deactivated_at', 'updated_at'])

        user_cart.refresh_from_db()
        ser = CartSerializer(user_cart, context={"request": request})
        return Response(ser.data)




class CartItemDetail(APIView):
    """
    Optional standalone endpoints to modify single cart items.
    These mirror the ViewSet actions and consistently soft-deactivate instead of hard-deleting.
    """

    permission_classes = [AllowAny]

    def delete(self, request, pk):
        cart = get_or_create_cart(request)  # shared resolver; same logic as _get_or_create_cart
        item = get_object_or_404(CartItem.all_objects, pk=pk, cart=cart)
        if item.is_active:
            item.is_active = False
            item.deactivated_at = timezone.now()
            item.save(update_fields=["is_active", "deactivated_at", "updated_at"])
        return Response(status=status.HTTP_204_NO_CONTENT)

    def patch(self, request, pk):
        cart = get_or_create_cart(request)
        item = get_object_or_404(CartItem.all_objects, pk=pk, cart=cart)
        try:
            qty = int(request.data.get("quantity", 1))
        except (TypeError, ValueError):
            return Response({"detail": "Invalid quantity."}, status=400)

        if qty <= 0:
            if item.is_active:
                item.is_active = False
                item.deactivated_at = timezone.now()
                item.save(update_fields=["is_active", "deactivated_at", "updated_at"])
            return Response(status=status.HTTP_204_NO_CONTENT)

        # If the item was inactive, reactivate/update it for completeness
        item.is_active = True
        item.deactivated_at = None
        item.quantity = qty
        item.save(update_fields=["quantity", "is_active", "deactivated_at", "updated_at"])

        return Response(CartItemSerializer(item).data)


class CartClearView(APIView):
    """
    Soft-deactivate all active items in the current cart.
    """
    permission_classes = [AllowAny]
    def delete(self, request):
        cart = get_or_create_cart(request)
        CartItem.all_objects.filter(cart=cart, is_active=True).update(
            is_active=False, deactivated_at=timezone.now()
        )
        return Response(status=204)



class PublicProducerDetailView(APIView):
    permission_classes = [AllowAny]

    def get(self, request, pk: int):
        from . import serializers as s

        producer = get_object_or_404(
            CustomUser.objects
            .filter(is_active=True, type="producer")
            .select_related("main_address__city__department__region")
            .prefetch_related("addresses__city__department__region"), 
            pk=pk,
        )

        companies = (
            Company.objects
            .filter(owner=producer, is_active=True)
            .select_related("address__city__department__region")
            .prefetch_related("certifications")
        )

        featured_bundles = (
            ProductBundle.objects
            .filter(
                is_active=True,
                status="published",
                discounted_percentage__gt=0,
                stock__gt=0,
                items__product__company__owner=producer,
            )
            .distinct()
            .order_by("-created_at")[:3]
        )

        producer_bundle_ids = list(
            ProductBundleItem.objects
            .filter(product__company__owner=producer)
            .values_list("bundle_id", flat=True)
            .distinct()
        )

        rated_items = (
            OrderItem.objects
            .filter(bundle_id__in=producer_bundle_ids, customer_rating__isnull=False)
            .select_related("bundle")
            .order_by("-rated_at")
        )

        seen = set()
        recent_bundle_ids = []
        for oi in rated_items:
            if oi.bundle_id not in seen:
                seen.add(oi.bundle_id)
                recent_bundle_ids.append(oi.bundle_id)
            if len(recent_bundle_ids) >= 10:
                break

        recent_bundles = (
            ProductBundle.objects
            .filter(id__in=recent_bundle_ids, is_active=True, status="published")
        )

        recent_bundles_ser = s.ProductBundleSerializer(
            recent_bundles, many=True, context={"request": request}
        ).data

        recent_by_id = {b["id"]: b for b in recent_bundles_ser}
        recently_rated_bundles = [recent_by_id[i] for i in recent_bundle_ids if i in recent_by_id]
        for b in recently_rated_bundles:
            evals = b.get("evaluations", []) or []
            b["last_rated_at"] = evals[0]["rated_at"] if evals else None
            b["evaluations"] = evals[:5]

        payload = {
            "producer": s.PublicProducerSerializer(producer, context={"request": request}).data,
            "companies": s.PublicCompanySerializer(companies, many=True, context={"request": request}).data,
            "featured_bundles": s.ProductBundleSerializer(featured_bundles, many=True, context={"request": request}).data,
            "recently_rated_bundles": recently_rated_bundles,
        }
        return Response(payload)


@method_decorator(cache_page(60), name="dispatch")
class PublicProducerListView(generics.ListAPIView):
    permission_classes = [AllowAny]

    def get_serializer_class(self):
        from . import serializers as s
        return s.PublicProducerSerializer

    def get_queryset(self):
        from .models import Company, CustomUser
        from django.db.models import Prefetch

        qs = (
            CustomUser.objects
            .filter(is_active=True, type="producer")
            .select_related("main_address__city__department__region")
            .prefetch_related("addresses__city__department__region")
            .order_by("-created_at")
        )
        qs = qs.prefetch_related(
            Prefetch(
                "companies",
                queryset=(
                    Company.objects
                    .filter(is_active=True)
                    .select_related("address__city__department__region")
                    .prefetch_related("certifications")
                )
            )
        )
        return qs



class ProducerOrdersView(ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = OrderSerializer

    def get_queryset(self):
        from django.db.models import Exists, OuterRef, Prefetch
        from .models import Order, OrderItem, ProductBundleItem, Company

        user = self.request.user
        if getattr(user, "type", None) != "producer":
            from rest_framework.exceptions import PermissionDenied
            raise PermissionDenied("Only producers can access this endpoint.")

        company_ids = list(
            Company.objects.filter(owner=user, is_active=True).values_list("id", flat=True)
        )

        exists_company_item = ProductBundleItem.objects.filter(
            bundle=OuterRef("bundle"),
            product__company_id__in=company_ids,
        )

        bundle_items_qs = (
            ProductBundleItem.objects
            .select_related(
                "product__company__address__city__department__region",
                "product__catalog_entry",
            )
            .prefetch_related("product__images")
            .order_by("id")
        )

        producer_items = (
            OrderItem.objects
            .annotate(has_company=Exists(exists_company_item))
            .filter(has_company=True, is_active=True)
            .select_related("bundle")
            .prefetch_related(Prefetch("bundle__items", queryset=bundle_items_qs))
        )

        include_all = (self.request.query_params.get("include_all_items", "false").lower() == "true")

        qs = (
            Order.objects
            .filter(items__in=producer_items)
            .distinct()
            .order_by("-created_at")
            .select_related(
                "shipping_address__city__department__region",
                "billing_address__city__department__region",
                "payment_method",
            )
        )

        if include_all:
            qs = qs.prefetch_related(
                Prefetch(
                    "items",
                    queryset=OrderItem.objects
                    .filter(is_active=True)
                    .select_related("bundle")
                    .prefetch_related(Prefetch("bundle__items", queryset=bundle_items_qs))
                )
            )
        else:
            qs = qs.prefetch_related(Prefetch("items", queryset=producer_items))

        return qs



class BlogCategoryViewSet(viewsets.ModelViewSet):
    queryset = BlogCategory.objects.all().order_by('order','name')
    serializer_class = BlogCategorySerializer
    permission_classes = [IsAdminUser]
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name','slug','description']
    ordering_fields = ['order','name','created_at']

class PublicBlogPostViewSet(viewsets.ReadOnlyModelViewSet):
    permission_classes = [AllowAny]
    serializer_class = BlogPostReadSerializer
    lookup_field = 'slug'

    def get_queryset(self):
        qs = (BlogPost.objects
              .filter(is_active=True)
              .filter(status='published') 
              .order_by('-pinned','-published_at','-created_at'))
        q = self.request.query_params.get('q')
        if q:
            qs = qs.filter(Q(title__icontains=q) | Q(content__icontains=q) | Q(excerpt__icontains=q))
        cat = self.request.query_params.get('category')
        if cat:
            qs = qs.filter(category__slug=cat)
        return qs
    


class AdminBlogPostViewSet(viewsets.ModelViewSet):
    permission_classes = [IsAdminUser]
    queryset = BlogPost.objects.filter(is_active=True).order_by('-created_at')
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['title','slug','content','excerpt','category__name','author__public_display_name']
    ordering_fields = ['title','published_at','read_time_min','pinned','status','created_at','updated_at','category__name','author__public_display_name']
    ordering = ['-created_at']
    lookup_field = 'pk'
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def get_serializer_class(self):
        return BlogPostWriteSerializer if self.action in ('create','update','partial_update') else BlogPostReadSerializer

    def create(self, request, *args, **kwargs):
        ser = self.get_serializer(data=request.data, context={'request': request})
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)
        self.perform_create(ser)
        return Response(BlogPostReadSerializer(ser.instance, context={'request': request}).data, status=status.HTTP_201_CREATED)

    def update(self, request, *args, **kwargs):
        partial = kwargs.pop('partial', False)
        inst = self.get_object()
        ser = self.get_serializer(inst, data=request.data, partial=partial, context={'request': request})
        if not ser.is_valid():
            return Response(ser.errors, status=status.HTTP_400_BAD_REQUEST)
        self.perform_update(ser)
        return Response(BlogPostReadSerializer(ser.instance, context={'request': request}).data, status=status.HTTP_200_OK)

    def _enforce_single_pin(self, instance: BlogPost):
        if instance.pinned:
            BlogPost.objects.exclude(pk=instance.pk).update(pinned=False)

    def perform_create(self, serializer):
        post = serializer.save(author=self.request.user, is_active=True)
        self._enforce_single_pin(post)

    def perform_update(self, serializer):
        inst = serializer.instance
        author = inst.author or self.request.user
        post = serializer.save(author=author)
        self._enforce_single_pin(post)

    def destroy(self, request, *args, **kwargs):
        instance = self.get_object()
        if instance.is_active:
            instance.is_active = False
            instance.deactivated_at = timezone.now()
            instance.save(update_fields=['is_active', 'deactivated_at', 'updated_at'])
        data = BlogPostReadSerializer(instance, context={'request': request}).data
        return Response(data, status=status.HTTP_200_OK)



class PublicBlogPostListView(generics.ListAPIView):
    permission_classes = [AllowAny]
    serializer_class = BlogPostPublicSerializer

    def get_queryset(self):
        return BlogPost.objects.filter(
            is_active=True,
            status="published",
            published_at__lte=timezone.now()
        ).select_related("category").order_by("-published_at")



class RecommendationsView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        try:
            limit = int(request.GET.get("limit") or 3)
            limit = max(1, min(limit, 12))
        except Exception:
            limit = 3

        eligible_status = ("confirmed", "delivered", "fulfilled", "completed")

        user_order_ids = (
            Order.objects
            .filter(user=request.user, status__in=eligible_status)
            .values_list("id", flat=True)
        )
        user_bundle_ids: Set[int] = set(
            OrderItem.objects
            .filter(order_id__in=user_order_ids)
            .values_list("bundle_id", flat=True)
        )
        has_history = len(user_bundle_ids) > 0

        scores: Dict[int, float] = {}
        if has_history:
            other_user_order_ids = (
                OrderItem.objects
                .filter(bundle_id__in=user_bundle_ids)
                .exclude(order__user=request.user)
                .values_list("order_id", flat=True)
                .distinct()
            )
            purchases_by_order: Dict[int, List[int]] = {}
            for oid, bid in (
                OrderItem.objects
                .filter(order_id__in=other_user_order_ids)
                .values_list("order_id", "bundle_id")
            ):
                purchases_by_order.setdefault(oid, []).append(bid)

            if purchases_by_order:
                scores = rank_copurchased_candidates(user_bundle_ids, purchases_by_order.values())

        qs = ProductBundle.objects.filter(stock__gt=0)
        if has_history:
            qs = qs.exclude(id__in=user_bundle_ids)

        if scores:
            candidates = list(qs.values("id"))
            for c in candidates:
                c["score"] = scores.get(c["id"], 0.0)

            ranked_ids = [
                c["id"] for c in sorted(candidates, key=lambda x: x["score"], reverse=True)
            ][: max(limit * 4, 50)]

            ranked_qs = ProductBundle.objects.filter(id__in=ranked_ids)
            ranked_serialized_list = ProductBundleSerializer(
                ranked_qs, many=True, context={"request": request}
            ).data

            ranked_serialized = {b["id"]: b for b in ranked_serialized_list}
            payload = []
            for bid in ranked_ids:
                b = ranked_serialized.get(bid)
                if b:
                    bb = dict(b)
                    bb["_rec_score"] = scores.get(bid, 0.0)
                    payload.append(bb)

            if len(payload) < limit:
                need = limit - len(payload)
                try:
                    fallback_qs = (
                        ProductBundle.objects
                        .filter(stock__gt=0)
                        .exclude(id__in=set(ranked_ids) | user_bundle_ids)
                        .order_by(
                            F("discounted_percentage").desc(nulls_last=True),
                            F("avg_rating").desc(nulls_last=True),
                        )[: need]
                    )
                except Exception:
                    fallback_qs = (
                        ProductBundle.objects
                        .filter(stock__gt=0)
                        .exclude(id__in=set(ranked_ids) | user_bundle_ids)[: need]
                    )

                payload.extend(ProductBundleSerializer(
                    fallback_qs, many=True, context={"request": request}
                ).data)

            return Response(payload[:limit])

        try:
            fallback_qs = (
                ProductBundle.objects
                .filter(stock__gt=0)
                .order_by(
                    F("discounted_percentage").desc(nulls_last=True),
                    F("avg_rating").desc(nulls_last=True),
                )[: limit]
            )
        except Exception:
            fallback_qs = ProductBundle.objects.filter(stock__gt=0)[:limit]

        return Response(ProductBundleSerializer(
            fallback_qs, many=True, context={"request": request}
        ).data)
    
def defer_if_exists(qs, model, *field_names):
    """
    Safely defer fields only if they actually exist on the model.
    Prevents FieldDoesNotExist errors when models differ across environments.
    """
    existing = {f.name for f in model._meta.get_fields() if getattr(f, "attname", None)}
    to_defer = [name for name in field_names if name in existing]
    return qs.defer(*to_defer) if to_defer else qs


@method_decorator(cache_page(60), name="dispatch") 
class PublicProductBundleListView(ListAPIView):
    permission_classes = [permissions.AllowAny]
    serializer_class = ProductBundleSerializer

    def get_queryset(self):
        items_qs = (
            ProductBundleItem.objects
            .select_related("product__company__address__city__department__region",
                            "product__catalog_entry")
            .prefetch_related("product__images")
            .order_by("id")  
        )

        qs = (
            ProductBundle.objects
            .filter(is_active=True, status="published")
            .prefetch_related(Prefetch("items", queryset=items_qs))
            .order_by("-id")
        )

        try:
            qs = defer_if_exists(qs, ProductBundle, "long_description", "description")
        except Exception:
            pass

        return qs
        

class PublicBundlesPagination(LimitOffsetPagination):
    default_limit = 20
    max_limit = 100




@method_decorator(cache_page(60), name="dispatch")
class PublicBundlesView(ListAPIView):
    """
    """
    serializer_class = PublicBundleListSerializer
    pagination_class = PublicBundlesPagination

    def get_queryset(self):
        # Prefetch bundle items and hop to product -> company to avoid N+1
        items_qs = (
            ProductBundleItem.objects
            .select_related(
                "product",
                "product__company",
                "product__catalog_entry",
            )
        )

        return (
            ProductBundle.objects
            .filter(is_active=True, stock__gt=0)
            # DO NOT select_related("company", ...) because bundle has no such field
            .prefetch_related(
                Prefetch("items", queryset=items_qs),
            )
            # Safe defaults so serializer never sees nulls for rating counters
            .annotate(
                avg_rating_safe=Coalesce("avg_rating", Value(0.0), output_field=FloatField()),
                ratings_count_safe=Coalesce("ratings_count", Value(0), output_field=IntegerField()),
            )
            .order_by("-id")
        )

    


class CreatePayPalOrderView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        auth = (settings.PAYPAL_CLIENT_ID, settings.PAYPAL_SECRET)

        token_response = requests.post(
            f"https://api-m.{settings.PAYPAL_ENV}.paypal.com/v1/oauth2/token",
            auth=auth,
            data={'grant_type': 'client_credentials'}
        )
        access_token = token_response.json()['access_token']

        order_data = {
            "intent": "CAPTURE",
            "purchase_units": [{
                "amount": {
                    "currency_code": "EUR",
                    "value": str(request.data.get("amount", "0.01"))
                }
            }]
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }

        order_response = requests.post(
            f"https://api-m.{settings.PAYPAL_ENV}.paypal.com/v2/checkout/orders",
            headers=headers,
            json=order_data
        )

        return Response(order_response.json())
    

class AboutSectionViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = AboutSection.objects.all()
    serializer_class = AboutSectionSerializer

class CoreValueViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = CoreValue.objects.all()
    serializer_class = CoreValueSerializer

class LegalInformationViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = LegalInformation.objects.all()
    serializer_class = LegalInformationSerializer

class SiteSettingViewSet(viewsets.ModelViewSet):
    queryset = SiteSetting.objects.all()
    serializer_class = SiteSettingSerializer
    permission_classes = [IsAdminUser]


