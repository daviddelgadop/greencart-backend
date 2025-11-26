from __future__ import annotations

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import PasswordResetTokenGenerator, default_token_generator
from django.utils.encoding import force_bytes, force_str
from django.utils.http import urlsafe_base64_encode, urlsafe_base64_decode

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework import status
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework.exceptions import AuthenticationFailed

from .serializers import CustomUserSerializer
from .emails import send_mailgun_email, build_frontend_url

User = get_user_model()


def _flag(val) -> bool:
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "on")


EMAIL_VERIFICATION_ENABLED: bool = _flag(getattr(settings, "EMAIL_VERIFICATION_ENABLED", True))


class EmailVerificationTokenGenerator(PasswordResetTokenGenerator):
    def _make_hash_value(self, user, timestamp):
        return f"{user.pk}{user.is_active}{timestamp}"


email_verification_token_generator = EmailVerificationTokenGenerator()


class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    username_field = User.EMAIL_FIELD

    def validate(self, attrs):
        data = super().validate(attrs)
        if EMAIL_VERIFICATION_ENABLED and not self.user.is_active:
            raise AuthenticationFailed(
                "Account is not verified. Please check your email.",
                code="not_verified",
            )
        data["user"] = {
            "id": self.user.id,
            "email": self.user.email,
            "first_name": self.user.first_name,
            "last_name": self.user.last_name,
            "type": getattr(self.user, "type", None),
        }
        return data


class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer


class RegisterUserView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        serializer = CustomUserSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        user = serializer.save()

        if EMAIL_VERIFICATION_ENABLED:
            if user.is_active:
                user.is_active = False
                user.save(update_fields=["is_active"])
            ok, payload = self._send_verification(user)
            if ok:
                return Response(
                    {"message": "Account created. Check your email to verify your account."},
                    status=status.HTTP_201_CREATED,
                )
            return Response(
                {
                    "message": "Account created, but we couldn't send the verification email. "
                               "Use the resend endpoint or contact support.",
                    "email_error": payload,
                },
                status=status.HTTP_201_CREATED,
            )
        else:
            if not user.is_active:
                user.is_active = True
                user.save(update_fields=["is_active"])
            return Response(
                {"message": "Account created and activated."},
                status=status.HTTP_201_CREATED,
            )

    def _send_verification(self, user):
        uid = urlsafe_base64_encode(force_bytes(user.pk))
        token = email_verification_token_generator.make_token(user)
        verify_url = build_frontend_url("/verify-email", {"uid": uid, "token": token})
        subject = "Verify your account"
        text = (
            f"Hi {user.first_name or ''},\n\n"
            f"Please verify your account by clicking the link below:\n{verify_url}\n\n"
            f"If you did not create an account, you can ignore this email."
        )
        return send_mailgun_email(user.email, subject, text)


class VerifyEmailView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        if not EMAIL_VERIFICATION_ENABLED:
            return Response({"detail": "Email verification is disabled."}, status=200)

        uid = request.data.get("uid")
        token = request.data.get("token")
        if not uid or not token:
            return Response({"detail": "uid and token are required."}, status=400)

        try:
            uid_int = force_str(urlsafe_base64_decode(uid))
            user = User.objects.get(pk=uid_int)
        except Exception:
            return Response({"detail": "Invalid verification link."}, status=400)

        if not email_verification_token_generator.check_token(user, token):
            return Response({"detail": "Invalid or expired verification token."}, status=400)

        if not user.is_active:
            user.is_active = True
            user.save(update_fields=["is_active"])

        return Response({"detail": "Email verified successfully."}, status=200)


class ResendVerificationEmailView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        if not EMAIL_VERIFICATION_ENABLED:
            return Response({"detail": "Email verification is disabled."}, status=200)

        email = request.data.get("email")
        if not email:
            return Response({"detail": "Email is required."}, status=400)

        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            return Response(
                {"detail": "If the account exists, a verification email has been sent."},
                status=200,
            )

        if user.is_active:
            return Response({"detail": "Account already verified."}, status=200)

        ok, _ = RegisterUserView()._send_verification(user)
        if ok:
            return Response({"detail": "Verification email sent."}, status=200)
        return Response(
            {"detail": "We couldn't send the verification email. Try again later."},
            status=503,
        )


class PasswordResetRequestView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        email = request.data.get("email")
        if not email:
            return Response({"detail": "Email is required."}, status=400)

        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            return Response(
                {"detail": "If the email exists, a reset link has been sent."},
                status=200,
            )

        uid = urlsafe_base64_encode(force_bytes(user.pk))
        token = default_token_generator.make_token(user)
        reset_url = build_frontend_url("/reset-password", {"uid": uid, "token": token})

        subject = "Reset your password"
        text = (
            "Hello,\n\nWe received a request to reset your password. "
            f"Use the link below to set a new password:\n{reset_url}\n\n"
            "If you did not request this, you can ignore this email."
        )
        send_mailgun_email(user.email, subject, text)
        return Response(
            {"detail": "If the email exists, a reset link has been sent."}, status=200
        )


class PasswordResetConfirmView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        uid = request.data.get("uid")
        token = request.data.get("token")
        new_password = request.data.get("new_password")
        confirm_password = request.data.get("confirm_password")

        if not all([uid, token, new_password, confirm_password]):
            return Response(
                {"detail": "uid, token, new_password, and confirm_password are required."},
                status=400,
            )
        if new_password != confirm_password:
            return Response({"detail": "Passwords do not match."}, status=400)

        try:
            uid_int = force_str(urlsafe_base64_decode(uid))
            user = User.objects.get(pk=uid_int)
        except Exception:
            return Response({"detail": "Invalid reset link."}, status=400)

        if not default_token_generator.check_token(user, token):
            return Response({"detail": "Invalid or expired token."}, status=400)

        user.set_password(new_password)
        user.save(update_fields=["password"])
        send_mailgun_email(
            user.email,
            "Your password was changed",
            "Your password was just changed. If this wasn't you, contact support immediately.",
        )
        return Response({"detail": "Password updated successfully."}, status=200)


class ChangePasswordView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request):
        current_password = request.data.get("current_password")
        new_password = request.data.get("new_password")
        confirm_password = request.data.get("confirm_password")

        if not all([current_password, new_password, confirm_password]):
            return Response(
                {"detail": "current_password, new_password, and confirm_password are required."},
                status=400,
            )
        if new_password != confirm_password:
            return Response({"detail": "Passwords do not match."}, status=400)
        if not request.user.check_password(current_password):
            return Response({"detail": "Current password is incorrect."}, status=400)

        request.user.set_password(new_password)
        request.user.save(update_fields=["password"])
        send_mailgun_email(
            request.user.email,
            "Your password was changed",
            "Your password was just changed. If this wasn't you, contact support immediately.",
        )
        return Response({"detail": "Password updated successfully."}, status=200)
