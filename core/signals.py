from django.db.models.signals import pre_save, post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model

from .emails import send_mailgun_email

User = get_user_model()

TRACKED_FIELDS = [
    "email", "first_name", "last_name", "public_display_name",
    "phone", "date_of_birth", "description_utilisateur",
]


@receiver(pre_save, sender=User)
def user_pre_save(sender, instance, **kwargs):
    if not instance.pk:
        return
    try:
        previous = sender.objects.get(pk=instance.pk)
    except sender.DoesNotExist:
        return
    changed = {}
    for f in TRACKED_FIELDS:
        old = getattr(previous, f, None)
        new = getattr(instance, f, None)
        if old != new:
            changed[f] = {"old": old, "new": new}
    instance._profile_changes = changed


@receiver(post_save, sender=User)
def user_post_save(sender, instance, created, **kwargs):
    if created:
        return
    changed = getattr(instance, "_profile_changes", None)
    if not changed:
        return

    if "email" in changed:
        old_email = changed["email"]["old"]
        new_email = changed["email"]["new"]
        try:
            if old_email:
                send_mailgun_email(
                    old_email,
                    "Your email was changed",
                    f"Your account email was changed to {new_email}. "
                    f"If this wasn't you, contact support."
                )
            if new_email:
                send_mailgun_email(
                    new_email,
                    "Email change confirmation",
                    "Your email address on the account was updated successfully."
                )
        except Exception:
            pass
        del changed["email"]

    if changed:
        try:
            lines = [
                f"- {k}: '{v['old']}' -> '{v['new']}'"
                for k, v in changed.items()
            ]
            body = "The following profile fields were updated:\n" + "\n".join(lines)
            send_mailgun_email(instance.email, "Your profile was updated", body)
        except Exception:
            pass

    # cleanup
    if hasattr(instance, "_profile_changes"):
        del instance._profile_changes
