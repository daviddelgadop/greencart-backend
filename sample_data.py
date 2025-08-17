
from core.models import CustomUser, Category, Product, Order, OrderItem, Article
from django.utils import timezone

# Nettoyage pour éviter doublons si relancé plusieurs fois
CustomUser.objects.all().delete()
Category.objects.all().delete()
Product.objects.all().delete()
Order.objects.all().delete()
OrderItem.objects.all().delete()
Article.objects.all().delete()

# Création d'utilisateurs
producer = CustomUser.objects.create_user(
    username='producer1',
    email='producer1@example.com',
    password='test1234',
    is_producer=True
)

client = CustomUser.objects.create_user(
    username='client1',
    email='client1@example.com',
    password='test1234',
    is_client=True
)

# Création de catégories
fruits = Category.objects.create(name='Fruits', description='Fruits frais')
legumes = Category.objects.create(name='Légumes', description='Légumes locaux')

# Création de produits
pommes = Product.objects.create(
    producer=producer,
    name='Pommes Bio',
    description='Pommes rouges biologiques et locales',
    price=2.99,
    stock=50,
    category=fruits
)

carottes = Product.objects.create(
    producer=producer,
    name='Carottes',
    description='Carottes croquantes issues de l’agriculture raisonnée',
    price=1.49,
    stock=80,
    category=legumes
)

# Création de commande
commande = Order.objects.create(client=client, is_paid=True)

OrderItem.objects.create(
    order=commande,
    product=pommes,
    quantity=3,
    price_at_purchase=pommes.price
)

OrderItem.objects.create(
    order=commande,
    product=carottes,
    quantity=2,
    price_at_purchase=carottes.price
)

# Création d’un article de blog
Article.objects.create(
    title='Comment bien conserver ses fruits ?',
    content='Voici nos astuces pour garder vos fruits frais plus longtemps...',
    author=producer,
    created_at=timezone.now()
)

print("✅ Données de démonstration créées avec succès.")
