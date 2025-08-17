from core.models import ProductCategory

ProductCategory.objects.all().delete()

categories = [
    ("fruits", "Fruits frais"),
    ("vegetables", "Légumes frais"),
    ("dairy", "Produits laitiers"),
    ("meat", "Viandes"),
    ("fish", "Poissons"),
    ("eggs", "Œufs"),
    ("bakery", "Boulangerie / Pâtisserie"),
    ("drinks", "Boissons"),
    ("groceries", "Épicerie salée"),
    ("sweets", "Épicerie sucrée"),
    ("frozen", "Produits surgelés"),
    ("vegan", "Produits vegan / végétariens"),
    ("snacks", "Snacks et apéritifs"),
    ("spices", "Épices et condiments"),
    ("baby", "Produits pour bébé"),
    ("hygiene", "Hygiène & beauté"),
    ("household", "Produits ménagers"),
    ("pet", "Produits pour animaux"),
    ("others", "Autres"),
]

for code, label in categories:
    ProductCategory.objects.get_or_create(code=code, defaults={"label": label})


from core.models import ProductCatalog, ProductCategory

ProductCatalog.objects.all().delete()
catalog_items = [
    # Fruits
    ("Pomme", "fruits", "A", "Température ambiante"),
    ("Poire", "fruits", "A", "Température ambiante"),
    ("Fraise", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Framboise", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Myrtille", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Groseille", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Cassis", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Cerise", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Abricot", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Pêche", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Nectarine", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Melon Charentais", "fruits", "B", "Température ambiante"),
    ("Pastèque", "fruits", "B", "Température ambiante"),
    ("Figue", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Prune", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Raisin Italia", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Raisin Muscat", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Coing", "fruits", "B", "Température ambiante"),
    ("Châtaigne", "fruits", "A", "Sec"),
    ("Noix", "fruits", "A", "Sec"),
    ("Amande fraîche", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Pomme Reinettes", "fruits", "A", "Température ambiante"),
    ("Pomme Fuji", "fruits", "A", "Température ambiante"),
    ("Pomme Pink Lady", "fruits", "B", "Température ambiante"),
    ("Poire Conférence", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Poire Comice", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Kaki", "fruits", "B", "Température ambiante"),
    ("Mangue (locale)", "fruits", "C", "Réfrigéré (4-8°C)"),
    ("Banane (bio de Guadeloupe)", "fruits", "B", "Température ambiante"),
    ("Litchi (Réunion)", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Grenade", "fruits", "B", "Température ambiante"),
    ("Citron", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Orange", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Clémentine", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Pamplemousse", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Kiwi", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Tomate cerise", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Physalis", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Rhubarbe", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Pomelo", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Ananas (bio Réunion)", "fruits", "C", "Réfrigéré (4-8°C)"),
    ("Baies de sureau", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Nashi", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Mirabelle", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Quetsche", "fruits", "A", "Réfrigéré (4-8°C)"),
    ("Sorbe", "fruits", "B", "Sec"),
    ("Autre fruit rouge", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Autre agrume", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Autre fruit tropical", "fruits", "B", "Réfrigéré (4-8°C)"),
    ("Autre fruit", "fruits", "B", "Selon le produit"),

    # Vegetables
    ("Carotte", "vegetables", "A", "Frais (0-4°C)"),
    ("Betterave", "vegetables", "A", "Frais (0-4°C)"),
    ("Radis", "vegetables", "A", "Frais (0-4°C)"),
    ("Navet", "vegetables", "A", "Frais (0-4°C)"),
    ("Topinambour", "vegetables", "A", "Frais (0-4°C)"),
    ("Pomme de terre", "vegetables", "A", "Température ambiante"),
    ("Oignon jaune", "vegetables", "A", "Sec"),
    ("Oignon rouge", "vegetables", "A", "Sec"),
    ("Ail", "vegetables", "A", "Sec"),
    ("Échalote", "vegetables", "A", "Sec"),
    ("Poireau", "vegetables", "A", "Frais (0-4°C)"),
    ("Chou blanc", "vegetables", "A", "Frais (0-4°C)"),
    ("Chou rouge", "vegetables", "A", "Frais (0-4°C)"),
    ("Chou frisé", "vegetables", "A", "Frais (0-4°C)"),
    ("Chou de Bruxelles", "vegetables", "A", "Frais (0-4°C)"),
    ("Céleri rave", "vegetables", "A", "Frais (0-4°C)"),
    ("Céleri branche", "vegetables", "A", "Frais (0-4°C)"),
    ("Courgette ronde", "vegetables", "A", "Frais (0-4°C)"),
    ("Courge butternut", "vegetables", "A", "Température ambiante"),
    ("Potimarron", "vegetables", "A", "Température ambiante"),
    ("Citrouille", "vegetables", "A", "Température ambiante"),
    ("Salade feuille de chêne", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Salade batavia", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Mâche", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Roquette", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Épinard", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Blette", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Haricot vert", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Petit pois", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Fève", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Pois gourmand", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Artichaut", "vegetables", "B", "Réfrigéré (4-8°C)"),
    ("Asperge verte", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Asperge blanche", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Fenouil", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Concombre", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Cornichon", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Poivron vert", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Aubergine", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Tomate", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Cresson", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Chicorée", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Endive", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Panais", "vegetables", "A", "Frais (0-4°C)"),
    ("Rutabaga", "vegetables", "A", "Frais (0-4°C)"),
    ("Salsifis", "vegetables", "A", "Frais (0-4°C)"),
    ("Champignon de Paris", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Pleurote", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Shiitake", "vegetables", "A", "Réfrigéré (4-8°C)"),
    ("Autre légume", "vegetables", "B", "Selon le produit"),

    # Dairy
    ("Lait cru fermier", "dairy", "B", "Réfrigéré (4-8°C)"),
    ("Fromage de chèvre", "dairy", "B", "Réfrigéré (4-8°C)"),
    ("Yaourt fermier nature", "dairy", "A", "Réfrigéré (4-8°C)"),
    ("Beurre demi-sel", "dairy", "C", "Réfrigéré (4-8°C)"),
    ("Crème fraîche épaisse", "dairy", "C", "Réfrigéré (4-8°C)"),
    ("Autre produit laitier", "dairy", "B", "Selon le produit"),

    # Meat
    ("Poulet fermier", "meat", "C", "Réfrigéré (4-8°C)"),
    ("Saucisse artisanale", "meat", "D", "Réfrigéré (4-8°C)"),
    ("Steak haché de bœuf", "meat", "D", "Réfrigéré (4-8°C)"),
    ("Magret de canard", "meat", "D", "Réfrigéré (4-8°C)"),
    ("Rôti de porc", "meat", "C", "Réfrigéré (4-8°C)"),
    ("Autre viande", "meat", "C", "Selon le produit"),

    # Fish
    ("Truite fumée", "fish", "C", "Réfrigéré (4-8°C)"),
    ("Filet de bar", "fish", "C", "Réfrigéré (4-8°C)"),
    ("Saumon d’élevage local", "fish", "D", "Réfrigéré (4-8°C)"),
    ("Autre poisson", "fish", "C", "Selon le produit"),

    # Eggs
    ("Œufs plein air x6", "eggs", "A", "Température ambiante"),
    ("Œufs bio x6", "eggs", "A", "Température ambiante"),
    ("Autre œuf", "eggs", "A", "Selon le produit"),

    # Bakery
    ("Pain au levain", "bakery", "B", "Température ambiante"),
    ("Baguette tradition", "bakery", "B", "Température ambiante"),
    ("Brioche maison", "bakery", "C", "Température ambiante"),
    ("Tarte aux pommes", "bakery", "D", "Réfrigéré (4-8°C)"),
    ("Autre produit de boulangerie", "bakery", "B", "Selon le produit"),

    # Drinks
    ("Jus de pomme artisanal", "drinks", "B", "Température ambiante"),
    ("Cidre brut", "drinks", "C", "Température ambiante"),
    ("Kéfir maison", "drinks", "A", "Réfrigéré (4-8°C)"),
    ("Bière locale blonde", "drinks", "D", "Température ambiante"),
    ("Autre boisson", "drinks", "B", "Selon le produit"),

    # Groceries
    ("Pâtes artisanales", "groceries", "A", "Sec"),
    ("Lentilles vertes", "groceries", "A", "Sec"),
    ("Pois chiches secs", "groceries", "A", "Sec"),
    ("Farine de blé T80", "groceries", "A", "Sec"),
    ("Autre produit sec", "groceries", "B", "Selon le produit"),

    # Sweets
    ("Confiture de fraise maison", "sweets", "C", "Température ambiante"),
    ("Miel d’acacia", "sweets", "B", "Température ambiante"),
    ("Pâte à tartiner maison", "sweets", "D", "Température ambiante"),
    ("Biscuits sablés", "sweets", "D", "Température ambiante"),
    ("Autre sucrerie", "sweets", "C", "Selon le produit"),

    # Frozen
    ("Sorbets artisanaux", "frozen", "D", "Surgelé (-18°C)"),
    ("Légumes congelés bio", "frozen", "A", "Surgelé (-18°C)"),
    ("Autre produit surgelé", "frozen", "C", "Surgelé (-18°C)"),

    # Vegan
    ("Tofu fumé local", "vegan", "A", "Réfrigéré (4-8°C)"),
    ("Galettes végétales maison", "vegan", "A", "Réfrigéré (4-8°C)"),
    ("Seitan bio", "vegan", "B", "Réfrigéré (4-8°C)"),
    ("Autre produit vegan", "vegan", "B", "Selon le produit"),

    # Snacks
    ("Noix de cajou grillées", "snacks", "C", "Sec"),
    ("Chips artisanales", "snacks", "D", "Sec"),
    ("Barres de céréales", "snacks", "C", "Sec"),
    ("Autre snack", "snacks", "C", "Selon le produit"),

    # Spices
    ("Herbes de Provence", "spices", "A", "Sec"),
    ("Sel marin", "spices", "A", "Sec"),
    ("Poivre noir moulu", "spices", "A", "Sec"),
    ("Autre épice", "spices", "A", "Selon le produit"),

    # Baby
    ("Purée bio pomme-carotte", "baby", "A", "Température ambiante"),
    ("Compote sans sucre", "baby", "A", "Température ambiante"),
    ("Biscuit bébé bio", "baby", "B", "Sec"),
    ("Autre produit bébé", "baby", "A", "Selon le produit"),

    # Hygiene
    ("Savon surgras", "hygiene", "A", "Sec"),
    ("Dentifrice solide", "hygiene", "A", "Sec"),
    ("Shampoing solide", "hygiene", "A", "Sec"),
    ("Autre produit d’hygiène", "hygiene", "A", "Selon le produit"),

    # Household
    ("Lessive naturelle", "household", "B", "Sec"),
    ("Liquide vaisselle écolo", "household", "B", "Sec"),
    ("Nettoyant multi-usage", "household", "C", "Sec"),
    ("Autre produit ménager", "household", "B", "Selon le produit"),

    # Pet
    ("Croquettes chien bio", "pet", "C", "Sec"),
    ("Friandises pour chats", "pet", "C", "Sec"),
    ("Autre produit animalier", "pet", "C", "Selon le produit"),

    # Others
    ("Produit local divers", "others", "B", "Selon le produit"),
]


for name, category_code, eco_score, storage in catalog_items:
    category = ProductCategory.objects.get(code=category_code)
    ProductCatalog.objects.get_or_create(
        name=name,
        category=category,
        defaults={
            "eco_score": eco_score,
            "storage_instructions": storage
        }
    )

