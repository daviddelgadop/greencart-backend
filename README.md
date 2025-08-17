# GreenCart 🛒🌿

Plateforme e-commerce responsable en circuit court pour la vente de produits alimentaires revalorisés et locaux.

## 🌐 Technologies

- **Frontend** : React + TypeScript + Vite + TailwindCSS
- **Backend** : Django + Django REST Framework
- **Base de données** : SQLite (dev) / PostgreSQL (prod)
- **Authentification** : JWT


### 🧩 Dépendances générales

#### Frontend

```bash
cd greencart-frontend
npm install
npm install axios
npm install react-toastify
npm install --save-dev @types/react-toastify
npm run dev
```

#### Backend

```bash
cd greencart-backend
python -m venv env
env\Scripts\activate           # Windows
# source env/bin/activate     # Linux/macOS
pip install -r requirements.txt

pip install django-cors-headers
pip install djangorestframework-simplejwt
```


## 📝 TODO

- [ ] Gestion des utilisateurs
- [ ] Intégration paiement (Stripe)
- [ ] Interface admin
- [ ] Tableau de bord écologique

---

© 2025 GreenCart – Tous droits réservés.
