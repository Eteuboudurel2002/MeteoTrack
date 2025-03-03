# MeteoTrack

## 📖 Description  
Ce projet vise à **collecter automatiquement des données météorologiques** à partir de l’API **OpenWeather**, à les **nettoyer, stocker dans une base PostgreSQL**, et à les **orchestrer avec Apache Airflow**.  

L’objectif est d’aider une entreprise du **secteur du tourisme** à optimiser ses recommandations en fonction des conditions climatiques de différentes destinations.

---

## 🚀 Fonctionnalités
✅ Connexion à l’API **OpenWeather** pour récupérer les données météo  
✅ Nettoyage et transformation des données avant stockage  
✅ Stockage des données dans une base **PostgreSQL**  
✅ Orchestration des tâches avec **Apache Airflow**  
✅ Déploiement avec **Docker** pour une installation simplifiée  
✅ Analyse et comparaison de l’attractivité touristique via **l’ICT de Mieczkowski**  

---

## 🏗️ Technologies Utilisées
- **Python**
- **PostgreSQL**
- **Apache Airflow**  
- **Docker**  
- **Git**

---

## ⚙️ Installation et Configuration

### 🔹 1️⃣ Cloner le dépôt  
```bash
git clone https://github.com/Eteuboudurel2002/MeteoTrack.git
cd Meteotrack
```
### 🔹 2️⃣ Lancer les services Docker
```bash
docker-compose up -d
```
Cela démarre :
PostgreSQL et Airflow

###🔹 3️⃣ Accéder aux servises
- Airflow UI : http://localhost:8080 
- PostgreSQL : localhost:5432 (via PGAdmin ou un client SQL)


