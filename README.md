# 🚗 Road Accidents Analysis Pipeline — Spark & Scala

🇫🇷 Pipeline de traitement et d'analyse de données sur les accidents de la route, développé avec **Apache Spark** et **Scala**.

🇬🇧 Data processing and analysis pipeline on road accidents, built with **Apache Spark** and **Scala**.

---

## 🎯 Objectif / Objective

🇫🇷 Traiter et analyser un dataset d'accidents de la route afin d'en extraire des indicateurs clés : profil des conducteurs impliqués, heures à risque, causes des accidents graves.

🇬🇧 Process and analyze a road accidents dataset to extract key indicators: driver profiles, high-risk hours, and causes of serious accidents.

---

## ⚙️ Stack technique / Tech Stack

- **Apache Spark** — traitement distribué / distributed processing
- **Scala** — langage principal / main language
- **Spark SQL** — requêtes analytiques et fonctions de fenêtrage / analytical queries and window functions
- **UDF** (User Defined Functions) — recodage de la gravité / severity recoding

---

## 🔄 Pipeline

```
cleaned.csv (données brutes / raw data)
      ↓
Nettoyage / Cleaning (types, doublons / duplicates, valeurs manquantes / missing values)
      ↓
Transformation (extraction heure / hour extraction, normalisation, recodage / recoding)
      ↓
Agrégations & KPIs métier / Business KPIs
      ↓
Export CSV (output/)
```

---

## 📊 Analyses réalisées / Analyses performed

🇫🇷
- Nombre total d'accidents et détection des doublons
- Pourcentage d'accidents impliquant des conducteurs masculins
- Répartition des accidents par tranche d'âge
- Niveau d'éducation le plus courant parmi les conducteurs accidentés
- Accidents par catégorie de conducteur (owner, employee...)
- Heure de la journée avec le plus d'accidents (Spark SQL + Window Function)
- Principales causes des accidents très graves (UDF + agrégation)
- Répartition des accidents par sexe en pourcentage

🇬🇧
- Total number of accidents and duplicate detection
- Percentage of accidents involving male drivers
- Accident distribution by age group
- Most common education level among drivers involved in accidents
- Accidents by driver category (owner, employee...)
- Hour of the day with the most accidents (Spark SQL + Window Function)
- Main causes of very serious accidents (UDF + aggregation)
- Accident distribution by gender in percentage

---

## 📁 Structure du projet / Project structure

```
road-accidents-pipeline/
├── main.scala          ← pipeline complet / full pipeline
├── cleaned.csv         ← dataset source
├── output/
│   ├── accidents_par_heure.csv
│   ├── accidents_par_sexe.csv
│   └── causes_accidents_graves.csv
└── README.md
```

---

## 🚀 Lancer le projet / Run the project

```bash
sbt run
```

---

## 📂 Dataset

Dataset public sur les accidents de la route / Public road accidents dataset — [Kaggle](https://www.kaggle.com)

---

## 👤 Auteur / Author

**Fatou Bintou GASSAMA** — Data Engineer & Data Analyst
📧 fatou.b.gassama@gmail.com
🔗 [LinkedIn](https://www.linkedin.com/in/fatoubintougassama)
🐙 [GitHub](https://github.com/Gashu99)
