# README — Projet PySpark Big Data E‑commerce

## Problématique
Ce notebook vise à analyser un dataset e‑commerce à grande échelle (plusieurs dizaines de millions d’événements utilisateurs) afin de répondre à des enjeux métiers clés :

- Comprendre le comportement des utilisateurs (navigation, panier, achats)
- Identifier les profils clients et leurs habitudes
- Mesurer la performance commerciale (conversion, revenue, AOV)
- Segmenter les clients pour le marketing ciblé
- Construire un système de recommandation produit
- Optimiser les traitements Big Data avec Spark

Le projet s’inscrit dans un contexte **Big Data analytics & Machine Learning distribué** avec PySpark.

---

## Architecture globale du traitement

Pipeline analytique complet :

1. Configuration de l’environnement Spark
2. Chargement et préparation des données e‑commerce
3. Nettoyage et transformation des données
4. Analyse exploratoire et métriques business
5. Segmentation clients (RFM & clustering)
6. Recommandation produits (ALS)
7. Optimisation des performances Spark
8. Construction d’indicateurs et dashboard

---

## Bibliothèques utilisées

### PySpark
- `SparkSession` : point d’entrée Spark
- `pyspark.sql.functions` : transformations SQL distribuées
- `pyspark.sql.types` : définition de schémas
- `Window` : fonctions analytiques

### Spark MLlib
- `VectorAssembler` : préparation des features
- `StandardScaler` : normalisation des variables
- `KMeans` : segmentation non supervisée
- `ALS` : système de recommandation collaboratif

### Data & Visualisation
- `pandas` : manipulation locale
- `matplotlib` / `seaborn` : visualisation

### Data sourcing
- `kaggle` : téléchargement datasets
- `findspark` : configuration Spark

---

## Données

Dataset : **E‑commerce behavior data (multi‑category store)**

Contenu principal :
- événements utilisateurs : view, cart, purchase
- produits et catégories
- prix
- sessions utilisateurs
- timestamps

Volume : plusieurs dizaines de millions de lignes.

---

## Étapes de traitement

### Configuration Spark
- Création de la session Spark
- Paramétrage mémoire, partitions et exécution adaptative

### Chargement des données
- Définition d’un schéma explicite
- Lecture distribuée du CSV
- Mise en cache mémoire

### Nettoyage
- Suppression des lignes invalides
- Gestion des valeurs nulles
- Filtrage prix et identifiants

### Feature Engineering
- Extraction temporelle (heure, jour, mois)
- Construction de la catégorie principale
- Création de métriques utilisateur

### Analyse exploratoire
- Statistiques descriptives
- Funnel de conversion
- Analyse comportementale

### Segmentation clients
#### RFM
- Recency
- Frequency
- Monetary

#### Clustering K‑Means
- préparation des features
- normalisation
- identification des segments utilisateurs

### Recommandation produit
- création de ratings implicites
- entraînement ALS
- évaluation RMSE
- génération recommandations

### Optimisation Spark
- repartitionnement
- benchmark requêtes
- réduction shuffle

### KPIs & Dashboard
- revenue total
- nombre commandes
- clients uniques
- AOV
- top produits
- visualisations

---

## Concepts clés mobilisés

### Big Data
- traitement distribué
- partitionnement
- exécution lazy vs eager

### Data Engineering
- schéma explicite
- pipeline ETL
- optimisation mémoire

### Machine Learning
- clustering non supervisé
- normalisation
- systèmes de recommandation

### Business Analytics
- conversion
- segmentation clients
- performance produits

---

## Optimisations utilisées

- cache des DataFrames
- filtrage précoce
- repartitionnement
- schémas explicites
- réduction du shuffle

---

## Résultats attendus

- compréhension du comportement client
- identification des segments à forte valeur
- amélioration du ciblage marketing
- recommandations produits personnalisées
- indicateurs décisionnels exploitables

---

## Cas d’usage

- marketing digital
- e‑commerce analytics
- personnalisation de l’expérience client
- data science appliquée
- formation Big Data / PySpark

---

## Conclusion

Ce projet démontre la mise en œuvre d’un **pipeline complet Big Data + Machine Learning** avec PySpark pour résoudre une problématique réelle d’analyse e‑commerce.

Il combine :
- ingénierie des données
- analytics métier
- intelligence artificielle
- optimisation distribuée

L’objectif final est de transformer des données massives en **insights actionnables et systèmes intelligents de recommandation**.

