# 🐦 Tweet Sentiment Analysis — Hadoop Ecosystem

Système d'analyse de sentiment des tweets avec l'écosystème Hadoop.
**Aucune API Twitter requise** — utilise des datasets CSV locaux.

---

## 📁 Structure du projet

```
tweet-sentiment-hadoop/
├── data/
│   ├── generate_dataset.py     ← Génère un dataset CSV de test
│   └── tweets.csv              ← Dataset généré (3000 tweets)
├── ingestion/
│   ├── load_to_hdfs.py         ← Upload CSV vers HDFS
│   └── kafka_producer.py       ← Simule un flux Kafka depuis CSV
├── preprocessing/
│   └── preprocess.py           ← Nettoyage Spark (NLP)
├── ml/
│   ├── train_model.py          ← Entraînement Naive Bayes + LR (scikit-learn)
│   └── spark_train.py          ← Entraînement distribué Spark MLlib
├── streaming/
│   └── spark_streaming.py      ← Consommateur Kafka + prédiction temps réel
├── visualization/
│   └── dashboard.py            ← Dashboard Streamlit interactif
├── notebooks/
│   └── exploration.ipynb       ← Analyse exploratoire
├── config/
│   └── config.yaml             ← Configuration centralisée
├── docker-compose.yml          ← Stack complète (Hadoop + Kafka + Spark)
├── hadoop.env                  ← Variables Hadoop pour Docker
├── Dockerfile                  ← Image pour le dashboard
└── requirements.txt
```

---

## 🚀 Démarrage rapide (sans Hadoop/Kafka)

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Générer le dataset
```bash
python data/generate_dataset.py
```
> Pour utiliser un vrai dataset :
> - **Sentiment140** : https://www.kaggle.com/datasets/kazanova/sentiment140
> - **Twitter Airline Sentiment** : https://www.kaggle.com/datasets/crowdflower/twitter-airline-sentiment
>
> Renommez le fichier `tweets.csv` avec les colonnes : `id`, `text`, `sentiment`

### 3. Entraîner les modèles ML
```bash
python ml/train_model.py
```
Les modèles et graphiques sont sauvegardés dans `ml/models/`.

### 4. Lancer le dashboard
```bash
streamlit run visualization/dashboard.py
```
Ouvrez → http://localhost:8501

### 5. (Optionnel) Explorer avec Jupyter
```bash
jupyter notebook notebooks/exploration.ipynb
```

---

## 🐳 Démarrage avec Docker (stack complète)

```bash
# Lancer tous les services
docker-compose up -d

# Vérifier les conteneurs
docker-compose ps
```

| Service        | URL                         |
|----------------|-----------------------------|
| HDFS Web UI    | http://localhost:9870        |
| Spark Master   | http://localhost:8080        |
| Dashboard      | http://localhost:8501        |
| Kafka          | localhost:9092               |

---

## 📊 Pipeline de données

```
Dataset CSV
    │
    ├─► [Batch]   load_to_hdfs.py   → HDFS (Raw Zone)
    │                ↓
    │            preprocess.py      → HDFS (Clean Zone)
    │                ↓
    │            spark_train.py     → Modèles ML
    │
    └─► [Stream]  kafka_producer.py → Kafka Topic
                     ↓
                 spark_streaming.py → Prédictions temps réel

Dashboard Streamlit ← Résultats
```

---

## ⚙️ Configuration

Modifiez `config/config.yaml` pour adapter :
- URLs HDFS / Kafka
- Paramètres ML (test_size, max_features…)
- Intervalles Spark Streaming

---

## 📈 Modèles disponibles

| Modèle              | Bibliothèque  | Fichier           |
|---------------------|---------------|-------------------|
| Naive Bayes         | scikit-learn  | train_model.py    |
| Logistic Regression | scikit-learn  | train_model.py    |
| Naive Bayes         | Spark MLlib   | spark_train.py    |
| Logistic Regression | Spark MLlib   | spark_train.py    |

---

## 🔧 Commandes utiles

```bash
# Prétraitement Spark (local)
python preprocessing/preprocess.py --input data/tweets.csv --output data/tweets_clean.parquet

# Upload vers HDFS (Hadoop requis)
python ingestion/load_to_hdfs.py --file data/tweets.csv

# Flux Kafka (Kafka requis)
python ingestion/kafka_producer.py --delay 0.5

# Spark Streaming (Kafka + modèle requis)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
    streaming/spark_streaming.py
```

---

## 📋 Livrables

- ✅ Code source complet
- ✅ Dataset de test inclus
- ✅ Modèles ML (Naive Bayes, Logistic Regression)
- ✅ Dashboard Streamlit interactif
- ✅ Pipeline Spark (batch + streaming)
- ✅ Stack Docker complète
