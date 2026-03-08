# 📊 Reddit Data Science Pipeline — Plan d'exécution

## Architecture finale

```
HDFS (Data Engineering)                    Data Science Cluster
┌──────────────────────┐                  ┌───────────────────────────┐
│ /posts/       (1.8M) │                  │ Spark Master + 2 Workers  │
│ /comments/    (TBD)  │  ◄── reads ───  │ Jupyter Lab (GPU)         │
│ /images/      (2.4G) │                  │   PySpark + PyTorch       │
│ /post_updates/ (TBD) │                  │   BERT + BERTopic         │
└──────────────────────┘                  └───────────────────────────┘
```

## 4 Targets ML

| Target | Nom | Type | Prédit |
|--------|-----|------|--------|
| T1 | popularity_class | Classification | Low/Medium/High/Viral |
| T2 | predicted_score | Régression | Score exact |
| T3 | predicted_comments | Régression | Num comments exact |
| T4 | dominant_emotion | Classification | Emotion dominante des comments |

---

## Étapes d'exécution (dans l'ordre)

### STEP 0 — Assurer que Data Engineering est running
```bash
cd c:\Users\youss\Desktop\reddit
docker compose ps
# Kafka + HDFS doivent être UP
# Si down: docker compose up -d
```

### STEP 1 — Fetch Comments + Updated Metrics
```bash
cd c:\Users\youss\Desktop\reddit

# Rebuild fetch-comments (code updated)
docker compose build fetch-comments

# Start Flink + HDFS Writer pour process les updates
docker compose start flink-processor hdfs-writer

# Run fetch-comments (manual — jib comments + updated score)
docker compose run --rm fetch-comments

# Attendre que tout soit écrit en HDFS
# (60-90 secondes après que fetch-comments termine)

# Vérifier les données
docker compose exec namenode hdfs dfs -du -h /data/reddit

# Graceful stop
docker compose stop flink-processor hdfs-writer
```

### STEP 2 — Build & Launch Data Science Cluster
```bash
cd c:\Users\youss\Desktop\reddit\data-science

# Build (première fois = ~10-15 min, PyTorch + BERT)
docker compose up -d --build

# Vérifier que tout est UP
docker compose ps

# Ouvrir dans le navigateur:
# Jupyter Lab: http://localhost:8888 (token: reddit)
# Spark UI:    http://localhost:8080
```

> Si pas de GPU NVIDIA: utiliser le profil CPU
> ```bash
> docker compose --profile cpu-only up -d --build jupyter-cpu spark-master spark-worker-1 spark-worker-2
> ```

### STEP 3 — Phase 1: Data Extraction (dans Jupyter)
Ouvrir `notebooks/01_data_extraction.ipynb`:
```python
from src.data_loader import *

spark = create_spark_session()

posts_df = load_posts(spark)
comments_df = load_comments(spark)
updates_df = load_post_updates(spark)

# Join posts + updated metrics
posts_full = join_posts_with_updates(posts_df, updates_df)

# Stats
posts_full.printSchema()
posts_full.count()
posts_full.show(5)
```

### STEP 4 — Phase 2: EDA (dans Jupyter)
Ouvrir `notebooks/02_eda.ipynb`:
```python
# Distributions, top subreddits, engagement analysis
# Media type breakdown, correlation heatmaps
# Score distribution, comment distribution
```

### STEP 5 — Phase 3A: NLP Analysis (dans Jupyter)
Ouvrir `notebooks/03_nlp_analysis.ipynb`:
```python
from src.text_processor import TextAnalyzer, build_bertopic_model

# Initialize (charge les modèles sur GPU)
analyzer = TextAnalyzer()

# Sentiment + Emotion + Toxicity pour posts
posts_pd = posts_full.toPandas()
titles = posts_pd["title"].tolist()

results = analyzer.analyze_batch(titles)
# → sentiment_score, dominant_emotion, toxicity_score pour chaque post

# Embeddings (pour ML + BERTopic)
embeddings = analyzer.get_embeddings(titles)
```

### STEP 6 — Phase 3B: Topic Modeling (dans Jupyter)
```python
from src.text_processor import build_bertopic_model

# BERTopic avec embeddings pré-calculés
model, topics, probs = build_bertopic_model(titles, embeddings=embeddings)

# Visualiser les topics
model.get_topic_info()
model.visualize_topics()
```

### STEP 7 — Phase 4: Image Analysis (dans Jupyter)
Ouvrir `notebooks/04_image_analysis.ipynb`:
```python
from src.data_loader import download_all_images
from src.image_processor import ImageClassifier

# Télécharger images de HDFS → container
image_paths = download_all_images(max_images=500)

# Classifier (GPU)
classifier = ImageClassifier()
results = classifier.classify_batch(image_paths)

# Deep features pour ML
image_features = classifier.extract_features(image_paths)
```

### STEP 8 — Phase 5: ML Training (dans Jupyter)
Ouvrir `notebooks/05_ml_prediction.ipynb`:
```python
from src.ml_pipeline import EngagementPipeline, prepare_features

# Préparer features (text + image + metadata)
df, feature_cols = prepare_features(
    posts_pd,
    text_embeddings=embeddings,
    image_embeddings=image_features,
)

# Train 4 models
pipeline = EngagementPipeline(df, feature_cols)
results = pipeline.train_all()

# Sauvegarder
pipeline.save_models()
```

### STEP 9 — Phase 6: Visualization (dans Jupyter)
Ouvrir `notebooks/06_visualisation.ipynb`:
```python
# Dashboard final:
# - Confusion matrices
# - Feature importance plots
# - ROC curves
# - Word clouds par topic
# - Engagement charts
```

---

## Fichiers modifiés

| Fichier | Modification |
|---------|-------------|
| `fetch-comments/fetch_comments.py` | + capture updated_score/ratio/comments → Kafka |
| `flink-processor/processor.py` | + processing post-updates topic |
| `hdfs-writer/writer.py` | + POST_UPDATE_SCHEMA + écriture post_updates |
| `docker-compose.yml` | + HDFS init post_updates directory |
| `data-science/jupyter/requirements.txt` | + transformers, sentence-transformers, bertopic |
| `data-science/src/text_processor.py` | VADER → BERT + Emotion + Toxicity + Embeddings |
| `data-science/src/ml_pipeline.py` | 4 targets (T1-T4) avec RF/GBT/XGBoost |
| `data-science/src/data_loader.py` | + load_post_updates + join_posts_with_updates |
