"""
Text Processor — NLP utilities using BERT Transformer models.

Models used:
    - Sentiment: nlptown/bert-base-multilingual-uncased-sentiment (1-5 stars, 100+ languages)
    - Emotion:   j-hartmann/emotion-english-distilroberta-base (joy/anger/sadness/fear/surprise/disgust)
    - Toxicity:  unitary/toxic-bert (toxic / not toxic)
    - Embeddings: sentence-transformers/all-MiniLM-L6-v2 (384-dim dense vectors)
    - Topics:    BERTopic (clustering on embeddings)

All models run on GPU if available (RTX 5060 Ti).

Usage:
    from src.text_processor import TextAnalyzer
    analyzer = TextAnalyzer()
    results = analyzer.analyze_batch(texts)
"""
import logging
import re
from typing import Optional

import numpy as np
import torch

logger = logging.getLogger(__name__)

# ── Device ──────────────────────────────────────────────────────────────────
DEVICE = 0 if torch.cuda.is_available() else -1
DEVICE_NAME = "GPU" if DEVICE == 0 else "CPU"
logger.info("NLP device: %s", DEVICE_NAME)


# ── Text cleaning ───────────────────────────────────────────────────────────
def clean_text_for_nlp(text: str) -> str:
    """Clean text for NLP processing."""
    if not text or text in ("[deleted]", "[removed]"):
        return ""
    text = re.sub(r"https?://\S+", "", text)          # Remove URLs
    text = re.sub(r"\s+", " ", text).strip()           # Normalize whitespace
    return text


# ── PySpark UDFs for distributed sentiment ──────────────────────────────────
def create_sentiment_udf():
    """Create a PySpark UDF for BERT sentiment analysis."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import FloatType

    def _get_sentiment(text: str) -> float:
        """Convert BERT 1-5 stars to -1.0 to +1.0 score."""
        if not text or text.strip() == "":
            return 0.0
        try:
            from transformers import pipeline
            analyzer = pipeline(
                "sentiment-analysis",
                model="nlptown/bert-base-multilingual-uncased-sentiment",
                device=DEVICE,
                truncation=True,
                max_length=512,
            )
            result = analyzer(text[:512])[0]
            # Convert "1 star" -> -1.0, "3 stars" -> 0.0, "5 stars" -> +1.0
            stars = int(result["label"][0])
            return (stars - 3) / 2.0
        except Exception:
            return 0.0

    return udf(_get_sentiment, FloatType())


# ── Main TextAnalyzer class (batch processing on GPU) ───────────────────────
class TextAnalyzer:
    """
    Comprehensive text analyzer using BERT models on GPU.
    Performs: sentiment, emotion, toxicity, and embedding extraction.
    """

    def __init__(self):
        from transformers import pipeline
        from sentence_transformers import SentenceTransformer

        logger.info("Loading NLP models on %s...", DEVICE_NAME)

        # 1. Sentiment (multilingual, 1-5 stars)
        self.sentiment_model = pipeline(
            "sentiment-analysis",
            model="nlptown/bert-base-multilingual-uncased-sentiment",
            device=DEVICE,
            truncation=True,
            max_length=512,
            batch_size=32,
        )
        logger.info("  ✓ Sentiment model loaded")

        # 2. Emotion (joy, anger, sadness, fear, surprise, disgust)
        self.emotion_model = pipeline(
            "text-classification",
            model="j-hartmann/emotion-english-distilroberta-base",
            device=DEVICE,
            truncation=True,
            max_length=512,
            batch_size=32,
            top_k=None,  # Return all emotion scores
        )
        logger.info("  ✓ Emotion model loaded")

        # 3. Toxicity
        self.toxicity_model = pipeline(
            "text-classification",
            model="unitary/toxic-bert",
            device=DEVICE,
            truncation=True,
            max_length=512,
            batch_size=32,
        )
        logger.info("  ✓ Toxicity model loaded")

        # 4. Embeddings (for BERTopic + ML features)
        self.embedding_model = SentenceTransformer(
            "sentence-transformers/all-MiniLM-L6-v2",
            device="cuda" if DEVICE == 0 else "cpu",
        )
        logger.info("  ✓ Embedding model loaded (384-dim)")

        logger.info("All NLP models ready!")

    def _stars_to_score(self, label: str) -> float:
        """Convert '1 star' / '5 stars' to -1.0 / +1.0"""
        try:
            stars = int(label[0])
            return (stars - 3) / 2.0
        except (ValueError, IndexError):
            return 0.0

    def _score_to_label(self, score: float) -> str:
        """Convert score to positive/negative/neutral."""
        if score >= 0.25:
            return "positive"
        elif score <= -0.25:
            return "negative"
        return "neutral"

    def analyze_sentiment(self, texts: list[str]) -> list[dict]:
        """
        Analyze sentiment for a batch of texts.
        Returns: [{"score": float, "label": str, "stars": int}, ...]
        """
        cleaned = [clean_text_for_nlp(t) or "neutral" for t in texts]
        results = self.sentiment_model(cleaned)
        output = []
        for r in results:
            score = self._stars_to_score(r["label"])
            output.append({
                "sentiment_score": score,
                "sentiment_label": self._score_to_label(score),
                "sentiment_stars": int(r["label"][0]),
                "sentiment_confidence": r["score"],
            })
        return output

    def analyze_emotion(self, texts: list[str]) -> list[dict]:
        """
        Detect dominant emotion for a batch of texts.
        Returns: [{"dominant_emotion": str, "emotion_scores": dict}, ...]
        """
        cleaned = [clean_text_for_nlp(t) or "neutral" for t in texts]
        results = self.emotion_model(cleaned)
        output = []
        for r in results:
            # r is a list of dicts: [{"label": "anger", "score": 0.8}, ...]
            emotion_scores = {item["label"]: round(item["score"], 4) for item in r}
            dominant = max(r, key=lambda x: x["score"])
            output.append({
                "dominant_emotion": dominant["label"],
                "emotion_confidence": dominant["score"],
                **{f"emotion_{k}": v for k, v in emotion_scores.items()},
            })
        return output

    def analyze_toxicity(self, texts: list[str]) -> list[dict]:
        """
        Detect toxicity for a batch of texts.
        Returns: [{"toxicity_score": float, "is_toxic": bool}, ...]
        """
        cleaned = [clean_text_for_nlp(t) or "safe" for t in texts]
        results = self.toxicity_model(cleaned)
        output = []
        for r in results:
            score = r["score"] if r["label"] == "toxic" else 1.0 - r["score"]
            output.append({
                "toxicity_score": round(score, 4),
                "is_toxic": score > 0.5,
            })
        return output

    def get_embeddings(self, texts: list[str], batch_size: int = 64) -> np.ndarray:
        """
        Get dense embeddings (384-dim) for a batch of texts.
        Returns numpy array of shape (n_texts, 384).
        """
        cleaned = [clean_text_for_nlp(t) or "" for t in texts]
        embeddings = self.embedding_model.encode(
            cleaned,
            batch_size=batch_size,
            show_progress_bar=True,
            normalize_embeddings=True,
        )
        return embeddings

    def analyze_batch(self, texts: list[str]) -> list[dict]:
        """
        Run ALL analyses (sentiment + emotion + toxicity) on a batch.
        Returns list of combined result dicts.
        """
        logger.info("Analyzing %d texts...", len(texts))

        sentiments = self.analyze_sentiment(texts)
        emotions = self.analyze_emotion(texts)
        toxicities = self.analyze_toxicity(texts)

        combined = []
        for i in range(len(texts)):
            result = {}
            result.update(sentiments[i])
            result.update(emotions[i])
            result.update(toxicities[i])
            combined.append(result)

        logger.info("Analysis complete for %d texts", len(texts))
        return combined


# ── Subjectivity (TextBlob — lightweight) ───────────────────────────────────
def get_subjectivity(text: str) -> float:
    """
    Get subjectivity score (0.0 = objective/factual, 1.0 = subjective/opinion).
    Uses TextBlob (lightweight, no GPU needed).
    """
    if not text or text.strip() == "":
        return 0.0
    try:
        from textblob import TextBlob
        cleaned = clean_text_for_nlp(text)
        return TextBlob(cleaned).sentiment.subjectivity
    except Exception:
        return 0.0


# ── BERTopic (Topic Modeling with embeddings) ───────────────────────────────
def build_bertopic_model(
    texts: list[str],
    embeddings: Optional[np.ndarray] = None,
    nr_topics: int = 10,
    min_topic_size: int = 10,
):
    """
    Build a BERTopic model for topic discovery.

    Args:
        texts: List of text strings
        embeddings: Pre-computed embeddings (optional, will compute if None)
        nr_topics: Target number of topics (auto-reduce if needed)
        min_topic_size: Minimum documents per topic

    Returns:
        (model, topics, probs) — BERTopic model, topic assignments, probabilities
    """
    from bertopic import BERTopic

    logger.info("Building BERTopic model (nr_topics=%d)...", nr_topics)

    model = BERTopic(
        nr_topics=nr_topics,
        min_topic_size=min_topic_size,
        language="multilingual",
        verbose=True,
    )

    cleaned = [clean_text_for_nlp(t) or "empty" for t in texts]

    if embeddings is not None:
        topics, probs = model.fit_transform(cleaned, embeddings=embeddings)
    else:
        topics, probs = model.fit_transform(cleaned)

    topic_info = model.get_topic_info()
    logger.info("BERTopic found %d topics", len(topic_info) - 1)  # -1 for outlier topic

    return model, topics, probs


# ── Helper: add all NLP columns to a Pandas DataFrame ──────────────────────
def enrich_dataframe_with_nlp(df, text_column: str = "title", analyzer: TextAnalyzer = None):
    """
    Add all NLP features to a Pandas DataFrame.
    Adds: sentiment_score, sentiment_label, dominant_emotion, emotion_*,
          toxicity_score, is_toxic, subjectivity
    """
    import pandas as pd

    if analyzer is None:
        analyzer = TextAnalyzer()

    texts = df[text_column].fillna("").tolist()

    # Batch analysis (GPU)
    results = analyzer.analyze_batch(texts)
    nlp_df = pd.DataFrame(results)

    # Subjectivity (CPU — lightweight)
    nlp_df["subjectivity"] = [get_subjectivity(t) for t in texts]

    # Merge
    for col in nlp_df.columns:
        df[col] = nlp_df[col].values

    logger.info("Added %d NLP columns to DataFrame", len(nlp_df.columns))
    return df
