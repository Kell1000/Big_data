"""
ML Pipeline — Engagement Prediction with 4 Targets.

Targets:
    T1: popularity_class    — Classification (Low/Medium/High/Viral)
    T2: predicted_score     — Regression (exact score)
    T3: predicted_comments  — Regression (exact num_comments)
    T4: dominant_emotion    — Classification (joy/anger/sadness/fear/surprise/disgust)

All models use the SAME features (X), only the target (Y) changes.

Features combine:
    - Text embeddings (50 dims via PCA from BERT 384-dim)
    - Image embeddings (50 dims via PCA from ResNet50 2048-dim)
    - NLP scores (sentiment, emotion, toxicity, subjectivity)
    - Topic ID (from BERTopic)
    - Metadata (hour, day, subreddit, has_media, NSFW, etc.)

Usage:
    from src.ml_pipeline import EngagementPipeline
    pipeline = EngagementPipeline(df)
    results = pipeline.train_all()
"""
import logging
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (
    accuracy_score, f1_score, classification_report, confusion_matrix,
    mean_squared_error, mean_absolute_error, r2_score,
)
from sklearn.decomposition import PCA

logger = logging.getLogger(__name__)


# ── Target definitions ──────────────────────────────────────────────────────
def create_popularity_classes(scores: pd.Series) -> pd.Series:
    """T1: Convert scores to popularity classes."""
    bins = [-np.inf, 10, 100, 1000, np.inf]
    labels = ["Low", "Medium", "High", "Viral"]
    return pd.cut(scores, bins=bins, labels=labels).astype(str)


def create_discussion_levels(num_comments: pd.Series) -> pd.Series:
    """Helper: Convert num_comments to discussion levels (for analysis)."""
    bins = [-np.inf, 5, 50, np.inf]
    labels = ["Few", "Some", "Many"]
    return pd.cut(num_comments, bins=bins, labels=labels).astype(str)


# ── Feature preparation ────────────────────────────────────────────────────
def prepare_features(
    df: pd.DataFrame,
    text_embeddings: Optional[np.ndarray] = None,
    image_embeddings: Optional[np.ndarray] = None,
    n_components: int = 50,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Prepare all features for ML models.
    Returns (feature_df, feature_column_names).
    """
    logger.info("Preparing features...")
    feature_cols = []

    # ── Text embeddings (PCA reduction) ──
    if text_embeddings is not None and len(text_embeddings) > 0:
        n_comp = min(n_components, text_embeddings.shape[1], len(text_embeddings))
        pca_text = PCA(n_components=n_comp, random_state=42)
        text_pca = pca_text.fit_transform(text_embeddings)
        text_cols = [f"text_emb_{i}" for i in range(n_comp)]
        for i, col in enumerate(text_cols):
            df[col] = text_pca[:, i]
        feature_cols.extend(text_cols)
        logger.info("  Text embeddings: %d dims → %d (PCA %.1f%% variance)",
                     text_embeddings.shape[1], n_comp,
                     pca_text.explained_variance_ratio_.sum() * 100)

    # ── Image embeddings (PCA reduction) ──
    if image_embeddings is not None and len(image_embeddings) > 0:
        n_comp = min(n_components, image_embeddings.shape[1], len(image_embeddings))
        pca_img = PCA(n_components=n_comp, random_state=42)
        img_pca = pca_img.fit_transform(image_embeddings)
        img_cols = [f"img_emb_{i}" for i in range(n_comp)]
        for i, col in enumerate(img_cols):
            df[col] = img_pca[:, i]
        feature_cols.extend(img_cols)
        logger.info("  Image embeddings: %d dims → %d (PCA %.1f%% variance)",
                     image_embeddings.shape[1], n_comp,
                     pca_img.explained_variance_ratio_.sum() * 100)

    # ── NLP features ──
    nlp_cols = ["sentiment_score", "toxicity_score", "subjectivity",
                "emotion_joy", "emotion_anger", "emotion_sadness",
                "emotion_fear", "emotion_surprise", "emotion_disgust"]
    for col in nlp_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            feature_cols.append(col)

    # ── Text stats ──
    stat_cols = ["title_length", "text_length", "word_count"]
    for col in stat_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            feature_cols.append(col)

    # ── Metadata features ──
    if "post_hour" in df.columns:
        df["post_hour"] = pd.to_numeric(df["post_hour"], errors="coerce").fillna(12)
        feature_cols.append("post_hour")

    if "post_day_of_week" in df.columns:
        df["post_day_of_week"] = pd.to_numeric(df["post_day_of_week"], errors="coerce").fillna(3)
        feature_cols.append("post_day_of_week")

    bool_cols = ["has_media", "is_self", "over_18"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(float)
            feature_cols.append(col)

    if "image_count" in df.columns:
        df["image_count"] = pd.to_numeric(df["image_count"], errors="coerce").fillna(0)
        feature_cols.append("image_count")

    # ── Topic ID ──
    if "topic_id" in df.columns:
        df["topic_id"] = pd.to_numeric(df["topic_id"], errors="coerce").fillna(-1)
        feature_cols.append("topic_id")

    # ── Subreddit (encoded) ──
    if "subreddit" in df.columns:
        le = LabelEncoder()
        df["subreddit_encoded"] = le.fit_transform(df["subreddit"].fillna("unknown"))
        feature_cols.append("subreddit_encoded")

    logger.info("  Total features: %d", len(feature_cols))
    return df, feature_cols


# ── Model training for each target ──────────────────────────────────────────
class EngagementPipeline:
    """
    Train and evaluate 4 models for engagement prediction.
    Same features, different targets.
    """

    def __init__(self, df: pd.DataFrame, feature_cols: list[str], test_size: float = 0.2):
        self.df = df.copy()
        self.feature_cols = feature_cols
        self.test_size = test_size
        self.results = {}

    def _get_X(self):
        """Get feature matrix, handling NaN values."""
        X = self.df[self.feature_cols].copy()
        X = X.fillna(0)
        return X

    def train_t1_popularity(self) -> dict:
        """T1: Popularity Classification (Low/Medium/High/Viral)."""
        logger.info("=" * 50)
        logger.info("T1: Training Popularity Classifier...")
        logger.info("=" * 50)

        X = self._get_X()
        y = create_popularity_classes(self.df["updated_score"])

        # Remove NaN targets
        mask = y.notna()
        X, y = X[mask], y[mask]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=42, stratify=y
        )

        # Train models
        models = {
            "RandomForest": RandomForestClassifier(n_estimators=200, max_depth=15, random_state=42, n_jobs=-1),
            "GBT": GradientBoostingClassifier(n_estimators=150, max_depth=8, random_state=42),
        }

        best_model = None
        best_f1 = 0
        model_results = {}

        for name, model in models.items():
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            acc = accuracy_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred, average="weighted")

            model_results[name] = {"accuracy": acc, "f1": f1}
            logger.info("  %s — Accuracy: %.4f, F1: %.4f", name, acc, f1)

            if f1 > best_f1:
                best_f1 = f1
                best_model = model

        # Feature importance
        importances = best_model.feature_importances_
        feat_imp = sorted(zip(self.feature_cols, importances), key=lambda x: x[1], reverse=True)

        result = {
            "target": "T1_popularity",
            "type": "classification",
            "best_model": best_model,
            "model_comparison": model_results,
            "best_f1": best_f1,
            "feature_importance": feat_imp[:20],
            "classification_report": classification_report(y_test, best_model.predict(X_test)),
            "confusion_matrix": confusion_matrix(y_test, best_model.predict(X_test)),
            "classes": ["Low", "Medium", "High", "Viral"],
        }
        self.results["T1"] = result
        return result

    def train_t2_score_regression(self) -> dict:
        """T2: Score Regression (predict exact score)."""
        logger.info("=" * 50)
        logger.info("T2: Training Score Regressor...")
        logger.info("=" * 50)

        X = self._get_X()
        y = pd.to_numeric(self.df["updated_score"], errors="coerce").fillna(0)

        # Log transform for better distribution
        y_log = np.log1p(y)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y_log, test_size=self.test_size, random_state=42
        )

        # Use XGBoost if available, else RandomForest
        try:
            from xgboost import XGBRegressor
            model = XGBRegressor(
                n_estimators=200, max_depth=10, learning_rate=0.1,
                random_state=42, n_jobs=-1,
            )
        except ImportError:
            model = RandomForestRegressor(n_estimators=200, max_depth=15, random_state=42, n_jobs=-1)

        model.fit(X_train, y_train)
        y_pred_log = model.predict(X_test)

        # Convert back from log
        y_test_orig = np.expm1(y_test)
        y_pred_orig = np.expm1(y_pred_log)

        rmse = np.sqrt(mean_squared_error(y_test_orig, y_pred_orig))
        mae = mean_absolute_error(y_test_orig, y_pred_orig)
        r2 = r2_score(y_test_orig, y_pred_orig)

        logger.info("  RMSE: %.2f, MAE: %.2f, R²: %.4f", rmse, mae, r2)

        # Feature importance
        if hasattr(model, "feature_importances_"):
            importances = model.feature_importances_
            feat_imp = sorted(zip(self.feature_cols, importances), key=lambda x: x[1], reverse=True)
        else:
            feat_imp = []

        result = {
            "target": "T2_score",
            "type": "regression",
            "model": model,
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
            "feature_importance": feat_imp[:20],
            "y_test": y_test_orig,
            "y_pred": y_pred_orig,
        }
        self.results["T2"] = result
        return result

    def train_t3_comments_regression(self) -> dict:
        """T3: Comments Regression (predict exact num_comments)."""
        logger.info("=" * 50)
        logger.info("T3: Training Comments Regressor...")
        logger.info("=" * 50)

        X = self._get_X()
        y = pd.to_numeric(self.df["updated_num_comments"], errors="coerce").fillna(0)

        # Log transform
        y_log = np.log1p(y)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y_log, test_size=self.test_size, random_state=42
        )

        try:
            from xgboost import XGBRegressor
            model = XGBRegressor(
                n_estimators=200, max_depth=10, learning_rate=0.1,
                random_state=42, n_jobs=-1,
            )
        except ImportError:
            model = RandomForestRegressor(n_estimators=200, max_depth=15, random_state=42, n_jobs=-1)

        model.fit(X_train, y_train)
        y_pred_log = model.predict(X_test)

        y_test_orig = np.expm1(y_test)
        y_pred_orig = np.expm1(y_pred_log)

        rmse = np.sqrt(mean_squared_error(y_test_orig, y_pred_orig))
        mae = mean_absolute_error(y_test_orig, y_pred_orig)
        r2 = r2_score(y_test_orig, y_pred_orig)

        logger.info("  RMSE: %.2f, MAE: %.2f, R²: %.4f", rmse, mae, r2)

        if hasattr(model, "feature_importances_"):
            importances = model.feature_importances_
            feat_imp = sorted(zip(self.feature_cols, importances), key=lambda x: x[1], reverse=True)
        else:
            feat_imp = []

        result = {
            "target": "T3_comments",
            "type": "regression",
            "model": model,
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
            "feature_importance": feat_imp[:20],
            "y_test": y_test_orig,
            "y_pred": y_pred_orig,
        }
        self.results["T3"] = result
        return result

    def train_t4_emotion_classification(self) -> dict:
        """T4: Dominant Emotion Classification (predict comment reaction)."""
        logger.info("=" * 50)
        logger.info("T4: Training Emotion Classifier...")
        logger.info("=" * 50)

        X = self._get_X()
        # Use comment_dominant_emotion (from aggregated comments)
        # Falls back to dominant_emotion (from post title) if not available
        if "comment_dominant_emotion" in self.df.columns:
            y = self.df["comment_dominant_emotion"]
        else:
            y = self.df["dominant_emotion"]

        # Remove NaN / unknown targets
        mask = y.notna() & (y != "neutral") & (y != "")
        X, y = X[mask], y[mask]

        if len(y.unique()) < 2:
            logger.warning("Not enough emotion classes for T4. Need at least 2, got %d", len(y.unique()))
            return {"target": "T4_emotion", "error": "Not enough classes"}

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=self.test_size, random_state=42, stratify=y
        )

        models = {
            "RandomForest": RandomForestClassifier(n_estimators=200, max_depth=15, random_state=42, n_jobs=-1),
            "GBT": GradientBoostingClassifier(n_estimators=150, max_depth=8, random_state=42),
        }

        best_model = None
        best_f1 = 0
        model_results = {}

        for name, model in models.items():
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            acc = accuracy_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred, average="weighted")

            model_results[name] = {"accuracy": acc, "f1": f1}
            logger.info("  %s — Accuracy: %.4f, F1: %.4f", name, acc, f1)

            if f1 > best_f1:
                best_f1 = f1
                best_model = model

        importances = best_model.feature_importances_
        feat_imp = sorted(zip(self.feature_cols, importances), key=lambda x: x[1], reverse=True)

        result = {
            "target": "T4_emotion",
            "type": "classification",
            "best_model": best_model,
            "model_comparison": model_results,
            "best_f1": best_f1,
            "feature_importance": feat_imp[:20],
            "classification_report": classification_report(y_test, best_model.predict(X_test)),
            "confusion_matrix": confusion_matrix(y_test, best_model.predict(X_test)),
            "classes": list(y.unique()),
        }
        self.results["T4"] = result
        return result

    def train_all(self) -> dict:
        """Train ALL 4 models and return combined results."""
        logger.info("Training 4 models on %d samples with %d features...",
                     len(self.df), len(self.feature_cols))

        self.train_t1_popularity()
        self.train_t2_score_regression()
        self.train_t3_comments_regression()
        self.train_t4_emotion_classification()

        logger.info("=" * 50)
        logger.info("  ALL MODELS TRAINED!")
        logger.info("=" * 50)
        for key, res in self.results.items():
            if "error" in res:
                logger.info("  %s: ERROR — %s", key, res["error"])
            elif res["type"] == "classification":
                logger.info("  %s: F1=%.4f", key, res["best_f1"])
            else:
                logger.info("  %s: R²=%.4f, RMSE=%.2f", key, res["r2"], res["rmse"])

        return self.results

    def save_models(self, output_dir: str = "/home/jovyan/models"):
        """Save all trained models to disk."""
        import joblib
        import os
        os.makedirs(output_dir, exist_ok=True)

        for key, res in self.results.items():
            model = res.get("best_model") or res.get("model")
            if model:
                path = os.path.join(output_dir, f"{key}_model.pkl")
                joblib.dump(model, path)
                logger.info("Saved %s → %s", key, path)
