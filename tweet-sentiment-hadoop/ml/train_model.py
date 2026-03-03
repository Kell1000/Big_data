"""
ml/train_model.py
------------------
Entraîne plusieurs modèles de classification de sentiment :
  - Naive Bayes
  - Logistic Regression
  - (optionnel) LSTM via TensorFlow/Keras

Utilise scikit-learn + TF-IDF pour une exécution rapide en local.
Pour l'entraînement distribué, voir spark_train.py.
"""

import os
import pickle
import argparse
import pandas as pd
import numpy as np
import yaml
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns


# ─── Config ───────────────────────────────────────────────────────────────────

def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


# ─── Preprocessing helpers (replicated for standalone use) ────────────────────

import re

STOP_WORDS = {
    "i","me","my","we","our","you","your","he","him","his","she","her",
    "it","its","they","them","this","that","these","those","am","is","are",
    "was","were","be","been","have","has","had","do","does","did","will",
    "would","could","should","a","an","the","and","but","or","as","at",
    "by","for","with","of","to","in","on","not","no",
}

def preprocess(text):
    text = str(text).lower()
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"@\w+|#\w+", "", text)
    text = re.sub(r"[^a-z\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [t for t in text.split() if t not in STOP_WORDS and len(t) > 2]
    return " ".join(tokens)


# ─── Training ─────────────────────────────────────────────────────────────────

def load_data(csv_path, cfg):
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=["text", "sentiment"])
    df["text_clean"] = df["text"].apply(preprocess)
    df = df[df["text_clean"].str.strip() != ""]
    return df


def train_and_evaluate(df, cfg, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    X = df["text_clean"]
    y = df["sentiment"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=cfg["ml"]["test_size"],
        random_state=cfg["ml"]["random_state"],
        stratify=y,
    )

    # TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        max_features=cfg["ml"]["max_features"],
        ngram_range=(1, 2),
    )
    X_train_tfidf = vectorizer.fit_transform(X_train)
    X_test_tfidf  = vectorizer.transform(X_test)

    models = {
        "NaiveBayes":         MultinomialNB(alpha=0.5),
        "LogisticRegression": LogisticRegression(
            max_iter=500, C=1.0, random_state=cfg["ml"]["random_state"]
        ),
    }

    results = {}
    for name, model in models.items():
        print(f"\n{'='*50}")
        print(f"Training: {name}")
        model.fit(X_train_tfidf, y_train)
        y_pred = model.predict(X_test_tfidf)

        acc = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred)
        cm = confusion_matrix(y_test, y_pred, labels=["positive","negative","neutral"])

        print(f"Accuracy: {acc:.4f}")
        print(report)

        results[name] = {"accuracy": acc, "y_test": y_test, "y_pred": y_pred, "cm": cm}

        # Save model
        with open(f"{output_dir}/{name}.pkl", "wb") as f:
            pickle.dump(model, f)
        print(f"Model saved → {output_dir}/{name}.pkl")

        # Confusion matrix plot
        plt.figure(figsize=(6, 5))
        sns.heatmap(
            cm, annot=True, fmt="d", cmap="Blues",
            xticklabels=["positive","negative","neutral"],
            yticklabels=["positive","negative","neutral"],
        )
        plt.title(f"Confusion Matrix – {name}")
        plt.ylabel("True Label")
        plt.xlabel("Predicted Label")
        plt.tight_layout()
        plt.savefig(f"{output_dir}/confusion_matrix_{name}.png", dpi=120)
        plt.close()

    # Save vectorizer
    with open(f"{output_dir}/tfidf_vectorizer.pkl", "wb") as f:
        pickle.dump(vectorizer, f)

    # Accuracy comparison bar chart
    names = list(results.keys())
    accs  = [results[n]["accuracy"] for n in names]
    plt.figure(figsize=(6, 4))
    plt.bar(names, accs, color=["steelblue", "darkorange"])
    plt.ylim(0, 1)
    plt.title("Model Accuracy Comparison")
    plt.ylabel("Accuracy")
    for i, v in enumerate(accs):
        plt.text(i, v + 0.01, f"{v:.2%}", ha="center", fontweight="bold")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/accuracy_comparison.png", dpi=120)
    plt.close()
    print(f"\nCharts saved to {output_dir}/")

    return results, vectorizer


def main():
    parser = argparse.ArgumentParser(description="Train sentiment analysis models")
    parser.add_argument("--input",  default="data/tweets.csv")
    parser.add_argument("--output", default="ml/models")
    args = parser.parse_args()

    cfg = load_config()
    df  = load_data(args.input, cfg)
    print(f"Dataset loaded: {len(df)} tweets")
    print(df["sentiment"].value_counts())

    train_and_evaluate(df, cfg, args.output)


if __name__ == "__main__":
    main()
