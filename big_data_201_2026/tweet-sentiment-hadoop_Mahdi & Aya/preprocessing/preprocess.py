"""
preprocessing/preprocess.py
-----------------------------
Nettoyage et prétraitement des tweets avec PySpark.
Fonctionne en mode local ou sur cluster Hadoop.
"""

import re
import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


# ─── NLP helpers ──────────────────────────────────────────────────────────────

STOP_WORDS = {
    "i", "me", "my", "myself", "we", "our", "you", "your", "he", "him",
    "his", "she", "her", "it", "its", "they", "them", "their", "what",
    "which", "who", "this", "that", "these", "those", "am", "is", "are",
    "was", "were", "be", "been", "being", "have", "has", "had", "do",
    "does", "did", "will", "would", "could", "should", "may", "might",
    "a", "an", "the", "and", "but", "if", "or", "as", "at", "by",
    "for", "with", "about", "of", "to", "in", "on", "not", "no",
}


def clean_tweet(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"http\S+|www\S+", "", text)        # URLs
    text = re.sub(r"@\w+", "", text)                   # mentions
    text = re.sub(r"#\w+", "", text)                   # hashtags
    text = re.sub(r"[^a-z\s]", "", text)               # ponctuation / chiffres
    text = re.sub(r"\s+", " ", text).strip()           # espaces multiples
    return text


def remove_stopwords(text: str) -> str:
    tokens = text.split()
    tokens = [t for t in tokens if t not in STOP_WORDS and len(t) > 2]
    return " ".join(tokens)


def preprocess(text: str) -> str:
    return remove_stopwords(clean_tweet(text))


# ─── Spark job ────────────────────────────────────────────────────────────────

def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def run(input_path: str, output_path: str, master: str = "local[*]"):
    spark = (
        SparkSession.builder
        .appName("TweetPreprocessing")
        .master(master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Spark] Reading from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    preprocess_udf = udf(preprocess, StringType())

    df_clean = (
        df.withColumn("text_clean", preprocess_udf(col("text")))
          .filter(col("text_clean") != "")
    )

    print(f"[Spark] Writing cleaned data to: {output_path}")
    df_clean.write.mode("overwrite").parquet(output_path)

    print(f"[Spark] Done. Rows processed: {df_clean.count()}")
    df_clean.show(5, truncate=80)
    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Preprocess tweets with Spark")
    parser.add_argument("--input",  default="data/tweets.csv")
    parser.add_argument("--output", default="data/tweets_clean.parquet")
    parser.add_argument("--master", default="local[*]")
    args = parser.parse_args()

    run(args.input, args.output, args.master)


if __name__ == "__main__":
    main()
