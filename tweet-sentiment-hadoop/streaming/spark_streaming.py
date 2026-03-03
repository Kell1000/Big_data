"""
streaming/spark_streaming.py
-----------------------------
Consomme les tweets depuis Kafka, applique le modèle ML
et écrit les résultats en temps réel.

Lancement :
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
      streaming/spark_streaming.py
"""

import pickle
import argparse
import re
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


STOP_WORDS = {
    "i","me","my","we","you","he","him","she","her","it","they","this",
    "that","am","is","are","was","were","have","has","do","does","a",
    "an","the","and","but","or","for","with","of","to","in","on","not",
}

def preprocess(text):
    text = str(text).lower()
    text = re.sub(r"http\S+|@\w+|#\w+|[^a-z\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [t for t in text.split() if t not in STOP_WORDS and len(t) > 2]
    return " ".join(tokens)


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def load_model(model_dir):
    with open(f"{model_dir}/LogisticRegression.pkl", "rb") as f:
        model = pickle.load(f)
    with open(f"{model_dir}/tfidf_vectorizer.pkl", "rb") as f:
        vectorizer = pickle.load(f)
    return model, vectorizer


def predict(text, model, vectorizer):
    cleaned = preprocess(text)
    features = vectorizer.transform([cleaned])
    return model.predict(features)[0]


def run(cfg):
    kafka_broker = cfg["kafka"]["broker"]
    kafka_topic  = cfg["kafka"]["topic"]
    model_dir    = "ml/models"

    spark = (
        SparkSession.builder
        .appName("TweetSentimentStreaming")
        .master(cfg["spark"]["master"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load model once (broadcast to workers in production)
    model, vectorizer = load_model(model_dir)
    predict_udf = udf(lambda t: predict(t, model, vectorizer), StringType())

    schema = StructType([
        StructField("id",   IntegerType()),
        StructField("text", StringType()),
        StructField("sentiment_label", StringType()),
    ])

    df_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    df_predicted = df_stream.withColumn(
        "predicted_sentiment", predict_udf(col("text"))
    )

    query = (
        df_predicted.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime=f"{cfg['spark']['streaming_interval']} seconds")
        .start()
    )

    query.awaitTermination()


def main():
    cfg = load_config()
    run(cfg)


if __name__ == "__main__":
    main()
