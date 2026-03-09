"""
ml/spark_train.py
------------------
Entraînement distribué avec Apache Spark MLlib.
Exécuter sur le cluster : spark-submit ml/spark_train.py
"""

import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
)
from pyspark.ml.classification import NaiveBayes, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def build_pipeline(classifier):
    tokenizer     = Tokenizer(inputCol="text_clean", outputCol="words")
    remover       = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashing_tf    = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=10000)
    idf           = IDF(inputCol="raw_features", outputCol="features")
    indexer       = StringIndexer(inputCol="sentiment", outputCol="label")

    return Pipeline(stages=[tokenizer, remover, hashing_tf, idf, indexer, classifier])


def run(input_path, output_path, master):
    spark = (
        SparkSession.builder
        .appName("TweetSentimentML")
        .master(master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load preprocessed data (parquet) or raw CSV
    if input_path.endswith(".parquet"):
        df = spark.read.parquet(input_path)
    else:
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        df = df.withColumnRenamed("text", "text_clean")

    df = df.filter(df.text_clean.isNotNull())
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )

    classifiers = {
        "NaiveBayes":         NaiveBayes(smoothing=0.5),
        "LogisticRegression": LogisticRegression(maxIter=100),
    }

    for name, clf in classifiers.items():
        pipeline = build_pipeline(clf)
        model    = pipeline.fit(train)
        preds    = model.transform(test)
        acc      = evaluator.evaluate(preds)
        print(f"[{name}] Accuracy: {acc:.4f}")
        model.write().overwrite().save(f"{output_path}/{name}_spark_model")
        print(f"  Model saved → {output_path}/{name}_spark_model")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Distributed ML with Spark MLlib")
    parser.add_argument("--input",  default="data/tweets_clean.parquet")
    parser.add_argument("--output", default="ml/models")
    parser.add_argument("--master", default="local[*]")
    args = parser.parse_args()

    cfg = load_config()
    run(args.input, args.output, args.master)


if __name__ == "__main__":
    main()
