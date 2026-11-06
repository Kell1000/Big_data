"""
ingestion/load_to_hdfs.py
--------------------------
Charge le dataset CSV local dans HDFS (zone Raw).
Mode LOCAL disponible si Hadoop n'est pas lancé.
"""

import os
import argparse
import pandas as pd
import yaml


def load_config(path="config/config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def load_to_hdfs(local_file: str, hdfs_path: str):
    """Copie un fichier vers HDFS via la commande shell hadoop fs."""
    print(f"[HDFS] Uploading {local_file} → {hdfs_path}")
    ret = os.system(f"hadoop fs -mkdir -p {hdfs_path}")
    ret = os.system(f"hadoop fs -put -f {local_file} {hdfs_path}/")
    if ret == 0:
        print("[HDFS] Upload successful.")
    else:
        print("[HDFS] Upload failed. Is Hadoop running?")


def preview_local(local_file: str, n: int = 5):
    df = pd.read_csv(local_file)
    print(f"\nDataset preview ({len(df)} rows):")
    print(df.head(n))
    print("\nSentiment distribution:")
    print(df["sentiment"].value_counts())


def main():
    parser = argparse.ArgumentParser(description="Load tweets dataset to HDFS")
    parser.add_argument("--local", action="store_true",
                        help="Preview only (no HDFS required)")
    parser.add_argument("--file", default="data/tweets.csv",
                        help="Path to local CSV dataset")
    args = parser.parse_args()

    cfg = load_config()
    local_file = args.file

    preview_local(local_file)

    if not args.local:
        raw_path = cfg["hadoop"]["raw_path"]
        load_to_hdfs(local_file, raw_path)
    else:
        print("\n[LOCAL MODE] Skipping HDFS upload.")


if __name__ == "__main__":
    main()
