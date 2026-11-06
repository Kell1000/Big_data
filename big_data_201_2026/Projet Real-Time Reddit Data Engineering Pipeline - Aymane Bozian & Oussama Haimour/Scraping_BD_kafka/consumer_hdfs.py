# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import time
from kafka import KafkaConsumer
from hdfs import InsecureClient
from datetime import datetime
import sys


# --- CONFIGURATION ---
KAFKA_TOPIC = 'bbc_news' # On garde le meme topic pour simplifier
KAFKA_BOOTSTRAP_SERVERS = ['192.168.172.128:9092']
HDFS_URL = 'http://192.168.172.128:50070'
HDFS_USER = 'cloudera'

# Connexion HDFS
client = InsecureClient(HDFS_URL, user=HDFS_USER)

# Connexion Kafka
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='latest', # On prend seulement les nouveaux messages
        enable_auto_commit=True,
        group_id='hdfs_quotes_archiver', # Nouveau Group ID pour eviter les conflits
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
except Exception as e:
    print("Erreur connexion Kafka: {}".format(e))
    sys.exit(1)

print("Demarrage du Consommateur HDFS (Mode: Citations)...")

batch_data = []
BATCH_SIZE = 5 # On reduit la taille du batch pour voir les resultats plus vite

for message in consumer:
    try:
        article = message.value
        batch_data.append(article)
        
        # Affichage propre dans la console
        title = article.get('title', 'No Title')
        # Astuce pour eviter le crash d'affichage sur Python 2
        try:
            print("Recu: {}".format(title))
        except:
            print("Recu: [Message avec accents]")
        
        if len(batch_data) >= BATCH_SIZE:
            # Generation du nom de fichier
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            # Changement du nom : quotes_...
            filename = "/user/cloudera/quotes_data/quotes_{}.json".format(timestamp)
            
            # Conversion en string (JSON Lines)
            content = "\n".join([json.dumps(a) for a in batch_data])
            
            # Ecriture HDFS
            with client.write(filename, encoding='utf-8') as writer:
                writer.write(content)
                
            print("--> SAUVEGARDE: {} citations dans {}".format(len(batch_data), filename))
            batch_data = [] # Reset buffer

    except Exception as e:
        print("Erreur traitement: {}".format(e))
