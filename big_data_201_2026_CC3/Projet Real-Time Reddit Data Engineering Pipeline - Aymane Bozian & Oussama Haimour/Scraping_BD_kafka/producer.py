# -*- coding: utf-8 -*-
from __future__ import print_function
import time
import json
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from datetime import datetime
import sys
import random


# --- CONFIGURATION ---
KAFKA_TOPIC = 'bbc_news'  # On garde le meme nom pour ne rien casser
# VERIFIEZ QUE C'EST BIEN VOTRE IP (celle utilisee dans les autres terminaux)
KAFKA_BOOTSTRAP_SERVERS = ['192.168.172.128:9092'] 
TARGET_URL = 'http://quotes.toscrape.com'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Connexion Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer
    )
    print("Succes: Connexion Kafka etablie.")
except Exception as e:
    print("Erreur critique Kafka: {}".format(e))
    sys.exit(1)

def get_quotes():
    try:
        # Pas besoin de headers complexes ici
        response = requests.get(TARGET_URL)
        
        if response.status_code != 200:
            print("Erreur HTTP: {}".format(response.status_code))
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        quotes_data = []
        
        # Le site contient des div avec la classe 'quote'
        divs = soup.find_all('div', class_='quote')
        
        for div in divs:
            try:
                text = div.find('span', class_='text').text.strip()
                author = div.find('small', class_='author').text.strip()
                
                # MAPPING DES DONNEES
                # Pour que ca ressemble a une news BBC dans votre systeme :
                # Auteur -> Titre
                # Citation -> Resume
                article = {
                    'source': 'QuotesBot',
                    'title': "Citation de {}".format(author),
                    'summary': text,
                    'url': TARGET_URL,
                    'timestamp': datetime.now().isoformat()
                }
                quotes_data.append(article)
            except:
                continue
                
        return quotes_data

    except Exception as e:
        print("Erreur Scraping: {}".format(e))
        return []

def run():
    print("Demarrage du Producteur de Citations...")
    print("Cible: {}".format(TARGET_URL))
    
    while True:
        quotes = get_quotes()
        count = 0
        
        if not quotes:
            print("Attention: Aucune citation trouvee.")
        
        # On melange pour simuler un flux vivant
        random.shuffle(quotes)
        
        for quote in quotes:
            # Envoi Kafka
            producer.send(KAFKA_TOPIC, quote)
            count += 1
            
            # Affichage
            try:
                print("Envoye: {}...".format(quote['title']))
            except:
                pass
            
            # Petite pause pour simuler un flux temps reel
            time.sleep(1)
        
        producer.flush()
        print("-" * 30)
        print("Batch termine. {} messages envoyes.".format(count))
        print("Attente de 10 secondes avant re-scan...")
        time.sleep(10)

if __name__ == '__main__':
    run()
