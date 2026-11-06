# -*- coding: utf-8 -*-
from flask import Flask, render_template_string
from kafka import KafkaConsumer
import json
import threading
import sys


app = Flask(__name__)

# Stockage memoire
latest_quotes = []

# --- CONFIGURATION ---
KAFKA_TOPIC = 'bbc_news'
KAFKA_BOOTSTRAP_SERVERS = ['192.168.172.128:9092']

def consume_kafka():
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            latest_quotes.insert(0, message.value)
            if len(latest_quotes) > 50: 
                latest_quotes.pop()
    except Exception as e:
        print("Erreur Kafka Background: {}".format(e))

# Thread d'ecoute
thread = threading.Thread(target=consume_kafka)
thread.daemon = True
thread.start()

# Template HTML adapte pour les Citations
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Live Quotes Stream</title>
    <meta http-equiv="refresh" content="3">
    <meta charset="UTF-8">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding: 20px; background-color: #e9ecef; font-family: 'Georgia', serif; }
        .quote-card { 
            margin-bottom: 20px; 
            border-left: 5px solid #28a745; 
            background: white;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .quote-text { font-size: 1.4em; font-style: italic; color: #333; }
        .author { color: #28a745; font-weight: bold; font-size: 1.1em; margin-top: 10px; display: block;}
        .timestamp { color: #999; font-size: 0.8em; float: right;}
        h1 { color: #28a745; text-align: center; margin-bottom: 30px; text-transform: uppercase; letter-spacing: 2px;}
    </style>
</head>
<body class="container">
    <h1>Flux de Citations (Kafka Live)</h1>
    
    {% if not quotes %}
        <div class="alert alert-info text-center">
            En attente de nouvelles citations... <br>
            (Verifiez que producer.py est lance)
        </div>
    {% endif %}

    <div>
        {% for item in quotes %}
        <div class="quote-card">
            <span class="timestamp">{{ item.timestamp }}</span>
            <div class="quote-text">"{{ item.summary }}"</div>
            <span class="author">- {{ item.title }}</span> </div>
        {% endfor %}
    </div>
</body>
</html>
"""

@app.route('/')
def index():
    # Note: On passe la variable 'quotes' au template
    return render_template_string(HTML_TEMPLATE, quotes=latest_quotes)

if __name__ == '__main__':
    print("Dashboard demarre sur http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
