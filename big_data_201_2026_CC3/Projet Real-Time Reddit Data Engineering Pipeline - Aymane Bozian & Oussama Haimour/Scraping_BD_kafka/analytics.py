# -*- coding: utf-8 -*-
from flask import Flask, render_template_string, jsonify, request, Response
from kafka import KafkaConsumer
import threading
import json
import collections
import time
import re


# --- CONFIGURATION ---
KAFKA_TOPIC = 'bbc_news'
KAFKA_BOOTSTRAP = ['192.168.172.128:9092'] # VOTRE IP

app = Flask(__name__)

# --- ANALYSE DE SENTIMENT (Dictionnaire simple pour Python 2) ---
POSITIVE_WORDS = set(['love', 'good', 'great', 'happy', 'life', 'best', 'hope', 'wisdom', 'truth', 'beautiful', 'success'])
NEGATIVE_WORDS = set(['hate', 'bad', 'death', 'fail', 'wrong', 'lies', 'stupid', 'pain', 'fear', 'worst', 'sorrow'])

def analyze_sentiment(text):
    text = text.lower()
    pos_count = sum(1 for w in POSITIVE_WORDS if w in text)
    neg_count = sum(1 for w in NEGATIVE_WORDS if w in text)
    
    if pos_count > neg_count: return 'Positive'
    if neg_count > pos_count: return 'Negative'
    return 'Neutral'

# --- MEMORY STORAGE (BUFFER CIRCULAIRE) ---
# On garde les 200 derniers messages pour permettre le filtrage
MAX_HISTORY = 200
message_buffer = collections.deque(maxlen=MAX_HISTORY)
stats_lock = threading.Lock()

def kafka_consumer_thread():
    print("Background: Starting Kafka Insight Engine...")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset='latest',
            value_deserializer=None
        )
        
        for message in consumer:
            try:
                # Decodage robuste
                val = message.value
                if hasattr(val, 'decode'):
                    val = val.decode('utf-8', 'ignore')
                
                data = json.loads(val)
                
                # Nettoyage
                raw_author = data.get('title', 'Unknown')
                author = raw_author.replace("Citation de ", "").replace("Quote by ", "")
                text = data.get('summary', '')
                
                # Enrichissement (Added Value)
                sentiment = analyze_sentiment(text)
                
                msg_obj = {
                    'author': author,
                    'text': text,
                    'sentiment': sentiment,
                    'timestamp': time.strftime('%H:%M:%S')
                }

                with stats_lock:
                    message_buffer.append(msg_obj)
                         
            except Exception:
                pass
    except Exception as e:
        print("Kafka Error: {}".format(e))

t = threading.Thread(target=kafka_consumer_thread)
t.daemon = True
t.start()

# --- WEB INTERFACE ---
HTML_PAGE = u"""
<!DOCTYPE html>
<html>
<head>
    <title>Insight Engine</title>
    <meta charset="UTF-8">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/bootswatch/4.6.0/darkly/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.9.4/Chart.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/js/all.min.js"></script>
    <style>
        body { padding-top: 20px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .card { margin-bottom: 20px; border: 1px solid #444; }
        .big-kpi { font-size: 2.5em; font-weight: bold; text-align: center; }
        .scroll-box { max-height: 400px; overflow-y: auto; font-size: 0.9em; }
        .badge-pos { background-color: #28a745; color: white; }
        .badge-neg { background-color: #dc3545; color: white; }
        .badge-neu { background-color: #6c757d; color: white; }
        .highlight { color: #f39c12; font-weight: bold; }
    </style>
</head>
<body class="container-fluid">
    
    <div class="row mb-3">
        <div class="col-md-4">
            <h3><i class="fas fa-brain"></i> Insight Engine</h3>
        </div>
        <div class="col-md-4">
            <div class="input-group">
                <div class="input-group-prepend">
                    <span class="input-group-text"><i class="fas fa-search"></i></span>
                </div>
                <input type="text" id="searchInput" class="form-control" placeholder="Filter by keyword (ex: life, love)...">
            </div>
        </div>
        <div class="col-md-4 text-right">
            <button id="pauseBtn" class="btn btn-warning btn-sm" onclick="togglePause()"><i class="fas fa-pause"></i> Pause</button>
            <a href="/download_csv" target="_blank" class="btn btn-success btn-sm"><i class="fas fa-download"></i> Export CSV</a>
        </div>
    </div>

    <div class="row">
        <div class="col-md-2">
            <div class="card bg-secondary">
                <div class="card-body p-2">
                    <div id="kpi-count" class="big-kpi text-white">0</div>
                    <p class="text-center m-0 text-white"><small>Filtered Quotes</small></p>
                </div>
            </div>
        </div>
        <div class="col-md-2">
            <div class="card bg-secondary">
                <div class="card-body p-2">
                    <div id="kpi-pos" class="big-kpi text-success">0%</div>
                    <p class="text-center m-0 text-white"><small>Positive Ratio</small></p>
                </div>
            </div>
        </div>
        <div class="col-md-8">
            <div class="progress" style="height: 45px; margin-top: 5px;">
                <div id="bar-pos" class="progress-bar bg-success" style="width: 33%">Pos</div>
                <div id="bar-neu" class="progress-bar bg-secondary" style="width: 33%">Neu</div>
                <div id="bar-neg" class="progress-bar bg-danger" style="width: 33%">Neg</div>
            </div>
        </div>
    </div>

    <div class="row">
        <div class="col-md-4">
            <div class="card">
                <div class="card-header">Top Speakers (Filtered)</div>
                <div class="card-body">
                    <canvas id="authorChart" height="200"></canvas>
                </div>
            </div>
        </div>
        <div class="col-md-8">
            <div class="card">
                <div class="card-header">Live Feed Context</div>
                <div class="card-body scroll-box">
                    <table class="table table-sm table-hover table-dark">
                        <thead><tr><th>Time</th><th>Sentiment</th><th>Author</th><th>Quote</th></tr></thead>
                        <tbody id="msg-table"></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <script>
        var isPaused = false;

        // CHART SETUP
        var ctxAuth = document.getElementById('authorChart').getContext('2d');
        var authorChart = new Chart(ctxAuth, {
            type: 'horizontalBar',
            data: { labels: [], datasets: [{ label: 'Quotes', data: [], backgroundColor: '#3498db' }] },
            options: { 
                legend: { display: false },
                scales: { xAxes: [{ ticks: { beginAtZero: true } }] } 
            }
        });

        function togglePause() {
            isPaused = !isPaused;
            var btn = document.getElementById('pauseBtn');
            if(isPaused) {
                btn.innerHTML = '<i class="fas fa-play"></i> Resume';
                btn.classList.remove('btn-warning');
                btn.classList.add('btn-info');
            } else {
                btn.innerHTML = '<i class="fas fa-pause"></i> Pause';
                btn.classList.remove('btn-info');
                btn.classList.add('btn-warning');
            }
        }

        function getBadge(sent) {
            if(sent === 'Positive') return '<span class="badge badge-pos">POS</span>';
            if(sent === 'Negative') return '<span class="badge badge-neg">NEG</span>';
            return '<span class="badge badge-neu">NEU</span>';
        }

        function updateDashboard() {
            if (isPaused) return;

            var filter = document.getElementById('searchInput').value;

            // Appel API avec le filtre
            fetch('/api/data?filter=' + encodeURIComponent(filter))
                .then(response => response.json())
                .then(data => {
                    // KPI
                    document.getElementById('kpi-count').innerText = data.total_filtered;
                    
                    // Sentiment Ratio Logic
                    var total = data.total_filtered || 1;
                    var pos_pct = Math.round((data.sentiment.Positive / total) * 100);
                    var neg_pct = Math.round((data.sentiment.Negative / total) * 100);
                    var neu_pct = 100 - pos_pct - neg_pct;
                    
                    document.getElementById('kpi-pos').innerText = pos_pct + "%";
                    
                    document.getElementById('bar-pos').style.width = pos_pct + "%";
                    document.getElementById('bar-pos').innerText = pos_pct > 5 ? pos_pct + "%" : "";
                    
                    document.getElementById('bar-neu').style.width = neu_pct + "%";
                    document.getElementById('bar-neu').innerText = neu_pct > 5 ? neu_pct + "%" : "";

                    document.getElementById('bar-neg').style.width = neg_pct + "%";
                    document.getElementById('bar-neg').innerText = neg_pct > 5 ? neg_pct + "%" : "";

                    // Chart Update
                    authorChart.data.labels = data.authors.map(x => x[0]);
                    authorChart.data.datasets[0].data = data.authors.map(x => x[1]);
                    authorChart.update();

                    // Table Update
                    var rows = "";
                    data.messages.forEach(msg => {
                        rows += `<tr>
                            <td><small>${msg.timestamp}</small></td>
                            <td>${getBadge(msg.sentiment)}</td>
                            <td class="highlight">${msg.author}</td>
                            <td>${msg.text}</td>
                        </tr>`;
                    });
                    document.getElementById('msg-table').innerHTML = rows;
                });
        }

        setInterval(updateDashboard, 1500);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_PAGE)

# API INTELLIGENTE AVEC FILTRAGE
@app.route('/api/data')
def get_data():
    filter_word = request.args.get('filter', '').lower()
    
    with stats_lock:
        # Copie locale pour travailler
        raw_data = list(message_buffer)
    
    # 1. FILTRAGE
    if filter_word:
        filtered_data = [m for m in raw_data if filter_word in m['text'].lower() or filter_word in m['author'].lower()]
    else:
        filtered_data = raw_data
        
    # 2. AGGREGATION TEMPS REEL (Sur les données filtrées uniquement)
    author_counts = collections.Counter([m['author'] for m in filtered_data])
    sentiment_counts = collections.Counter([m['sentiment'] for m in filtered_data])
    
    response = {
        'total_filtered': len(filtered_data),
        'authors': author_counts.most_common(5),
        'sentiment': dict(sentiment_counts),
        'messages': filtered_data[:15] # On renvoie les 15 plus récents pour le tableau
    }
    return jsonify(response)

@app.route('/download_csv')
def download_csv():
    with stats_lock:
        data = list(message_buffer)
    
    def generate():
        yield "Timestamp,Author,Sentiment,Quote\n"
        for row in data:
            # Nettoyage CSV basique pour éviter les conflits de virgules
            clean_quote = row['text'].replace('"', "'").replace(',', ';')
            # ASCII encoding for download safety
            line = u"{},{},{},{}\n".format(row['timestamp'], row['author'], row['sentiment'], clean_quote)
            yield line.encode('utf-8')
            
    return Response(generate(), mimetype='text/csv', headers={"Content-disposition": "attachment; filename=kafka_export.csv"})

if __name__ == '__main__':
    print("INSIGHT ENGINE STARTED on http://0.0.0.0:5001")
    app.run(host='0.0.0.0', port=5001, debug=True, use_reloader=False)
