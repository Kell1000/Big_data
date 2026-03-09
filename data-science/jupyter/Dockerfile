# ── Jupyter Lab + PySpark + PyTorch (GPU/CPU) ──────────────────────────────
# This image contains everything needed for distributed data science:
#   - PySpark (connected to Spark cluster)
#   - PyTorch + torchvision (GPU accelerated — Blackwell/sm_120 support)
#   - NLP: BERT (transformers), BERTopic, TextBlob
#   - ML: Scikit-learn, XGBoost
#   - Viz: Matplotlib, Seaborn, Plotly, WordCloud
# ───────────────────────────────────────────────────────────────────────────

FROM python:3.12-slim-bookworm

# ── System dependencies ──────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc g++ \
        openjdk-17-jre-headless \
        curl wget \
        libffi-dev \
        libgl1-mesa-dev \
        libglib2.0-0 \
        && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ── Create virtual environment ───────────────────────────────────────────
RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$JAVA_HOME/bin:$PATH
ENV VIRTUAL_ENV=/opt/venv

# ── Install Python dependencies (except torch) ──────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# ── Install PyTorch nightly with CUDA 12.8 (Blackwell/sm_120 support) ───
RUN pip install --no-cache-dir --pre \
    torch torchvision \
    --index-url https://download.pytorch.org/whl/nightly/cu128

# ── Download NLTK data ───────────────────────────────────────────────────
RUN python -c "\
import nltk; \
nltk.download('punkt'); \
nltk.download('punkt_tab'); \
nltk.download('stopwords'); \
nltk.download('wordnet'); \
nltk.download('averaged_perceptron_tagger'); \
nltk.download('vader_lexicon')"

# ── Setup workspace ─────────────────────────────────────────────────────
RUN useradd -m -s /bin/bash jovyan
WORKDIR /home/jovyan

RUN mkdir -p notebooks src data/raw data/processed data/images models outputs/figures outputs/reports
RUN chown -R jovyan:jovyan /home/jovyan

# Ensure venv PATH for all terminal sessions
RUN echo 'export PATH=/opt/venv/bin:$PATH' >> /home/jovyan/.bashrc && \
    echo 'export PATH=/opt/venv/bin:$PATH' >> /etc/profile.d/venv.sh

USER jovyan

# ── Expose ports ────────────────────────────────────────────────────────
EXPOSE 8888 4040

# ── Start Jupyter Lab ───────────────────────────────────────────────────
CMD ["/opt/venv/bin/jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--IdentityProvider.token=reddit", \
     "--notebook-dir=/home/jovyan"]
