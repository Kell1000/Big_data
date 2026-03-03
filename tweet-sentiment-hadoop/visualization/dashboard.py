"""
visualization/dashboard.py
---------------------------
Dashboard interactif Streamlit pour l'analyse de sentiment des tweets.

Lancement : streamlit run visualization/dashboard.py
"""

import re
import pickle
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tweet Sentiment Analysis",
    page_icon="🐦",
    layout="wide",
)

# ─── Preprocessing (standalone) ───────────────────────────────────────────────
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


# ─── Load model ───────────────────────────────────────────────────────────────
@st.cache_resource
def load_model():
    try:
        with open("ml/models/LogisticRegression.pkl", "rb") as f:
            model = pickle.load(f)
        with open("ml/models/tfidf_vectorizer.pkl", "rb") as f:
            vectorizer = pickle.load(f)
        return model, vectorizer
    except FileNotFoundError:
        return None, None


@st.cache_data
def load_dataset():
    df = pd.read_csv("data/tweets.csv")
    df["text_clean"] = df["text"].apply(preprocess)
    # Fake timestamp for demo purposes
    base = pd.Timestamp("2024-01-01")
    df["timestamp"] = [base + pd.Timedelta(minutes=i * 5) for i in range(len(df))]
    return df


# ─── Prediction helper ────────────────────────────────────────────────────────
SENTIMENT_COLORS = {
    "positive": "#2ecc71",
    "negative": "#e74c3c",
    "neutral":  "#3498db",
}
SENTIMENT_EMOJI = {"positive": "😊", "negative": "😠", "neutral": "😐"}


def predict_sentiment(text, model, vectorizer):
    cleaned  = preprocess(text)
    features = vectorizer.transform([cleaned])
    label    = model.predict(features)[0]
    proba    = model.predict_proba(features)[0]
    classes  = model.classes_
    return label, dict(zip(classes, proba))


# ─── Main layout ──────────────────────────────────────────────────────────────
def main():
    st.title("🐦 Analyse de Sentiment des Tweets")
    st.markdown("**Plateforme Big Data — Hadoop Ecosystem**")
    st.divider()

    model, vectorizer = load_model()
    df = load_dataset()

    # ── Sidebar filters ──────────────────────────────────────────────────────
    st.sidebar.header("⚙️ Filtres")
    selected_sentiments = st.sidebar.multiselect(
        "Sentiments à afficher",
        options=["positive", "negative", "neutral"],
        default=["positive", "negative", "neutral"],
    )
    keyword = st.sidebar.text_input("🔍 Filtrer par mot-clé", "")
    sample_n = st.sidebar.slider("Nombre de tweets affichés", 5, 100, 20)

    df_filtered = df[df["sentiment"].isin(selected_sentiments)]
    if keyword:
        df_filtered = df_filtered[
            df_filtered["text"].str.lower().str.contains(keyword.lower(), na=False)
        ]

    # ── KPIs ─────────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    counts = df_filtered["sentiment"].value_counts()
    total  = len(df_filtered)

    col1.metric("📊 Total tweets", total)
    col2.metric("😊 Positifs", counts.get("positive", 0),
                f"{counts.get('positive', 0)/max(total,1)*100:.1f}%")
    col3.metric("😠 Négatifs", counts.get("negative", 0),
                f"{counts.get('negative', 0)/max(total,1)*100:.1f}%")
    col4.metric("😐 Neutres",  counts.get("neutral", 0),
                f"{counts.get('neutral', 0)/max(total,1)*100:.1f}%")

    st.divider()

    # ── Charts row ───────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Répartition des sentiments")
        pie_data = counts.reset_index()
        pie_data.columns = ["sentiment", "count"]
        fig_pie = px.pie(
            pie_data, values="count", names="sentiment",
            color="sentiment",
            color_discrete_map=SENTIMENT_COLORS,
            hole=0.4,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)

    with c2:
        st.subheader("Évolution temporelle des sentiments")
        df_time = (
            df_filtered.set_index("timestamp")
            .resample("1H")["sentiment"]
            .value_counts()
            .unstack(fill_value=0)
        )
        fig_time = go.Figure()
        for sentiment in ["positive", "negative", "neutral"]:
            if sentiment in df_time.columns:
                fig_time.add_trace(go.Scatter(
                    x=df_time.index, y=df_time[sentiment],
                    name=sentiment, mode="lines+markers",
                    line=dict(color=SENTIMENT_COLORS[sentiment], width=2),
                ))
        fig_time.update_layout(xaxis_title="Heure", yaxis_title="Nombre de tweets")
        st.plotly_chart(fig_time, use_container_width=True)

    # ── Live prediction ──────────────────────────────────────────────────────
    st.divider()
    st.subheader("🔮 Prédiction en temps réel")

    user_input = st.text_area("Entrez un tweet :", placeholder="Type a tweet here...")
    if st.button("Analyser", type="primary"):
        if not user_input.strip():
            st.warning("Veuillez entrer un tweet.")
        elif model is None:
            st.error("Modèle non chargé. Lancez d'abord : python ml/train_model.py")
        else:
            label, probas = predict_sentiment(user_input, model, vectorizer)
            color = SENTIMENT_COLORS[label]
            emoji = SENTIMENT_EMOJI[label]
            st.markdown(
                f"<h3 style='color:{color}'>{emoji} Sentiment prédit : "
                f"<strong>{label.upper()}</strong></h3>",
                unsafe_allow_html=True,
            )
            proba_df = pd.DataFrame({
                "Sentiment": list(probas.keys()),
                "Probabilité": list(probas.values()),
            }).sort_values("Probabilité", ascending=True)
            fig_bar = px.bar(
                proba_df, x="Probabilité", y="Sentiment",
                orientation="h", color="Sentiment",
                color_discrete_map=SENTIMENT_COLORS,
                range_x=[0, 1],
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # ── Tweet sample table ────────────────────────────────────────────────────
    st.divider()
    st.subheader(f"📋 Échantillon de tweets ({sample_n})")
    sample = df_filtered[["text", "sentiment"]].head(sample_n).copy()
    sample["emoji"] = sample["sentiment"].map(SENTIMENT_EMOJI)
    st.dataframe(sample, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
