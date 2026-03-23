from flask import Flask, render_template_string
from kafka import KafkaConsumer
import json
import threading
from collections import defaultdict, deque
import plotly.graph_objs as go
import plotly

app = Flask(__name__)

sentiment_counts = defaultdict(int)
recent_articles = deque(maxlen=50)

def consume_analyzed():
    consumer = KafkaConsumer(
        'news-analyzed',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    for message in consumer:
        data = message.value
        sentiment = data.get('sentiment', 'neutral')
        sentiment_counts[sentiment] += 1
        recent_articles.appendleft(data)

thread = threading.Thread(target=consume_analyzed, daemon=True)
thread.start()

DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>News Sentiment Dashboard</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: white; padding: 20px; }
        h1 { color: #e94560; }
        .charts { display: flex; gap: 20px; flex-wrap: wrap; }
        .card { background: #16213e; border-radius: 10px; padding: 20px; margin-bottom: 20px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 8px; text-align: left; border-bottom: 1px solid #444; font-size: 13px; }
        .positive { color: #4ecca3; }
        .negative { color: #e94560; }
        .neutral { color: #a8a8a8; }
    </style>
</head>
<body>
    <h1>Real-Time News Sentiment Dashboard</h1>
    <div class="charts">
        <div class="card">{{ pie_chart | safe }}</div>
        <div class="card">{{ bar_chart | safe }}</div>
    </div>
    <div class="card">
        <h2>Recent Articles</h2>
        <table>
            <tr><th>Title</th><th>Source</th><th>Sentiment</th><th>Score</th></tr>
            {% for article in articles %}
            <tr>
                <td>{{ article.title[:80] }}</td>
                <td>{{ article.source }}</td>
                <td class="{{ article.sentiment }}">{{ article.sentiment }}</td>
                <td>{{ "%.3f"|format(article.sentiment_score|float) }}</td>
            </tr>
            {% endfor %}
        </table>
    </div>
</body>
</html>
"""

@app.route('/')
def index():
    labels = list(sentiment_counts.keys()) or ["positive", "negative", "neutral"]
    values = [sentiment_counts[l] for l in labels] or [1, 1, 1]

    pie = go.Figure(go.Pie(labels=labels, values=values,
                           marker_colors=['#4ecca3', '#e94560', '#a8a8a8']))
    pie.update_layout(title="Sentiment Distribution", paper_bgcolor='#16213e',
                      font_color='white', height=350)
    pie_html = plotly.io.to_html(pie, full_html=False)

    bar = go.Figure(go.Bar(x=labels, y=values,
                           marker_color=['#4ecca3', '#e94560', '#a8a8a8']))
    bar.update_layout(title="Sentiment Counts", paper_bgcolor='#16213e',
                      font_color='white', height=350)
    bar_html = plotly.io.to_html(bar, full_html=False)

    return render_template_string(DASHBOARD_HTML,
                                  pie_chart=pie_html,
                                  bar_chart=bar_html,
                                  articles=list(recent_articles)[:20])

if __name__ == '__main__':
    app.run(debug=False, port=5000)
