import requests
import json
import re
import time
from kafka import KafkaProducer
from datetime import datetime

# NewsAPI configuration
API_KEY = "e4a3760c007942d5b2f7066a377a684b"
BASE_URL = "https://newsapi.org/v2/everything"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KEYWORD = "technology"

def clean_text(text):
    if not text:
        return ""
    text = re.sub(r'http\S+', '', text)          # Remove URLs
    text = re.sub(r'[^A-Za-z0-9 .,!?]+', '', text)  # Remove special chars
    text = text.strip()
    return text

def fetch_and_stream():
    print(f"Streaming news articles for keyword: {KEYWORD}")
    seen_ids = set()
    
    while True:
        try:
            params = {
                "q": KEYWORD,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 20,
                "apiKey": API_KEY
            }
            response = requests.get(BASE_URL, params=params)
            data = response.json()

            if data.get("status") == "ok":
                for article in data.get("articles", []):
                    article_id = article.get("url", "")
                    if article_id in seen_ids:
                        continue
                    seen_ids.add(article_id)

                    record = {
                        "id": article_id,
                        "title": clean_text(article.get("title", "")),
                        "text": clean_text(article.get("description", "")),
                        "source": article.get("source", {}).get("name", "unknown"),
                        "timestamp": datetime.utcnow().isoformat()
                    }

                    if record["title"]:
                        producer.send('news-raw', record)
                        print(f"Sent: {record['title'][:70]}...")

            else:
                print(f"API error: {data.get('message')}")

        except Exception as e:
            print(f"Error: {e}")

        print("Waiting 60 seconds before next fetch...")
        time.sleep(60)  

if __name__ == "__main__":
    fetch_and_stream()
