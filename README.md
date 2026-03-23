# 📰 Real-Time News Sentiment Analyzer

A real-time big data pipeline that streams news headlines from **NewsAPI**, performs **VADER sentiment analysis** using Apache Spark, stores results in **Apache Hive**, and visualizes live sentiment trends on an interactive **Flask dashboard**.

---

## 🏗️ Architecture

```
NewsAPI
   │
   ▼
news_producer.py  ──►  Kafka Topic: news-raw
                               │
                               ▼
                     q3_spark_sentiment.py  (PySpark + VADER)
                        │              │
                        ▼              ▼
              Kafka: news-analyzed    Hive: news_sentiment
                        │
                        ▼
                  q3_dashboard.py  ──►  http://localhost:5000
```

**Additional analysis scripts:**
- `trending_keywords.py` — extracts top trending keywords from the stream
- `most_commented.py` — ranks most-discussed topics
- `sentiment_per_trailer.py` — per-topic sentiment breakdown

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Data Source | NewsAPI (free tier) |
| Message Broker | Apache Kafka 4.x (KRaft mode, no Zookeeper) |
| Stream Processing | Apache Spark 4.0.1 + PySpark |
| Sentiment Analysis | VADER (vaderSentiment) |
| Storage | Apache Hive + HDFS |
| Dashboard | Flask + Plotly |
| Platform | macOS / Apple Silicon |

---

## 📋 Prerequisites

- macOS with [Homebrew](https://brew.sh/)
- Java 17 (`brew install openjdk@17`)
- Apache Hadoop (`brew install hadoop`)
- Apache Kafka (`brew install kafka`)
- Apache Spark (`brew install apache-spark`)
- Apache Hive (`brew install hive`)
- Python 3.x with pip

---

## ⚙️ Installation

### 1. Clone the repo

```bash
git clone https://github.com/<your-username>/news-sentiment-pipeline.git
cd news-sentiment-pipeline
```

### 2. Install Python dependencies

```bash
pip install kafka-python pyspark vaderSentiment flask plotly newsapi-python requests findspark
```

### 3. Get a NewsAPI key

Sign up for free at [https://newsapi.org](https://newsapi.org) and copy your API key.

Open `q3_news_producer.py` and replace the placeholder:

```python
API_KEY = "your_newsapi_key_here"
```

### 4. Configure Hadoop

Make sure `core-site.xml` has:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
```

And `hdfs-site.xml` has:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
</configuration>
```

### 5. Configure Kafka (KRaft mode)

Format Kafka storage (only needed once):

```bash
kafka-storage format --standalone \
  -t $(kafka-storage random-uuid) \
  -c /opt/homebrew/etc/kafka/server.properties
```

---

## 🚀 Running the Pipeline

Open **4 separate terminal windows** and run each step in order:

### Terminal 1 — Start Infrastructure

```bash
# Start HDFS
start-dfs.sh

# Start Kafka
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

### Terminal 2 — Create Kafka Topics (first time only)

```bash
kafka-topics --create --topic news-raw --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic news-analyzed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Terminal 3 — Start the News Producer

```bash
cd news-sentiment-pipeline
python q3_news_producer.py
```

Fetches headlines from NewsAPI every 60 seconds and publishes to `news-raw`.

### Terminal 4 — Start Spark Sentiment Analysis

```bash
cd news-sentiment-pipeline
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3 q3_spark_sentiment.py
```

Reads from `news-raw`, applies VADER sentiment, writes results to `news-analyzed` and Hive.

### Terminal 5 — Start the Dashboard

```bash
cd news-sentiment-pipeline
python q3_dashboard.py
```

Open your browser at **http://localhost:5000**

---

## 📊 Dashboard Features

- **Live pie chart** — positive / negative / neutral sentiment breakdown
- **Bar chart** — article volume by sentiment category
- **Article table** — latest headlines with sentiment scores
- **Auto-refreshes** every 10 seconds

---

## 📂 Project Structure

```
news-sentiment-pipeline/
├── q3_news_producer.py       # Fetches NewsAPI → publishes to Kafka
├── q3_spark_sentiment.py     # PySpark streaming sentiment analysis
├── q3_dashboard.py           # Flask + Plotly live dashboard
├── trending_keywords.py      # Top keyword trend analysis
├── most_commented.py         # Most-discussed topic ranking
├── sentiment_per_trailer.py  # Per-topic sentiment breakdown
└── README.md
```

---

## 🐛 Known Issues & Workarounds

| Issue | Fix |
|-------|-----|
| `spark-sql-kafka` connector incompatibility with Spark 4.x | Use `_2.13:3.5.3` build; if it fails, use HDFS intermediate storage workaround |
| Derby `metastore_db` version mismatch | Delete `metastore_db/` folder in project root and re-run — Spark will recreate it |
| Kafka KRaft cluster ID mismatch | `rm -rf /opt/homebrew/var/lib/kraft-combined-logs` then reformat with `--standalone` |
| SSH refused for Hadoop | Enable Remote Login: System Settings → General → Sharing → Remote Login |

---

## 📝 Notes

- Kafka runs in **KRaft mode** (no Zookeeper required) — Kafka 3.x+
- Sentiment scores use VADER's compound score: `≥ 0.05` = positive, `≤ -0.05` = negative
- Hive table `news_sentiment` stores results in Parquet format on HDFS
- Windowed trend analysis uses 5-minute windows with 1-minute slide intervals

---

## 📄 License

MIT
