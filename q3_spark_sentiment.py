import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, to_json, struct
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize Spark Session with Kafka and Hive support
spark = SparkSession.builder \
    .appName("NewsSentimentAnalysis") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3") \
    .config("spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=/tmp/spark_metastore_db;create=true") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka JSON
schema = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("source", StringType()),
    StructField("timestamp", StringType())
])

# VADER sentiment functions
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not text:
        return "neutral"
    score = analyzer.polarity_scores(text)['compound']
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"

def get_sentiment_score(text):
    if not text:
        return 0.0
    return float(analyzer.polarity_scores(text)['compound'])

sentiment_udf = udf(get_sentiment, StringType())
score_udf = udf(get_sentiment_score, FloatType())

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "news-raw") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Apply sentiment analysis
df_analyzed = df_parsed \
    .withColumn("sentiment", sentiment_udf(col("title"))) \
    .withColumn("sentiment_score", score_udf(col("title")))

# Write analyzed data back to Kafka topic
df_kafka_out = df_analyzed.select(
    to_json(struct("*")).alias("value")
)

kafka_query = df_kafka_out.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "news-analyzed") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .outputMode("append") \
    .start()

# Create Hive table and write to it in Parquet format
spark.sql("""
    CREATE TABLE IF NOT EXISTS news_sentiment (
        id STRING,
        title STRING,
        text STRING,
        source STRING,
        timestamp STRING,
        sentiment STRING,
        sentiment_score FLOAT
    )
    STORED AS PARQUET
""")

def write_to_hive(batch_df, batch_id):
    try:
        if not batch_df.isEmpty():
            batch_df.write \
                .mode("append") \
                .parquet("hdfs://localhost:9000/user/hive/warehouse/news_sentiment")
            print(f"Batch {batch_id}: wrote {batch_df.count()} rows to HDFS")
        else:
            print(f"Batch {batch_id}: empty, skipping")
    except Exception as e:
        print(f"Batch {batch_id} ERROR: {e}")

hive_query = df_analyzed.writeStream \
    .foreachBatch(write_to_hive) \
    .option("checkpointLocation", "/tmp/hive-checkpoint") \
    .outputMode("append") \
    .start()

# Print to console so you can see it working
console_query = df_analyzed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Windowed trend analysis (Rubric 3.3)
from pyspark.sql.functions import window, count, avg

windowed_trends = df_analyzed \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("sentiment")
    ) \
    .agg(
        count("*").alias("article_count"),
        avg("sentiment_score").alias("avg_score")
    )

trend_query = windowed_trends.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

console_query.awaitTermination()
