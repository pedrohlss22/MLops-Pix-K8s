import os
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "pix-cluster-kafka-bootstrap:9092")
KAFKA_TOPIC_IN = "transacoes-pix"
KAFKA_TOPIC_DLQ = "transacoes-pix-dlq"
KAFKA_USER = os.getenv("KAFKA_USER", "spark-kafka-user")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

REDIS_HOST = os.getenv("REDIS_HOST", "redis-service.datalake.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://bucket-minio.datalake.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
CHECKPOINT_LOCATION = "s3a://processed/checkpoints_pix_v4/"
OUTPUT_PATH_PARQUET = "s3a://processed/pix_history_v4/"

jaas_config = f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'

spark = SparkSession.builder \
    .appName("PIX-Fraud-Streaming-Enterprise") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(">>> [INFO] Spark Streaming inicializado (modo cluster)")

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, TimestampType
schema_transacao = StructType([
    StructField("valor_pix", DoubleType(), True),
    StructField("hora_transacao", IntegerType(), True),
    StructField("score_conta_origem", IntegerType(), True),
    StructField("tipo_chave", IntegerType(), True),
    StructField("is_fraude", IntegerType(), True),
    StructField("event_time", TimestampType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC_IN) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as value_str") \
    .withColumn("data", from_json(col("value_str"), schema_transacao))

condicao_erro = (
    col("data.valor_pix").isNull() |
    col("data.event_time").isNull() |
    col("data.tipo_chave").isNull()
)

df_roteado = df_parsed.withColumn("is_bad_record", condicao_erro)
df_bad = df_roteado.filter(col("is_bad_record")).selectExpr("value_str as value")
df_good = df_roteado.filter(~col("is_bad_record")).select("data.*")

query_dlq = df_bad.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", KAFKA_TOPIC_DLQ) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "dlq") \
    .start()

df_with_watermark = df_good.withWatermark("event_time", "1 minute")
df_window_counts = df_with_watermark \
    .groupBy(window(col("event_time"), "5 minutes"), col("tipo_chave")) \
    .agg(count("*").alias("qtd_transacoes"))

def update_redis_from_window(df_batch, batch_id):
    if df_batch.count() == 0:
        return
    pdf = df_batch.toPandas()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    pipeline = r.pipeline()
    for _, row in pdf.iterrows():
        tipo = int(row["tipo_chave"])
        chave = f"feature_store:tipo_chave:{tipo}:qtd_5min"
        pipeline.setex(chave, 360, int(row["qtd_transacoes"]))
    pipeline.execute()
    print(f">>> [REDIS] Atualizadas {len(pdf)} janelas de 5min, batch {batch_id}")

query_redis = df_window_counts.writeStream \
    .foreachBatch(update_redis_from_window) \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "redis_window") \
    .start()

query_datalake = df_good.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", OUTPUT_PATH_PARQUET) \
    .option("checkpointLocation", CHECKPOINT_LOCATION + "parquet") \
    .trigger(processingTime="10 seconds") \
    .start()

print(">>> [INFO] Todas as queries de streaming iniciadas. Aguardando...")
spark.streams.awaitAnyTermination()