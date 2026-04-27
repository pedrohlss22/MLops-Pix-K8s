import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder \
    .appName("PIX-Fraud-Streaming-Enterprise") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://bucket-minio.datalake.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(">>> [INFO] Motor de Streaming Enterprise Iniciado.")

schema_transacao = StructType([
    StructField("valor_pix", DoubleType(), True),
    StructField("hora_transacao", IntegerType(), True),
    StructField("score_conta_origem", IntegerType(), True),
    StructField("tipo_chave", IntegerType(), True),
    StructField("is_fraude", IntegerType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pix-cluster-kafka-bootstrap:9092") \
    .option("subscribe", "transacoes-pix") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as value_str") \
    .withColumn("data", from_json(col("value_str"), schema_transacao))

condicao_erro = (
    col("data.valor_pix").isNull() | 
    col("data.hora_transacao").isNull() | 
    col("data.score_conta_origem").isNull() | 
    col("data.tipo_chave").isNull()
)

df_roteado = df_parsed.withColumn("is_bad_record", condicao_erro)

print(">>> [INFO] Rota DLQ Ativada (Monitorando erros...)")
df_bad = df_roteado.filter(col("is_bad_record")) \
    .selectExpr("value_str as value")

query_dlq = df_bad.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pix-cluster-kafka-bootstrap:9092") \
    .option("topic", "transacoes-pix-dlq") \
    .option("checkpointLocation", "s3a://processed/checkpoints_pix_dlq_v1/") \
    .start()

print(">>> [INFO] Rota Principal Ativada (Processando transações válidas...)")
df_good = df_roteado.filter(~col("is_bad_record")).select("data.*")

assembler = VectorAssembler(
    inputCols=["valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave"], 
    outputCol="features"
)
df_features = assembler.transform(df_good)

query_datalake = df_features.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://processed/pix_history_v2/") \
    .option("checkpointLocation", "s3a://processed/checkpoints_pix_good_v1/") \
    .start()

spark.streams.awaitAnyTermination()