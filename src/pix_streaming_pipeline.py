import os
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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
df_bad = df_roteado.filter(col("is_bad_record")).selectExpr("value_str as value")
df_good = df_roteado.filter(~col("is_bad_record")).select("data.*")

query_dlq = df_bad.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pix-cluster-kafka-bootstrap:9092") \
    .option("topic", "transacoes-pix-dlq") \
    .option("checkpointLocation", "s3a://processed/checkpoints_pix_dlq_v2/") \
    .start()

assembler = VectorAssembler(inputCols=["valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave"], outputCol="features")
df_features = assembler.transform(df_good)

query_datalake = df_features.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://processed/pix_history_v3/") \
    .option("checkpointLocation", "s3a://processed/checkpoints_pix_good_v3/") \
    .start()
def atualizar_feature_store(df_batch, batch_id):
    pdf = df_batch.toPandas()
    if not pdf.empty:
        r = redis.Redis(host='redis-service.datalake.svc.cluster.local', port=6379, db=0, decode_responses=True)
        
        for index, row in pdf.iterrows():
            chave_redis = f"feature_store:tipo_chave:{int(row['tipo_chave'])}:qtd_pix"

            r.incr(chave_redis)

            r.expire(chave_redis, 600) 
            
        print(f">>> [REDIS] Batch {batch_id} injetado na Feature Store com sucesso!")

print(">>> [INFO] Rota de Feature Store Ativada (Sincronizando com Redis...)")
query_redis = df_good.writeStream \
    .foreachBatch(atualizar_feature_store) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()