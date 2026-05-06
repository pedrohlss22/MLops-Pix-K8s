import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark


MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service.datalake.svc.cluster.local:5000")
MLFLOW_S3_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT", "http://bucket-minio.datalake.svc.cluster.local:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
MINIO_RAW_PATH = "s3a://raw/pix_raw/*.csv"   # ou .parquet

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("Deteccao_Fraude_PIX")

spark = SparkSession.builder \
    .appName("MLOps-PIX-Fraud-Pipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", MLFLOW_S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(">>> [INFO] Lendo dados reais do MinIO (raw)...")
try:
    df_raw = spark.read.option("header", "true").csv(MINIO_RAW_PATH)
except Exception as e:
    print(f">>> [ERRO] Não foi possível ler dados: {e}")
    spark.stop()
    exit(1)

df = df_raw.select(
    "valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave", "is_fraude"
).na.drop()

print(f">>> [INFO] Total de registros: {df.count()}")

train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

with mlflow.start_run():
    num_trees = 50
    max_depth = 10
    mlflow.log_param("num_trees", num_trees)
    mlflow.log_param("max_depth", max_depth)

    assembler = VectorAssembler(
        inputCols=["valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave"],
        outputCol="features"
    )
    rf = RandomForestClassifier(featuresCol="features", labelCol="is_fraude",
                                numTrees=num_trees, maxDepth=max_depth)
    pipeline = Pipeline(stages=[assembler, rf])

    model = pipeline.fit(train_data)

    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="is_fraude", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    mlflow.log_metric("accuracy", accuracy)
    print(f">>> [ML] Acurácia: {accuracy:.4f}")

    mlflow.spark.log_model(model, "pix_fraud_rf_model")
    print(">>> [MLOps] Modelo registrado no MLflow")

spark.stop()
print(">>> Fim do treinamento.")