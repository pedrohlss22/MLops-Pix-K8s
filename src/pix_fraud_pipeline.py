import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service.datalake.svc.cluster.local:5000")
MLFLOW_S3_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT", "http://bucket-minio.datalake.svc.cluster.local:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
MINIO_PROCESSED_PATH = "s3a://processed/pix_history_v4/"

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
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(">>> [INFO] Lendo histórico processado do MinIO...")
try:
    df_raw = spark.read.parquet(MINIO_PROCESSED_PATH)
except Exception as e:
    print(f">>> [ERRO] Não foi possível ler dados: {e}")
    spark.stop()
    exit(1)

df = df_raw.select(
    "valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave", "is_fraude"
).na.drop()

print(f">>> [INFO] Total de registros válidos para treino: {df.count()}")

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

    evaluator = BinaryClassificationEvaluator(labelCol="is_fraude", rawPredictionCol="rawPrediction", metricName="areaUnderPR")
    pr_auc = evaluator.evaluate(predictions)

    mlflow.log_metric("pr_auc", pr_auc)
    print(f">>> [ML] Precision-Recall AUC: {pr_auc:.4f}")

    mlflow.spark.log_model(
    model, 
    "pix_fraud_rf_model",
    registered_model_name="pix_fraud_prod"
    )

spark.stop()
print(">>> Fim do treinamento.")