import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import mlflow
import mlflow.spark

mlflow.set_tracking_uri("http://mlflow-service.datalake.svc.cluster.local:5000")
mlflow.set_experiment("Deteccao_Fraude_PIX")

spark = SparkSession.builder \
    .appName("MLOps-PIX-Fraud-Pipeline") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://bucket-minio.datalake.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

print(">>> [INFO] Spark & MLflow Iniciados. Tema: Fraude PIX.")

data = [
    (150.0, 14, 85, 1, 0),
    (4500.0, 3, 12, 3, 1),
    (45.0, 19, 90, 2, 0),
    (8000.0, 2, 8, 3, 1),
    (1200.0, 10, 75, 1, 0),
    (5000.0, 4, 15, 3, 1),
    (20.0, 15, 95, 2, 0)
]
columns = ["valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave", "is_fraude"]
df = spark.createDataFrame(data, columns)

df.write.mode("overwrite").csv("s3a://raw/pix_raw")
print(">>> [ETL] Transações brutas salvas no MinIO (raw).")

assembler_etl = VectorAssembler(inputCols=["valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave"], outputCol="features")
df_features = assembler_etl.transform(df)
df_features.write.mode("overwrite").parquet("s3a://processed/pix_features.parquet")
print(">>> [ETL] Features extraídas e salvas no MinIO em Parquet (processed).")

train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)

with mlflow.start_run():
    print(">>> [ML] Treinando modelo Random Forest com Pipeline MLOps...")
    
    num_trees = 20
    max_depth = 7
    mlflow.log_param("num_trees", num_trees)
    mlflow.log_param("max_depth", max_depth)
    
    assembler_ml = VectorAssembler(inputCols=["valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave"], outputCol="features")
    rf = RandomForestClassifier(featuresCol="features", labelCol="is_fraude", numTrees=num_trees, maxDepth=max_depth)
    
    pipeline = Pipeline(stages=[assembler_ml, rf])
    model = pipeline.fit(train_data)

    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(labelCol="is_fraude", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    
    mlflow.log_metric("accuracy", accuracy)
    print(f">>> [ML] Modelo treinado! Acurácia: {accuracy:.2f}")
    
    mlflow.spark.log_model(model, "pix_fraud_rf_model")
    print(">>> [MLOps] Modelo de Fraude registrado no MLflow e salvo no MinIO!")

spark.stop()
print(">>> [INFO] Pipeline End-to-End Concluído.")