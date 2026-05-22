import os
import json
import pandas as pd
import redis
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import mlflow.pyfunc
from contextlib import asynccontextmanager
from prometheus_fastapi_instrumentator import Instrumentator
from kafka import KafkaProducer

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service.datalake.svc.cluster.local:5000")
MLFLOW_S3_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT", "http://bucket-minio.datalake.svc.cluster.local:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-service.datalake.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "pix-cluster-kafka-bootstrap:9092")
KAFKA_USER = os.getenv("KAFKA_USER", "spark-kafka-user")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

MODEL_NAME = "pix_fraud_prod"

os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT
os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_KEY

pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
r = redis.Redis(connection_pool=pool)

model = None
producer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global model, producer
    try:
        MODEL_URI = f"models:/{MODEL_NAME}/latest"
        model = mlflow.pyfunc.load_model(MODEL_URI)
    except Exception as e:
        print(f"ERRO CRÍTICO: Falha ao carregar do Registry. Detalhes: {e}")
        
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USER,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            linger_ms=5
        )
    except Exception as e:
        print(f"ERRO: Falha ao conectar no Kafka. Detalhes: {e}")
        
    yield
    pool.disconnect()
    if producer:
        producer.close()

app = FastAPI(title="Motor de Anti-Fraude PIX - Hot Path", lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

class TransacaoPix(BaseModel):
    valor_pix: float
    hora_transacao: int
    score_conta_origem: int
    tipo_chave: int

def enviar_para_kafka(topic: str, payload: dict):
    if producer:
        payload["event_time"] = datetime.utcnow().isoformat() + "Z"
        producer.send(topic, payload)

@app.post("/predict")
def prever_fraude(transacao: TransacaoPix, background_tasks: BackgroundTasks):
    if model is None:
        raise HTTPException(status_code=503, detail="Modelo preditivo não disponível.")

    qtd_recente = 0
    try:
        qtd_redis = r.get(f"feature_store:tipo_chave:{transacao.tipo_chave}:qtd_5min")
        qtd_recente = int(qtd_redis) if qtd_redis else 0
    except redis.exceptions.ConnectionError:
        print("Aviso: Redis indisponível. Prosseguindo com fallback.")

    input_data = pd.DataFrame([{
        "valor_pix": transacao.valor_pix,
        "hora_transacao": transacao.hora_transacao,
        "score_conta_origem": transacao.score_conta_origem,
        "tipo_chave": transacao.tipo_chave
    }])
    
    predicao = model.predict(input_data)[0]
    
    if qtd_recente > 10:
        predicao = 1
        motivo = "Alta frequência recente"
    else:
        motivo = "Modelo base"

    background_tasks.add_task(enviar_para_kafka, "transacoes-pix", transacao.model_dump())

    return {
        "transacao_processada": transacao.model_dump(),
        "alerta_fraude": bool(predicao),
        "acao_recomendada": "BLOQUEADO (FRAUDE)" if predicao == 1 else "APROVADO",
        "feature_redis": qtd_recente,
        "motivo": motivo
    }

@app.get("/health")
def health_check():
    return {"status": "API Online", "model_name": MODEL_NAME, "model_status": "Loaded" if model else "Not Loaded"}