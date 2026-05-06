import os
import pandas as pd
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow.pyfunc

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-service.datalake.svc.cluster.local:5000")
MLFLOW_S3_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT", "http://bucket-minio.datalake.svc.cluster.local:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-service.datalake.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
RUN_ID = os.getenv("MODEL_RUN_ID", "ea6a6f5910e34050855eb670e3f2377c")

os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT
os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_KEY

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
MODEL_URI = f"runs:/{RUN_ID}/pix_fraud_rf_model"
print(f">>> Carregando modelo do MLflow: {MODEL_URI}")
model = mlflow.pyfunc.load_model(MODEL_URI)

app = FastAPI(title="Motor de Anti-Fraude PIX - Hot Path")

class TransacaoPix(BaseModel):
    valor_pix: float
    hora_transacao: int
    score_conta_origem: int
    tipo_chave: int

@app.post("/predict")
def prever_fraude(transacao: TransacaoPix):
    try:
        qtd_recente = r.get(f"feature_store:tipo_chave:{transacao.tipo_chave}:qtd_5min")
        qtd_recente = int(qtd_recente) if qtd_recente else 0
        input_data = pd.DataFrame([{
            "valor_pix": transacao.valor_pix,
            "hora_transacao": transacao.hora_transacao,
            "score_conta_origem": transacao.score_conta_origem,
            "tipo_chave": transacao.tipo_chave
        }])
        predicao = model.predict(input_data)[0]
        if qtd_recente > 10:
            predicao = 1
            motivo_adicional = "Alta frequência recente"
        else:
            motivo_adicional = "Modelo base"

        resultado = "BLOQUEADO (FRAUDE)" if predicao == 1 else "APROVADO"
        return {
            "transacao_processada": transacao.dict(),
            "alerta_fraude": bool(predicao),
            "acao_recomendada": resultado,
            "feature_redis": qtd_recente,
            "motivo": motivo_adicional
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "API Online", "run_id": RUN_ID}