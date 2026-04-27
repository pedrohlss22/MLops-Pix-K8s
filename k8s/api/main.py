import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow.pyfunc

os.environ["MLFLOW_TRACKING_URI"] = "http://mlflow-service.datalake.svc.cluster.local:5000"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://bucket-minio.datalake.svc.cluster.local:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

app = FastAPI(title="Motor de Anti-Fraude PIX", description="API de Inferência MLOps")

RUN_ID = "ea6a6f5910e34050855eb670e3f2377c"
MODEL_URI = f"runs:/{RUN_ID}/pix_fraud_rf_model"

print(f">>> [INFO] Carregando Modelo do MLflow: {MODEL_URI}...")
try:
    model = mlflow.pyfunc.load_model(MODEL_URI)
    print(">>> [INFO] Modelo carregado com sucesso na memória!")
except Exception as e:
    print(f"Erro ao carregar modelo: {e}")

class TransacaoPix(BaseModel):
    valor_pix: float
    hora_transacao: int
    score_conta_origem: int
    tipo_chave: int

@app.post("/predict")
def prever_fraude(transacao: TransacaoPix):
    try:
        input_data = pd.DataFrame([{
            "valor_pix": transacao.valor_pix,
            "hora_transacao": transacao.hora_transacao,
            "score_conta_origem": transacao.score_conta_origem,
            "tipo_chave": transacao.tipo_chave
        }])
        
        predicao = model.predict(input_data)
        
        resultado = int(predicao[0])
        
        status = "BLOQUEADO (FRAUDE)" if resultado == 1 else "APROVADO"
        
        return {
            "transacao_processada": transacao.dict(),
            "alerta_fraude": resultado == 1,
            "acao_recomendada": status
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "API Online", "modelo_carregado": RUN_ID}