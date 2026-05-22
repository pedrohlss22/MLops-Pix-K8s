from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from src.api.main import app

client = TestClient(app)

@patch("src.api.main.model")
@patch("src.api.main.r")
def test_prever_fraude_bloqueio_por_redis(mock_redis, mock_model):
    mock_model.predict.return_value = [0]
    
    mock_redis.get.return_value = "15"

    payload = {
        "valor_pix": 100.0,
        "hora_transacao": 14,
        "score_conta_origem": 90,
        "tipo_chave": 1
    }

    response = client.post("/predict", json=payload)
    data = response.json()

    assert response.status_code == 200
    assert data["alerta_fraude"] is True
    assert data["acao_recomendada"] == "BLOQUEADO (FRAUDE)"
    assert data["motivo"] == "Alta frequência recente"
    assert data["feature_redis"] == 15