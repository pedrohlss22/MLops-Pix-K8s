import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local") \
        .getOrCreate()

def test_condicao_erro_dlq(spark):
    dados = [
        (100.0, 14, 90, 1, "2026-05-12T10:00:00Z"),
        (None, 14, 90, 1, "2026-05-12T10:00:00Z"),
        (100.0, 14, 90, None, "2026-05-12T10:00:00Z")
    ]
    
    df = spark.createDataFrame(dados, ["valor_pix", "hora_transacao", "score_conta_origem", "tipo_chave", "event_time"])
    
    condicao_erro = (
        col("valor_pix").isNull() |
        col("event_time").isNull() |
        col("tipo_chave").isNull()
    )
    
    df_roteado = df.withColumn("is_bad_record", condicao_erro)
    df_bad = df_roteado.filter(col("is_bad_record"))
    df_good = df_roteado.filter(~col("is_bad_record"))
    
    assert df_good.count() == 1
    assert df_bad.count() == 2