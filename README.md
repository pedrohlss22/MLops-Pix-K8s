# Detecção de Fraude em PIX: MLOps & Streaming no Kubernetes

Este é um laboratório prático de arquitetura de dados e MLOps focado em um dos cenários mais críticos do sistema financeiro: a detecção de fraudes em transações PIX.

O desafio técnico principal do PIX é a latência. Você tem poucos milissegundos para aprovar ou bloquear uma transação. Se a API de inferência precisar consultar um banco de dados relacional para montar o histórico do cliente a cada transação, o sistema entra em gargalo. 

Para resolver isso, a arquitetura foi desenhada separando o processamento em dois fluxos paralelos (Dual-Path): um para a resposta em tempo real e outro para o processamento contínuo dos dados.

## O Desenho da Arquitetura

Toda a infraestrutura roda de forma conteinerizada sobre um cluster **Azure Kubernetes Service (AKS)**.

### 1. O Fluxo Rápido (API + Feature Store)
A linha de frente do sistema. O objetivo aqui é responder rápido.
* A requisição do PIX bate numa API feita em **FastAPI**. O modelo de Machine Learning (`.pkl` / `.joblib`) fica carregado estaticamente na memória RAM do pod para inferência imediata.
* **O papel do Redis:** A API sozinha sofre de "amnésia" (ela só vê o PIX atual). Para saber se aquele usuário fez 10 transações nos últimos 5 minutos, a API faz um GET em um **Redis**. O Redis atua como uma *Feature Store* de baixa latência, entregando o contexto da transação em menos de 1ms para o modelo tomar a decisão.

### 2. O Fluxo de Streaming (A "Cozinha" dos Dados)
Enquanto a API responde aos usuários, os dados brutos caem em um cluster **Apache Kafka** (gerenciado via Strimzi Operator) para processamento assíncrono.
* Um job contínuo do **Spark Structured Streaming** consome os tópicos do Kafka e faz o roteamento triplo da informação:
  1. **Atualização da Feature Store:** O Spark calcula as agregações temporais em tempo real e atualiza os contadores no Redis (com um *Time-to-Live* de 10 minutos).
  2. **Data Lake (Cold Storage):** Os dados válidos são limpos, transformados em formato colunar (Parquet) e salvos em um cluster **MinIO** (S3-compatible). Esses dados formarão o histórico para o retreino noturno do modelo.
  3. **Dead Letter Queue (DLQ):** Payloads malformados ou com campos nulos não quebram o Spark. Eles são isolados e roteados para um tópico específico de DLQ no Kafka para análise posterior.

## Engenharia de Software e MLOps

O projeto não se limita apenas ao fluxo de dados, mas também em como o código chega em produção e como os modelos são gerenciados:

* **Model Registry:** Utilizamos o **MLflow** deployado no Kubernetes para rastrear os experimentos, salvar métricas e versionar os modelos de fraude aprovados.
* **CI/CD:** Toda vez que a API recebe uma atualização no código, o **GitHub Actions** compila uma nova imagem Docker, envia para o Azure Container Registry (ACR) e executa um *Rolling Update* no Kubernetes, garantindo zero downtime.
* **Infraestrutura como Código:** O provisionamento da nuvem (AKS, ACR, permissões) está mapeado na pasta `/terraform`, facilitando a recriação do ambiente.

## Stack Resumida
* **Processamento:** Apache Spark (PySpark), Apache Kafka
* **Armazenamento:** MinIO (Data Lake), Redis (Feature Store)
* **ML & API:** Python, FastAPI, MLflow, Scikit-Learn
* **DevOps/SRE:** Kubernetes (AKS), Docker, Terraform, GitHub Actions

## Estrutura do Código

```text
├── .github/workflows/       # Automação de deploy da API
├── k8s/                     # Manifestos do Kubernetes (Deployments, Services, ConfigMaps)
│   ├── api/                 # microsserviço FastAPI
│   ├── kafka/               # Cluster Kafka Strimzi
│   ├── minio/               # Armazenamento S3
│   ├── mlflow/              # Tracker de ML
│   ├── redis/               # Servidor da Feature Store
│   └── spark/               # Scripts de Streaming e Batch
├── src/                     # Lógica dos pipelines
│   ├── pix_streaming_pipeline.py  # Script de processamento em tempo real
│   └── pix_fraud_pipeline.py      # Script de treino do modelo
├── terraform/               # Configuração da infraestrutura Azure
└── Dockerfile               # Receita da imagem da API