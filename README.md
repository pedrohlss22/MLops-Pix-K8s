Detecção de Fraude em PIX: Arquitetura Dual-Path com MLOps e Streaming

Este é um laboratório prático de engenharia de dados e MLOps desenhado para resolver um dos cenários mais rigorosos do sistema financeiro: a detecção de fraudes em transações PIX em tempo real.

O maior desafio antifraude do PIX é a latência. Com SLAs de resposta na casa dos milissegundos, consultar bancos de dados relacionais para reconstruir o histórico do cliente no momento da transação não é viável. Para contornar esse gargalo, esta arquitetura implementa um padrão Dual-Path (Fluxo Quente e Fluxo Frio), unindo processamento distribuído, Feature Stores em memória e deploy automatizado de modelos.
🏗️ Desenho da Arquitetura

O ecossistema é conteinerizado e orquestrado no Azure Kubernetes Service (AKS), com provisionamento elástico gerenciado via Terraform.
1. Fluxo Rápido (Hot Path: API + Feature Store)

A linha de frente do sistema. Responde à transação em tempo real.

    Inferência na Borda: Uma API em FastAPI carrega o modelo de Machine Learning dinamicamente via MLflow Model Registry. O modelo reside na memória RAM do pod para inferência I/O-free.

    Feature Store Efêmera: A API consulta um cluster Redis para resgatar agregados comportamentais (ex: "Quantas chaves distintas esse usuário usou nos últimos 5 minutos?"). O Redis entrega esse contexto em <1ms, permitindo que o modelo avalie o risco baseado em histórico recente sem atrasar o PIX.

    Observabilidade: A API está instrumentada nativamente para o Prometheus, exportando métricas de latência e contagem de bloqueios/aprovações via /metrics.

2. Fluxo de Streaming (Cold Path: Mensageria e Data Lake)

O motor assíncrono que processa os dados, retroalimenta o cache e prepara o terreno para retreinos.

    Ingestão Segura: Dados brutos caem em um cluster Apache Kafka gerenciado pelo operador Strimzi (com autenticação SCRAM-SHA-512).

    Processamento (Spark Structured Streaming): Um job contínuo em PySpark consome os tópicos e realiza um roteamento triplo:

        Atualização da Feature Store: Calcula agregações em janelas temporais (Window Functions) e injeta os novos contadores no Redis com um TTL (Time-to-Live).

        Data Lake (Historical): Limpa, tipa e salva os dados válidos em formato colunar (Parquet) no MinIO (S3-compatible) para o treinamento noturno.

        Dead Letter Queue (DLQ): Payloads nulos ou corrompidos são interceptados e desviados para um tópico de erro, garantindo que o streaming nunca trave.

⚙️ Diferenciais de Engenharia e MLOps

    Model Registry (Decoupled Deployment): A API não possui IDs de modelos hardcoded. O CronJob noturno do Spark treina novos dados e registra o modelo (pix_fraud_prod) no MLflow. A API sempre busca a versão latest do Registry no startup, separando o ciclo de vida do software do ciclo do modelo.

    DevSecOps e Imutabilidade: O pipeline utiliza imagens Spark customizadas (Dockerfile.spark) construídas rootless (UID 1001), mitigando vulnerabilidades de execução e falhas de Kerberos. Drivers do S3 e conectores do Kafka são embutidos no classpath via build stage.

    Qualidade e CI/CD: A esteira do GitHub Actions exige a aprovação em testes automatizados (pytest com Mocks e Local Spark Sessions) antes de compilar imagens, subir para o Azure Container Registry (ACR) e aplicar Rolling Updates no cluster.

🛠️ Stack Tecnológica

    Cloud & IaC: Azure (AKS, ACR), Terraform

    Processamento: Apache Spark (PySpark), Apache Kafka (Strimzi KRaft)

    Armazenamento: MinIO (Object Storage), Redis (In-Memory Data Grid)

    ML, API & QA: Python, FastAPI, MLflow, Scikit-Learn, Pytest

    DevOps: Kubernetes, Docker, GitHub Actions, Prometheus

📂 Estrutura do Repositório

├── .github/workflows/       # CI/CD (Testes e Deploy no AKS)
├── k8s/                     # Manifestos Kubernetes (Declarative State)
│   ├── api/                 # Deployment, Service e ServiceMonitor da API
│   ├── kafka/               # Cluster Strimzi, Tópicos e Autenticação
│   ├── mlflow/              # Deployment do Model Registry
│   ├── redis/               # StatefulSet da Feature Store
│   └── spark/               # Streaming Deployment e CronJob de Treinamento
├── src/                     # Código-fonte das aplicações
│   ├── api/main.py          # Backend FastAPI e Inferência
│   ├── pix_streaming.py     # Agregação contínua em tempo real
│   └── pix_fraud_train.py   # Pipeline de ML (RandomForest + PR-AUC)
├── tests/                   # Suíte de integração e testes unitários
├── terraform/               # Provisionamento da Azure (AKS Autoscaling, ACR)
├── docker-compose.yml       # Emulação do ambiente local para desenvolvimento
├── Dockerfile               # Imagem da API
└── Dockerfile.spark         # Imagem baseada na Bitnami, segura e customizada

🚀 Como Executar
Opção A: Ambiente Local (Desenvolvimento)

Ideal para validar a integração e rodar testes sem custos de nuvem.

# Sobe Kafka, Redis, MinIO e MLflow
docker-compose up -d

# Executa a suíte de testes unitários e de integração
pytest tests/


Opção B: Deploy em Produção (Azure AKS)

1. Suba a Infraestrutura (Terraform)

cd terraform
terraform init
terraform apply -auto-approve

2. Autentique no Cluster

az aks get-credentials --resource-group ProjetoMLOps-RG-v3 --name ClusterMLOps

3. Deploy do Data Plane (K8s)
Respeite a ordem de dependência dos manifestos:

# 1. Base e Armazenamento
kubectl apply -f k8s/minio/
kubectl apply -f k8s/redis/
kubectl apply -f k8s/mlflow/

# 2. Mensageria (Aguarde o broker ficar Ready)
kubectl apply -f k8s/kafka/

# 3. Processamento Contínuo
kubectl apply -f k8s/spark/streaming-deployment.yaml

# 4. Treinamento Inicial (Obrigatório para gerar o modelo V1 no MLflow)
kubectl create job init-treino --from=cronjob/spark-treino-diario -n datalake

# 5. Motor de Inferência (A API consumirá o modelo recém-treinado)
kubectl apply -f k8s/api/