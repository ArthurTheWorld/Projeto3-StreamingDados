Streaming de Dados Financeiros com Kafka, Spark e BigQuery

Este projeto implementa um pipeline de streaming de dados financeiros em tempo real.

A aplicação captura cotações da bolsa utilizando a API do Yahoo Finance, publica os dados em um tópico Apache Kafka e os consome com Apache Spark Structured Streaming, gravando os resultados na camada Silver de um Data Lake armazenado no Google Cloud Storage (GCS) ou BigQuery.

Arquitetura

Fluxo do pipeline:

Yahoo Finance → Kafka → Spark Structured Streaming → GCS / BigQuery

Estrutura do Projeto
projeto_3_streaming_dados_financeiros/
│
├── app/
│   ├── yfinance_listener.py
│   ├── gcs_silver_layer_writer.py
│   ├── gcs_reader.py
│   ├── carteira.txt
│   └── requirements.txt
│
├── jars/
│   └── gcs-connector-*.jar
│
├── docker-compose.yaml
├── Dockerfile
└── README.md


Descrição dos principais arquivos:

yfinance_listener.py
Publicador Kafka que coleta cotações em tempo real.

gcs_silver_layer_writer.py
Consumidor Spark Streaming que grava os dados na camada Silver.

gcs_reader.py
Script auxiliar para leitura e debug dos dados.

carteira.txt
Lista de ativos monitorados.

Tecnologias Utilizadas

Apache Kafka

Apache Spark Structured Streaming

Google Cloud Storage (GCS)

BigQuery (opcional)

Docker e Docker Compose

Python 3.10+

Yahoo Finance API (yfinance)

Pré-requisitos

Antes de executar:

Docker instalado

Docker Compose instalado

Conta no Google Cloud (caso utilize GCS ou BigQuery)

Configuração do Ambiente

Este projeto utiliza credenciais externas que não devem ser incluídas no repositório.

Crie um arquivo .env na raiz do projeto:

GOOGLE_APPLICATION_CREDENTIALS=/caminho/para/credencial.json
PROJECT_ID=seu_projeto
BUCKET_NAME=seu_bucket


Nunca envie:

.env
*.json

Como Executar
1. Baixar o conector GCS

Baixe o arquivo:

https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.5/gcs-connector-hadoop3-2.2.5-shaded.jar

Coloque dentro da pasta:

jars/

2. Subir a infraestrutura
docker-compose up -d

3. Executar o produtor (Yahoo Finance → Kafka)
docker exec -it app python app/yfinance_listener.py

4. Executar o consumidor (Kafka → GCS / BigQuery)
docker exec -it app spark-submit \
--jars jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
app/gcs_silver_layer_writer.py


Os dados serão gravados de forma particionada, respeitando o modo de streaming append.

Camadas do Data Lake

Este projeto utiliza o conceito de arquitetura em camadas:

Bronze → dados brutos

Silver → dados tratados e estruturados

Gold → dados prontos para análise (não implementado neste projeto)
