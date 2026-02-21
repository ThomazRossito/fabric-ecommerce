# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ff4c5f56-7490-4a4b-a0b1-115bd42aaaf2",
# META       "default_lakehouse_name": "TARN_LH_DEV",
# META       "default_lakehouse_workspace_id": "e1e7a468-d318-49d0-b238-287c09020d88",
# META       "known_lakehouses": [
# META         {
# META           "id": "ff4c5f56-7490-4a4b-a0b1-115bd42aaaf2"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 📊 Monitoramento: Log de Execução do Pipeline Medallion
# 
# ## Descrição
# 
# Este notebook é a central de governança e observabilidade do projeto. Ele consolida os metadados de execução capturados pelo Orquestrador e os persiste em uma tabela Delta para análise histórica e dashboards de monitoramento.
# 
# ### Funcionalidades:
# 
# 1. **📥 Captura de Metadados** - Processa os resultados de todas as camadas (Bronze → Gold)
# 2. **📈 Métricas de Performance** - Registra o tempo exato de cada atividade
# 3. **🔍 Gestão de Erros** - Armazena mensagens detalhadas para facilitar o troubleshooting
# 4. **💾 Persistência Delta** - Mantém o histórico completo de execuções para o Power BI
# 
# ### Detalhes do Log:
# 
# - `run_id`: Identificador único da execução
# - `nome_notebook`: Identifica qual etapa foi executada
# - `registros_processados`: Volume de dados por tabela
# - `duracao_segundos`: Performance granular por atividade
# - `mensagem_erro`: Descrição técnica de possíveis falhas
# 
# ### Arquitetura de Monitoramento:
# ```
# Orchestrator (results) → notebook_monitoramento_log → monitoramento.tb_monitoramento_log (Delta)
# ```


# MARKDOWN ********************

# ## 👨‍💻 **Autor** 👨‍💻
# 
# > **Estruturado por:** <span style="font-size: 1.5em;">Thomaz Antonio Rossito Neto</span>
# 
# <b><span style="font-size: 1.2em; font-style: italic;">🏆 Profissional Certificado Databricks</span></b>
# 
# <div style="display: flex; flex-wrap: wrap; align-items: center; margin-bottom: 20px;">
#     <img src="https://api.accredible.com/v1/frontend/credential_website_embed_image/badge/125134719" width="135" style="margin-right: -25px;"/>
#     <img src="https://api.accredible.com/v1/frontend/credential_website_embed_image/badge/167127257" width="135" style="margin-right: -25px;"/>
#     <img src="https://api.accredible.com/v1/frontend/credential_website_embed_image/badge/169321258" width="135" style="margin-right: -25px;"/>
#     <img src="https://api.accredible.com/v1/frontend/credential_website_embed_image/badge/125134780" width="135" style="margin-right: -25px;"/>
#     <img src="https://api.accredible.com/v1/frontend/credential_website_embed_image/badge/157011932" width="135"/>
# </div>
# 
# <div style="display: flex; flex-wrap: wrap; align-items: center;">
#     <img src="https://images.credly.com/images/af27ef78-6967-4082-b6ce-8111b1af47e1/MTA_Database_Fundamentals-01.png" width="115" style="margin-right: 10px;"/>
#     <img src="https://images.credly.com/size/340x340/images/70eb1e3f-d4de-4377-a062-b20fb29594ea/azure-data-fundamentals-600x600.png" width="115" style="margin-right: 10px;"/>
#     <img src="https://images.credly.com/images/bb4a3c26-9f24-4913-9ae5-7331a3d657a6/MCSA-Data-Engineering-with-Azure_2019.png" width="115" style="margin-right: 10px;"/>
#     <img src="https://images.credly.com/images/7e080b6a-0494-4b3e-a016-23f73566495f/MCSE-Data-Management-and-Analytics_2019.png" width="115"/>
# </div>
# 
# <br>
# 
# [Certificações Databricks](https://credentials.databricks.com/profile/thomazantoniorossitoneto39867/wallet)              
# [Certificações Microsoft](https://www.credly.com/users/thomaz-antonio-rossito-neto/badges#credly)
# 
# ---
# 
# ### Data de Criação
# ##### Fevereiro 2026


# MARKDOWN ********************

# ## ⚙️ Configurações e Parâmetros
# 
# Definição dos parâmetros de entrada que o Orquestrador enviará e configuração dos imports necessários.ros

# CELL ********************

%run utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

dag_results_raw = "" # Receberá o JSON do orquestrador
run_id = ""          # ID opcional da execução

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Bibliotecas

# CELL ********************

import json
import ast
from datetime import datetime
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    BooleanType, 
    DoubleType, 
    LongType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tratamento de Entrada
# 
# Converte a string JSON recebida para um dicionário Python utilizável

# CELL ********************

print("💾 Iniciando Persistência de Logs de Monitoramento...")

if isinstance(dag_results_raw, str) and dag_results_raw.strip() != "":
    try:
        results_dict = json.loads(dag_results_raw)
    except:
        # Fallback para aspas simples (ast) se o json falhar
        results_dict = ast.literal_eval(dag_results_raw)
else:
    results_dict = dag_results_raw

# Configurações de Destino
schema_name = "monitoramento"
table_name = "tb_monitoramento_log"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄 Processamento e Persistência
# 
# Lógica para transformar o dicionário de resultados em um DataFrame estruturado e salvar na tabela Delta de monitoramento.

# CELL ********************

print(f"🚀 Preparação de dados para o DataFrame...")

log_data = []
timestamp_monitoramento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if isinstance(results_dict, dict):
    for activity, detail in results_dict.items():
        # Extrai o nome da camada (Bronze/Silver/Gold) para filtros no dashboard
        camada_nome = activity.split('_')[1] if '_' in activity else activity
        
        log_data.append({
            "run_id": str(run_id),
            "nome_notebook": str(activity),
            "status": "Success" if detail.get("success") else "Failed",
            "sucesso": bool(detail.get("success", False)),
            "registros_processados": int(detail.get("records", 0)),
            "duracao_segundos": float(detail.get("duration", 0)),
            "mensagem_erro": str(detail.get("error", "")),
            "camada": str(camada_nome),
            "timestamp_execucao": timestamp_monitoramento
        })

# Definição de Schema explícito para performance e tipagem correta
schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("nome_notebook", StringType(), True),
    StructField("status", StringType(), True),
    StructField("sucesso", BooleanType(), True),
    StructField("registros_processados", LongType(), True),
    StructField("duracao_segundos", DoubleType(), True),
    StructField("mensagem_erro", StringType(), True),
    StructField("camada", StringType(), True),
    StructField("timestamp_execucao", StringType(), True)
])

if log_data:
    df_log = spark.createDataFrame(log_data, schema=schema)

    # Escrita Delta em modo Append para manter o histórico de execuções
    write_data(df_log, "delta", "append", f"{schema_name}.{table_name}")
    
    print(f"✅ {len(log_data)} logs persistidos com sucesso em {schema_name}.{table_name}")

    spark.sql("OPTIMIZE monitoramento.tb_monitoramento_log")
    spark.sql("VACUUM monitoramento.tb_monitoramento_log RETAIN 168 HOURS")
    
    print("✅ OPTIMIZE e VACUUM executados com sucesso!!!")
    print("✅ Monitoramento atualizado.")    

else:
    print("⚠️ Nenhum dado de log encontrado para processar.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
