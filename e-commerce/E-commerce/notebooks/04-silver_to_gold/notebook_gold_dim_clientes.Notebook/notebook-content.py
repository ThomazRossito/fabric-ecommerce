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

# # 🥇 Gold: Dimensão Clientes (SCD Type 2)

# MARKDOWN ********************

# ## Descrição
# 
# Criação da dimensão de clientes para camada Gold com Slowly Changing Dimension Type 2.
# 
# ### Funcionalidades:
# 
# 1. **🔑 Surrogate Key** - Cliente_SK (auto-incremento)
# 2. **📅 SCD Type 2** - Rastreamento de mudanças históricas
# 3. **🎯 Campos de Negócio** - Dados demográficos do cliente
# 4. **🏷️ Auditoria** - Metadados de rastreabilidade
# 
# ### Schema:
# 
# | Coluna | Tipo | Descrição |
# |--------|------|----------|
# | cliente_sk | BIGINT | Surrogate key (PK) |
# | cliente_id | STRING | ID natural do cliente |
# | nome | STRING | Nome do cliente |
# | email | STRING | Email |
# | cidade | STRING | Cidade |
# | estado | STRING | UF |
# | data_inicio | TIMESTAMP | Início da validade |
# | data_fim | TIMESTAMP | Fim da validade (NULL = ativo) |
# | ativo | BOOLEAN | TRUE = registro atual |
# | ingestion_timestamp | TIMESTAMP | Timestamp de ingestão |
# | camada | STRING | "gold" |
# 
# ---

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
# ##### Janeiro 2026


# MARKDOWN ********************

# ## 📦 Bibliotecas

# CELL ********************

%run utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    current_timestamp, 
    lit, 
    col, 
    row_number, 
    monotonically_increasing_id
)
from pyspark.sql.window import Window
import time

print("="*30)
print("🥇 GOLD - DIMENSÃO CLIENTES 🥇")
print("="*30)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⚙️ Configurações

# CELL ********************

schema_silver = "silver"
schema_gold = "gold"

table = "clientes"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_gold}")

print(f"⚙️ Configurações ⚙️")

print(f"📥 Silver: {schema_silver}.tb_{table}")
print(f"💾 Gold: {schema_gold}.dim_{table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄 Processamento

# CELL ********************

start_time = time.time()

print("📥 Lendo Silver...")

# Ler do Silver
full_table = f"{schema_silver}.tb_{table}"
df_silver = read_data(full_table, "table")

# Adicionar Surrogate Key
window_spec = Window.orderBy("cliente_id")
df_dim = df_silver.withColumn("cliente_sk", row_number().over(window_spec))

param_options = {
    # Adicionar campos SCD Type 2
    "data_inicio": current_timestamp(),
    "data_fim": lit(None).cast("timestamp"),
    "ativo": lit(True),
    # Adicionar campos auditoria
    "ingestion_timestamp": current_timestamp(),
    "camada": lit("gold")
}

df_dim = df_dim.withColumns(param_options)

# Reordenar colunas
df_dim = df_dim.select(
    "cliente_sk", "cliente_id", "nome", 
    "email", "cidade", "estado",
    "data_inicio", "data_fim", "ativo",
    "ingestion_timestamp", "camada"
)

count_records = df_dim.count()

print(f"📊 Clientes únicos: {count_records:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 💾 Salvar Dimensão

# CELL ********************

table_gold = f"{schema_gold}.dim_{table}"

write_data(df_dim, "delta", "overwrite", table_gold)

print(f"✅ Dimensão salva: {table_gold}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📋 Relatório Final Consolidado

# CELL ********************

total_duration = time.time() - start_time

print("\n" + "="*80)
print("📊 RELATÓRIO - DIM_CLIENTES")
print("="*80)
print(f"✅ Registros: {count_records:,}")
print(f"⏱️  Duração: {total_duration:.2f}s")
print(f"💾 Tabela: {table_gold}")
print("="*80)

mssparkutils.notebook.exit({
    "success": True,
    "records": count_records,
    "duration": total_duration
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
