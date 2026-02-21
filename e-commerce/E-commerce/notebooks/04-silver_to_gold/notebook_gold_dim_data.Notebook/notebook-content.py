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

# # 🥇 Gold: Dimensão Data (Calendar Table)

# MARKDOWN ********************

# ## Descrição
# 
# Criação da dimensão temporal (calendar table) para camada Gold.
# 
# ### Funcionalidades:
# 
# 1. **📅 Calendar Table** - Todas as datas entre 2023-2025
# 2. **🔑 Surrogate Key** - Data_SK (auto-incremento)
# 3. **📊 Atributos Temporais** - Ano, mês, trimestre, dia da semana, etc
# 4. **🏷️ Auditoria** - Metadados de rastreabilidade
# 
# ### Schema:
# 
# | Coluna | Tipo | Descrição |
# |--------|------|----------|
# | data_sk | BIGINT | Surrogate key (PK) |
# | data_completa | DATE | Data completa |
# | ano | INT | Ano (2023-2025) |
# | mes | INT | Mês (1-12) |
# | trimestre | INT | Trimestre (1-4) |
# | dia_mes | INT | Dia do mês (1-31) |
# | dia_semana | INT | Dia da semana (1=Dom, 7=Sáb) |
# | nome_mes | STRING | Nome do mês |
# | nome_dia_semana | STRING | Nome do dia |
# | fim_semana | BOOLEAN | TRUE = sábado/domingo |
# | ingestion_timestamp | TIMESTAMP | Timestamp |
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
    current_timestamp, lit, col, to_date, year, month, quarter,
    dayofmonth, dayofweek, date_format, when, row_number
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import time

print("="*40)
print("🥇 GOLD - DIMENSÃO DATA (CALENDAR) 🥇")
print("="*40)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⚙️ Configurações

# CELL ********************

schema_gold = "gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_gold}")

# Range de datas (2023-2026)
data_inicio = datetime(2023, 1, 1)
data_fim = datetime(2026, 12, 31)

print("⚙️ Configurações ⚙️")

print(f"📅 Range: {data_inicio.date()} a {data_fim.date()}")
print(f"💾 Gold: {schema_gold}.dim_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄 Geração de Calendar Table

# CELL ********************

start_time = time.time()

print("📅 Gerando calendar table...")

# Gerar lista de datas
delta = data_fim - data_inicio
dates = [(data_inicio + timedelta(days=i),) for i in range(delta.days + 1)]

# Criar DataFrame
df_dates = spark.createDataFrame(dates, ["data_completa"])

# Adicionar atributos temporais
df_dim = df_dates.select(
    col("data_completa"),
    year("data_completa").alias("ano"),
    month("data_completa").alias("mes"),
    quarter("data_completa").alias("trimestre"),
    dayofmonth("data_completa").alias("dia_mes"),
    dayofweek("data_completa").alias("dia_semana"),
    date_format("data_completa", "MMMM").alias("nome_mes"),
    date_format("data_completa", "EEEE").alias("nome_dia_semana")
)

# Adicionar flag fim de semana
df_dim = df_dim.withColumn("fim_semana", when(col("dia_semana").isin([1, 7]), True).otherwise(False))

# Adicionar Surrogate Key
window_spec = Window.orderBy("data_completa")
df_dim = df_dim.withColumn("data_sk", row_number().over(window_spec))

# Adicionar auditoria
df_dim = (df_dim.withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("camada", lit("gold")))

# Reordenar colunas
df_dim = df_dim.select(
    "data_sk", "data_completa", "ano", 
    "mes", "trimestre", "dia_mes", "dia_semana", 
    "nome_mes", "nome_dia_semana", "fim_semana",
    "ingestion_timestamp", "camada"
)

count_records = df_dim.count()

print(f"📊 Total de datas geradas: {count_records:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 💾 Salvar Dimensão

# CELL ********************

table_gold = f"{schema_gold}.dim_data"

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
print("📊 RELATÓRIO - DIM_DATA")
print("="*80)
print(f"✅ Registros: {count_records:,}")
print(f"📅 Range: {data_inicio.date()} a {data_fim.date()}")
print(f"⏱️ Duração: {total_duration:.2f}s")
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
