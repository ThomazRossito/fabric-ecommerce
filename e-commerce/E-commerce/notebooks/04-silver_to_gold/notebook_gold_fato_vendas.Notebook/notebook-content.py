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

# # 🥇 Gold: Fato Vendas (Fact Table)

# MARKDOWN ********************

# ## Descrição
# 
# Criação da tabela fato de vendas para camada Gold com JOINs nas dimensões.
# 
# ### Funcionalidades:
# 
# 1. **🔗 JOINs com Dimensões** - Relacionamento com dim_clientes, dim_data, dim_produtos
# 2. **📊 Métricas de Negócio** - Quantidade, valor total, receita
# 3. **🔑 Surrogate Keys** - Usa SKs das dimensões (não IDs naturais)
# 4. **🏷️ Auditoria** - Metadados de rastreabilidade
# 
# ### Arquitetura Star Schema:
# 
# ```
#         dim_clientes
#               |
#               | cliente_sk
#               |
#         fato_vendas -------- dim_produtos
#               |        produto_sk
#               | data_sk
#               |
#           dim_data
# ```
# 
# ### Schema:
# 
# | Coluna | Tipo | Descrição |
# |--------|------|----------|
# | venda_id | STRING | ID natural da venda (PK) |
# | data_sk | BIGINT | FK → dim_data |
# | cliente_sk | BIGINT | FK → dim_clientes |
# | produto_sk | BIGINT | FK → dim_produtos |
# | quantidade | INT | Quantidade vendida |
# | valor_unitario | DOUBLE | Preço unitário |
# | valor_total | DOUBLE | Quantidade × Valor unitário |
# | receita | DOUBLE | Valor total (métrica) |
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
    current_timestamp, 
    lit, 
    col, 
    to_date,
    sum as _sum, 
    avg, 
    count
)
import time

print("="*25)
print("🥇 GOLD - FATO VENDAS 🥇")
print("="*25)

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

table = "vendas"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_gold}")

print(f"⚙️  Configurações: fato_{table}")

print(f"📥 Silver: {schema_silver}.tb_{table}")
print(f"💾 Gold: {schema_gold}.fato_{table}")
print(f"🔗 Dimensões: dim_clientes, dim_data, dim_produtos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📥 Carregar Dados

# CELL ********************

start_time = time.time()

print("📥 Carregando: Silver/Dimensões...")

# Carregar vendas (Silver)
table_silver = f"{schema_silver}.tb_{table}"
df_vendas = read_data(table_silver, "table")

# Carregar dimensões (Gold)
df_dim_clientes = read_data(f"{schema_gold}.dim_clientes" , "table")
df_dim_produtos = read_data(f"{schema_gold}.dim_produtos" , "table")
df_dim_data = read_data(f"{schema_gold}.dim_data" , "table")

print(f"📊 Vendas (Silver): {df_vendas.count():,} registros")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔗 JOINs com Dimensões
# 
# ### Estratégia:
# 
# 1. **JOIN com dim_clientes** - Obter cliente_sk
# 2. **JOIN com dim_produtos** - Obter produto_sk
# 3. **JOIN com dim_data** - Obter data_sk
# 
# ### Tipo de JOIN:
# 
# - **INNER JOIN**: Garante que todas as FKs existem
# - Se algum ID não existir na dimensão, venda é descartada (integridade referencial)


# CELL ********************

print("🔗 Executando JOINs...")

# Converter data_venda para date (para JOIN com dim_data)
df_vendas = df_vendas.withColumn("data_venda_date", to_date(col("data_venda")))

# JOIN 1: dim_clientes
df_fato = df_vendas.join(
    df_dim_clientes.select("cliente_sk","cliente_id"), 
    "cliente_id",
    "inner")

print(f"✅ JOIN dim_clientes: {df_fato.count():,} registros")

# JOIN 2: dim_produtos
df_fato = df_fato.join(
    df_dim_produtos.select("produto_sk", "produto_id", col("preco").alias("preco_produto")),
    "produto_id",
    "inner")

print(f"✅ JOIN dim_produtos: {df_fato.count():,} registros")

# JOIN 3: dim_data
df_dim_data_temp = df_dim_data.select("data_sk", col("data_completa").alias("data_join"))

df_fato = df_fato.join(
    df_dim_data_temp,
    df_fato["data_venda_date"] == df_dim_data_temp["data_join"],
    "inner"
)

print(f"✅ JOIN dim_data: {df_fato.count():,} registros")

print(f"🔗 JOINs concluídos, df_fato: {df_fato.count():,} registros")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄 Transformações e Métricas

# CELL ********************

print("🔄 Aplicando transformações...")

# Selecionar e renomear colunas
df_fato = df_fato.withColumnRenamed("preco_produto", "valor_unitario")

# .select(
#     col("venda_id"),
#     col("data_sk"),
#     col("cliente_sk"),
#     col("produto_sk"),
#     col("quantidade"),
#     col("preco_produto").alias("valor_unitario"),
#     col("valor_total")
# )

param_options = {
    # Adicionar métrica 'receita' (igual a valor_total)
    "receita": col("valor_total"),
    # Adicionar auditoria
    "ingestion_timestamp": current_timestamp(),
    "camada": lit("gold")
}

df_fato = df_fato.withColumns(param_options)

# Reordenar colunas
df_fato = df_fato.select(
    "venda_id", "data_sk", "cliente_sk", "produto_sk",
    "quantidade", "valor_unitario", "valor_total", "receita",
    "ingestion_timestamp", "camada"
)

count_records = df_fato.count()

print(f"📊 Total de vendas processadas: {count_records:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 💾 Salvar Fato

# CELL ********************

table_gold = f"{schema_gold}.fato_{table}"

write_data(df_fato, "delta", "overwrite", table_gold)

print(f"✅ Fato salvo: {table_gold}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📋 Relatório Final e Métricas de Negócio Consolidado

# CELL ********************

total_duration = time.time() - start_time

metrics = df_fato.agg(
    count("venda_id").alias("total_vendas"),
    _sum("quantidade").alias("total_itens"),
    _sum("receita").alias("receita_total"),
    avg("valor_total").alias("ticket_medio")
).first()

print("\n" + "="*80)
print("📊 RELATÓRIO - FATO_VENDAS")
print("="*80)
print(f"\n📈 MÉTRICAS:")
print("-" * 80)
print(f"   Total de vendas: {metrics['total_vendas']:,}")
print(f"   Total de itens: {metrics['total_itens']:,}")
print(f"   Receita total: R$ {metrics['receita_total']:,.2f}")
print(f"   Ticket médio: R$ {metrics['ticket_medio']:,.2f}")

print(f"\n⏱️  Duração: {total_duration:.2f}s")
print(f"💾 Tabela: {table_gold}")
print("\n🔗 Relacionamentos:")
print("-" * 80)
print(f"   fato_vendas.data_sk → dim_data.data_sk")
print(f"   fato_vendas.cliente_sk → dim_clientes.cliente_sk")
print(f"   fato_vendas.produto_sk → dim_produtos.produto_sk")
print("="*80)

mssparkutils.notebook.exit({
    "success": True,
    "records": count_records,
    "receita_total": float(metrics['receita_total']),
    "duration": total_duration
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
