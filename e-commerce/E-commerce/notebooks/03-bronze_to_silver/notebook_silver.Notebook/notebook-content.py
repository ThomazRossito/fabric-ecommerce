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

# # 🥈 Pipeline Silver: Transformação Bronze → Silver

# MARKDOWN ********************

# ## Descrição
# 
# Transformação e limpeza de dados do Bronze para Silver com MERGE e deduplicação.
# 
# ### Funcionalidades:
# 
# 1. **🔄 Deduplicação** - Remove duplicatas usando `ROW_NUMBER()` OVER PARTITION
# 2. **🎯 Registro Mais Recente** - Mantém apenas a versão mais atual
# 3. **🏷️ Auditoria** - Adiciona metadados de rastreabilidade
# 4. **💾 MERGE** - Upsert em Delta Lake
# 
# ### Lógica de Deduplicação:
# 
# ```sql
# ROW_NUMBER() OVER (
#     PARTITION BY <chave_primaria>
#     ORDER BY ingestion_timestamp DESC
# ) = 1
# ```
# 
# **Resultado**: Apenas o registro mais recente de cada ID.
# 
# ### Campos de Auditoria:
# 
# - `file_name`: Arquivo de origem (herdado)
# - `ingestion_timestamp_bronze`: Timestamp do Bronze
# - `ingestion_timestamp_silver`: Timestamp do Silver
# - `camada`: "silver"
# 
# ### Arquitetura:
# 
# ```
# Bronze/tb_clientes → Silver/tb_clientes_clean (Delta)
# Bronze/tb_produtos → Silver/tb_produtos_clean (Delta)
# Bronze/tb_vendas   → Silver/tb_vendas_clean (Delta)
# ```
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

from pyspark.sql.functions import current_timestamp, lit, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import time

print("="*55)
print("🥈 PIPELINE SILVER - TRANSFORMAÇÃO BRONZE → SILVER 🥈")
print("="*55)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⚙️ Configurações

# CELL ********************

# Schemas
schema_bronze = "bronze"
schema_silver = "silver"

# Criar schema se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_silver}")

# Definir chaves primárias
primary_keys = {
    "tb_clientes": "cliente_id",
    "tb_produtos": "produto_id",
    # "tb_vendas": "venda_id"
    "vendas": "venda_id"
}

print(f"⚙️ Configurações: {schema_bronze} → {schema_silver}")

print(f"📥 Bronze: {schema_bronze}")
print(f"💾 Silver: {schema_silver}")
print(f"🔑 Chaves: {primary_keys}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔍 Descoberta de Tabelas
# 
# Lista todas as tabelas do Bronze.

# CELL ********************

print("🔍 Descobrindo tabelas Bronze...")

# Listar tabelas do Bronze
show_table = spark.sql(f"SHOW TABLES IN {schema_bronze}").cache()
show_table_count = show_table.count()
tables = show_table.take(show_table_count)
table_names = [row.tableName for row in tables if row.tableName.startswith('tb_')]

if not table_names:
    raise ValueError(f"Nenhuma tabela encontrada em {schema_bronze}")

print(f"✅ {len(table_names)} tabela(s)")
print(f"📋 Tabelas: {', '.join(table_names)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄 Processamento e Transformação
# 
# Para cada tabela:
# 1. Lê do Bronze
# 2. Remove duplicatas (ROW_NUMBER)
# 3. Adiciona auditoria
# 4. Faz MERGE no Silver

# CELL ********************

start_time = time.time()
results = []

print("\n🔄 Processando tabelas...\n")

for table_name in table_names:
    try:
        print(f"📥 Processando: {table_name}")
        
        # Ler do Bronze
        table_bronze = f"{schema_bronze}.{table_name}"
        df_bronze = read_data(table_bronze, "table")
        
        # Renomear ingestion_timestamp para ingestion_timestamp_bronze
        df_bronze = df_bronze.withColumnRenamed("ingestion_timestamp", "ingestion_timestamp_bronze")
        
        # Obter chave primária
        pk = primary_keys.get(table_name)
        if not pk:
            raise ValueError(f"Chave primária não definida para {table_name}")
        
        # Window para deduplicação
        window_spec = (Window.partitionBy(pk)
                             .orderBy(col("ingestion_timestamp_bronze").desc()))
        
        # Adicionar row_number e filtrar apenas row_num = 1 (mais recente)
        df_dedup = (df_bronze.withColumn("row_num", row_number().over(window_spec))
                             .filter(col("row_num") == 1)
                             .drop("row_num"))
        
        count_before = df_bronze.count()
        count_after = df_dedup.count()
        
        print(f"🔄 Deduplicação: {count_before:,} → {count_after:,} registros")
        
        # Adicionar campos de auditoria Silver
        df_silver = (df_dedup.withColumn("ingestion_timestamp_silver", current_timestamp())
                             .withColumn("camada", lit("silver")))
        
        # Nome da tabela Silver
        table_silver = f"{schema_silver}.{table_name}"
        
        # Verificar se tabela existe
        table_exists = spark.catalog.tableExists(table_silver)
        
        if table_exists:
            # MERGE (UPSERT)
            delta_table = DeltaTable.forName(spark, table_silver)
            
            (
                delta_table.alias("target")
                           .merge(df_silver.alias("source"),
                                f"target.{pk} = source.{pk}")
                           .whenMatchedUpdateAll()
                           .whenNotMatchedInsertAll()
                           .execute()
            )
            
            print(f"✅ MERGE executado: {table_silver}")
        else:
            # print("✅ Criar tabela - (primeira execução)")
            write_data(df_silver, "delta", "overwrite", table_silver)
            
            print(f"✅ Tabela criada: {table_silver}")
        
        # Validar
        count_final = read_data(table_silver, "table").count()
        
        results.append({
            "table": table_name,
            "records_before": count_before,
            "records_after": count_after,
            "records_final": count_final,
            "duplicates_removed": count_before - count_after,
            "success": True
        })
        
        print(f"   ✅ {table_name}: {count_before:,} → {count_after:,} (removidos: {count_before - count_after:,})")
        print(f"✅ {table_name}: {count_final:,} registros")
        
    except Exception as e:
        results.append({
            "table": table_name,
            "records_before": 0,
            "records_after": 0,
            "records_final": 0,
            "duplicates_removed": 0,
            "success": False,
            "error": str(e)
        })
        
        print(f"   ❌ {table_name}: {str(e)}")
        print(f"❌ {table_name}: {str(e)}")

total_duration = time.time() - start_time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📋 Relatório Final Consolidado

# CELL ********************

successes = [r for r in results if r['success']]
failures = [r for r in results if not r['success']]
total_records = sum(r['records_final'] for r in successes)
total_duplicates = sum(r['duplicates_removed'] for r in successes)

print("\n" + "="*80)
print("📊 RELATÓRIO FINAL - SILVER")
print("="*80)

print("\n📈 ESTATÍSTICAS:")
print("-" * 80)
print(f"   Total de tabelas: {len(results)}")
print(f"   ✅ Sucessos: {len(successes)}")
print(f"   ❌ Falhas: {len(failures)}")
print(f"   📊 Total de registros (Silver): {total_records:,}")
print(f"   🗑️  Duplicatas removidas: {total_duplicates:,}")
print(f"   ⏱️  Duração: {total_duration:.2f}s")

if successes:
    print("\n✅ TABELAS PROCESSADAS:")
    print("-" * 80)
    for r in successes:
        print(f"   {schema_silver}.{r['table']:<30} {r['records_final']:>10,} registros (removidos: {r['duplicates_removed']:,})")

if failures:
    print("\n❌ FALHAS:")
    print("-" * 80)
    for r in failures:
        print(f"   {r['table']}: {r.get('error', 'Unknown')}")

print("\n" + "="*80)
if len(failures) == 0:
    print("🎉 SILVER CONCLUÍDO COM SUCESSO!")
    print(f"🎉 Silver: {total_records:,} registros, {total_duplicates:,} duplicatas removidas")
    
    mssparkutils.notebook.exit({
        "success": True,
        "records": total_records,
        "duplicates_removed": total_duplicates,
        "duration": total_duration,
        "tables": len(successes)
    })
else:
    print("⚠️  SILVER COM FALHAS")
    print(f"❌ {len(failures)} tabelas falharam")
    
    mssparkutils.notebook.exit({
        "success": False,
        "error": f"{len(failures)} tabelas falharam",
        "duration": total_duration
    })

print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
