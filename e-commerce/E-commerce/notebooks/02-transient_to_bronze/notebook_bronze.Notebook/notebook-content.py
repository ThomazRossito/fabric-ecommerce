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

# # 🥉 Pipeline Bronze: Ingestão Transient → Bronze

# MARKDOWN ********************

# ## Descrição
# 
# Ingestão de dados brutos da camada Transient para Bronze com campos de auditoria.
# 
# ### Funcionalidades:
# 
# 1. **📥 Leitura de CSVs** - Descobre e processa todos os CSVs da Transient
# 2. **🏷️ Campos de Auditoria** - Adiciona metadados de rastreabilidade
# 3. **💾 Formato Delta** - Salva em Delta Lake (ACID)
# 4. **📊 Relatório** - Métricas de ingestão
# 
# ### Campos de Auditoria Adicionados:
# 
# - `file_name`: Nome do arquivo de origem
# - `ingestion_timestamp`: Data/hora da ingestão
# - `camada`: "bronze"
# 
# ### Arquitetura:
# 
# ```
# Transient/clientes.csv → Bronze/tb_clientes (Delta)
# Transient/produtos.csv → Bronze/tb_produtos (Delta)
# Transient/vendas.csv   → Bronze/tb_vendas (Delta)
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

from pyspark.sql.functions import current_timestamp, lit, input_file_name
from notebookutils import mssparkutils
import time

print("="*51)
print("🥉 PIPELINE BRONZE - INGESTÃO TRANSIENT → BRONZE 🥉")
print("="*51)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⚙️ Configurações

# CELL ********************

# Paths
path_transient = "Files/data/transient/ecommerce"
schema_bronze = "bronze"

# Criar schema se não existir
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_bronze}")

print(f"⚙️ Configurações ⚙️")
print(f"📁 Transient: {path_transient}")
print(f"💾 Bronze: {schema_bronze}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔍 Descoberta de Arquivos
# 
# Descobre todos os diretórios CSV na Transient.

# CELL ********************

print("🔍 Descobrindo arquivos...")

# Listar diretórios (cada diretório contém um CSV)
try:
    all_items = mssparkutils.fs.ls(path_transient)

    # Pegar apenas diretórios (cada um contém CSVs do coalesce)
    directories = [item.name.rstrip('/') for item in all_items if item.isDir]
    
    # Filtrar apenas os que têm nomes válidos (clientes, produtos, vendas)
    valid_tables = [d for d in directories if d in ['clientes', 'produtos', 'vendas']]
    
except Exception as e:
    print(f"❌ Erro ao acessar {path_transient}: {str(e)}")
    raise

if not valid_tables:
    raise ValueError(f"Nenhuma tabela válida em {path_transient}")

print(f"✅ {len(valid_tables)} tabela(s)")
print(f"📋 Tabelas encontradas: {', '.join(valid_tables)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔄 Processamento e Ingestão
# 
# Para cada arquivo:
# 1. Lê CSV da Transient
# 2. Adiciona campos de auditoria
# 3. Salva em Delta no Bronze

# CELL ********************

start_time = time.time()
results = []

print("\n🔄 Processando tabelas...\n")

for table_name in valid_tables:
    try:
        print(f"📥 Processando: {table_name}")
        
        # Ler CSV
        df = read_data(f"{path_transient}/{table_name}/", "csv")
        
        count_origem = df.count()
        
        # Adicionar campos de auditoria
        df_bronze = (df.withColumn("file_name", input_file_name())
                       .withColumn("ingestion_timestamp", current_timestamp())
                       .withColumn("camada", lit("bronze")))
        
        # Nome da tabela Bronze
        table_bronze = f"{schema_bronze}.tb_{table_name}"
        
        # Salvar em Delta
        write_data(df_bronze, "delta", "overwrite", table_bronze)
        
        # Validar
        count_destino = read_data(table_bronze, "table").count()
        
        results.append({
            "table": table_name,
            "records": count_destino,
            "success": True
        })
        
        print(f"   ✅ {table_name}: {count_destino:,} registros")
        
    except Exception as e:
        results.append({
            "table": table_name,
            "records": 0,
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
total_records = sum(r['records'] for r in successes)

print("\n" + "="*80)
print("📊 RELATÓRIO FINAL - BRONZE 📊")
print("="*80)

print("\n📈 ESTATÍSTICAS:")
print("-" * 80)
print(f"   Total de tabelas: {len(results)}")
print(f"   ✅ Sucessos: {len(successes)}")
print(f"   ❌ Falhas: {len(failures)}")
print(f"   📊 Total de registros: {total_records:,}")
print(f"   ⏱️  Duração: {total_duration:.2f}s")

if successes:
    print("\n✅ TABELAS PROCESSADAS:")
    print("-" * 80)
    for r in successes:
        print(f"   {schema_bronze}.tb_{r['table']:<20} {r['records']:>10,} registros")

if failures:
    print("\n❌ FALHAS:")
    print("-" * 80)
    for r in failures:
        print(f"   {r['table']}: {r.get('error', 'Unknown')}")

print("\n" + "="*80)
if len(failures) == 0:
    print("🎉 BRONZE CONCLUÍDO COM SUCESSO!")
    print(f"🎉 Bronze: {total_records:,} registros")
    
    # Retornar para DAG
    mssparkutils.notebook.exit({
        "success": True,
        "records": total_records,
        "duration": total_duration,
        "tables": len(successes)
    })
else:
    print("⚠️  BRONZE COM FALHAS")
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
