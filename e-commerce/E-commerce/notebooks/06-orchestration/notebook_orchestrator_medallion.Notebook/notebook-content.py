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

# # 🏗️ Orchestrator DAG: Pipeline Medallion Architecture

# MARKDOWN ********************

# ## Descrição
# 
# Pipeline de orquestração completo seguindo **Arquitetura Medallion** com DAG estruturada, dependências.
# 
# ### Funcionalidades Principais:
# 
# 1. **🔗 Dependências Gerenciadas** - Execução ordenada respeitando dependências
# 2. **⚡ Paralelismo Inteligente** - Dimensões processam em paralelo
# 3. **📊 Rastreabilidade** - Logs e métricas de cada etapa
# 4. **🎯 Pipeline Completo** - Transient → Bronze → Silver → Gold
# 
# ### Arquitetura do Pipeline:
# 
# ```
# TRANSIENT (Dados Brutos)
#     │
#     ├─> [BRONZE] notebook_bronze
#     │       │
#     │       ├─> [SILVER] notebook_silver
#     │       │       │
#     │       │       ├─> [GOLD - Dimensões] (PARALELO)
#     │       │       │       ├─> dim_clientes
#     │       │       │       ├─> dim_data
#     │       │       │       └─> dim_produtos
#     │       │       │               │
#     │       │       │               └─> [GOLD - Fato]
#     │       │       │                       └─> fato_vendas
#     │       │       │
#     │       │       └─> Camada Silver (Dados Limpos)
#     │       │
#     │       └─> Camada Bronze (Dados Brutos + Auditoria)
#     │
#     └─> Camada Transient (Landing Zone)
# ```
# 
# ### Estrutura de Dependências:
# 
# | Activity | Depende De | Paralelismo |
# |----------|-----------|-------------|
# | Bronze | (nenhum) | Início |
# | Silver | Bronze | Sequencial |
# | dim_clientes | Silver | Paralelo |
# | dim_data | Silver | Paralelo |
# | dim_produtos | Silver | Paralelo |
# | fato_vendas | dim_clientes, dim_data, dim_produtos | Após dimensões |
# 
# ### Características Técnicas:
# 
# - ✅ **Timeout por Notebook**: 300s (5 minutos)
# - ✅ **Isolamento**: Cada notebook em processo separado
# - ✅ **Resiliência**: Falha em dimensão não para outras
# - ✅ **Visualização DAG**: Grafo visual das dependências
# 
# ### Performance Esperada:
# 
# | Cenário | Tempo Sequencial | Tempo DAG | Ganho |
# |---------|-----------------|-----------|-------|
# | Sem paralelismo | ~30 min | ~30 min | 1x |
# | Com paralelismo (dim) | ~30 min | ~20 min | 1.5x |
# 
# **Ganho**: Dimensões processam simultaneamente.
# 
# ## Dependências:
# 
# - **PySpark**: DataFrame API
# - **mssparkutils**: APIs nativas do Fabric
# - **Logger customizado**: tarn_notebook_logger
# 
# ## Notebooks Requeridos:
# 
# 1. `notebook_bronze` - Ingestão Transient → Bronze
# 2. `notebook_silver` - Transformação Bronze → Silver
# 3. `notebook_gold_dim_clientes` - Dimensão Clientes
# 4. `notebook_gold_dim_data` - Dimensão Data
# 5. `notebook_gold_dim_produtos` - Dimensão Produtos
# 6. `notebook_gold_fato_vendas` - Fato Vendas
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

# ## 📦 Importação de Bibliotecas
# 
# Importa todas as dependências necessárias para orquestração da DAG Medallion.
# 
# ### Bibliotecas Utilizadas:
# 
# - **json**: Parsing de resultados dos notebooks
# - **time**: Medição de performance do pipeline
# - **datetime.timedelta**: Formatação de durações
# - **mssparkutils**: APIs nativas do Fabric


# CELL ********************

import json
import time
from datetime import timedelta
from notebookutils import mssparkutils

print("="*80)
print("🏗️ ORCHESTRATOR DAG - PIPELINE MEDALLION COMPLETO 🏗️")
print("="*80)
print("📊 Arquitetura: Transient → Bronze → Silver → Gold")
print("🔗 Dependências: Gerenciadas automaticamente")
print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## ⚙️ Configuração do Pipeline
# 
# ### Parâmetros Configuráveis:
# 
# #### TIMEOUT
# Timeout máximo por célula de cada notebook (segundos).
# - **Padrão**: 300s (5 minutos)
# - **Bronze**: Pode precisar de mais (muitos arquivos)
# - **Silver**: Transformações podem demorar
# - **Gold**: Geralmente rápido (agregações)
# 
# #### DISPLAY_DAG
# Habilita visualização gráfica da DAG.
# - **True**: Mostra grafo com dependências
# - **False**: Sem visualização (mais rápido)
# 
# ### Notebooks do Pipeline:
# 
# **IMPORTANTE**: Todos os notebooks listados DEVEM existir no workspace.
# 
# ---

# CELL ********************

# =============================================================================
# CONFIGURAÇÕES GLOBAIS
# =============================================================================

TIMEOUT = 300           # 5 minutos por célula
DISPLAY_DAG = True      # Visualizar grafo

# =============================================================================
# NOTEBOOKS DO PIPELINE
# =============================================================================

# Camada Bronze
NOTEBOOK_BRONZE = "notebook_bronze"

# Camada Silver
NOTEBOOK_SILVER = "notebook_silver"

# Camada Gold - Dimensões
NOTEBOOK_DIM_CLIENTES = "notebook_gold_dim_clientes"
NOTEBOOK_DIM_DATA = "notebook_gold_dim_data"
NOTEBOOK_DIM_PRODUTOS = "notebook_gold_dim_produtos"

# Camada Gold - Fato
NOTEBOOK_FATO_VENDAS = "notebook_gold_fato_vendas"

# =============================================================================
# LOG DE CONFIGURAÇÕES
# =============================================================================

print(f"⚙️ Configurações: ⚙️")

print("\n📋 Configurações do Pipeline:")
print(f"   Timeout: {TIMEOUT}s ({TIMEOUT/60:.1f} min)")
print(f"   Visualização DAG: {'✅ Habilitada' if DISPLAY_DAG else '❌ Desabilitada'}")
print(f"\n📓 Notebooks:")
print(f"   Bronze: {NOTEBOOK_BRONZE}")
print(f"   Silver: {NOTEBOOK_SILVER}")
print(f"   Dimensões: {NOTEBOOK_DIM_CLIENTES}, {NOTEBOOK_DIM_DATA}, {NOTEBOOK_DIM_PRODUTOS}")
print(f"   Fato: {NOTEBOOK_FATO_VENDAS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 🏗️ Construção da DAG com Dependências
# 
# ### Objetivo:
# 
# Construir estrutura DAG completa com todas as activities, dependências.
# 
# ### Estrutura de uma Activity:
# 
# ```python
# {
#     "name": "bronze",                    # Identificador único
#     "path": "notebook_bronze",           # Nome do notebook
#     "timeoutPerCellInSeconds": 300,      # Timeout
#     "args": {...},                       # Parâmetros (opcional)
#     "dependencies": ["bronze"]           # Lista de dependências (opcional)
# }
# ```
# 
# ### Fluxo de Dependências:
# 
# 1. **Bronze**: Executa primeiro (sem dependências)
# 2. **Silver**: Aguarda Bronze concluir
# 3. **Dimensões**: Aguardam Silver, executam em PARALELO
# 4. **Fato**: Aguarda TODAS as 3 dimensões concluírem
# 
# 
# ### Paralelismo:
# 
# - **Dimensões**: Como todas dependem apenas de Silver, executam simultaneamente
# - **Fato**: Aguarda barreira (todas dimensões concluídas)
# 
# ---


# CELL ********************

print("🏗️ Construindo DAG Medallion... 🏗️")

# =============================================================================
# ESTRUTURA DA DAG
# =============================================================================

dag = {
    "activities": [
        
        # =====================================================================
        # CAMADA BRONZE (Sem dependências - Executa primeiro)
        # =====================================================================
        {
            "name": "dag_bronze",
            "path": NOTEBOOK_BRONZE,
            "timeoutPerCellInSeconds": TIMEOUT,
            "args": {
                "pipe_name": "dag_bronze",
                "note_name": NOTEBOOK_BRONZE,
                "schema_atcual": "bronze"
            },
            # Sem dependencies - executa primeiro
        },
        
        # =====================================================================
        # CAMADA SILVER (Depende de Bronze)
        # =====================================================================
        {
            "name": "dag_silver",
            "path": NOTEBOOK_SILVER,
            "timeoutPerCellInSeconds": TIMEOUT,
            "args": {
                "pipe_name": "dag_silver",
                "note_name": NOTEBOOK_SILVER,
                "schema_atcual": "silver"
            },
            "dependencies": ["dag_bronze"]  # Aguarda Bronze
        },
        
        # =====================================================================
        # CAMADA GOLD - DIMENSÕES (Dependem de Silver, executam em PARALELO)
        # =====================================================================
        
        # Dimensão Clientes
        {
            "name": "dag_gold_dim_clientes",
            "path": NOTEBOOK_DIM_CLIENTES,
            "timeoutPerCellInSeconds": TIMEOUT,
            "args": {
                "pipe_name": "dag_gold_dim_clientes",
                "note_name": NOTEBOOK_DIM_CLIENTES,
                "schema_atcual": "gold"
            },
            "dependencies": ["dag_silver"]  # Aguarda Silver
        },
        
        # Dimensão Data
        {
            "name": "dag_gold_dim_data",
            "path": NOTEBOOK_DIM_DATA,
            "timeoutPerCellInSeconds": TIMEOUT,
            "args": {
                "pipe_name": "dag_gold_dim_data",
                "note_name": NOTEBOOK_DIM_DATA,
                "schema_atcual": "gold"
            },
            "dependencies": ["dag_silver"]  # Aguarda Silver
        },
        
        # Dimensão Produtos
        {
            "name": "dag_gold_dim_produtos",
            "path": NOTEBOOK_DIM_PRODUTOS,
            "timeoutPerCellInSeconds": TIMEOUT,
            "args": {
                "pipe_name": "dag_gold_dim_produtos",
                "note_name": NOTEBOOK_DIM_PRODUTOS,
                "schema_atcual": "gold"
            },
            "dependencies": ["dag_silver"]  # Aguarda Silver
        },
        
        # =====================================================================
        # CAMADA GOLD - FATO (Depende de TODAS as dimensões)
        # =====================================================================
        {
            "name": "dag_gold_fato_vendas",
            "path": NOTEBOOK_FATO_VENDAS,
            "timeoutPerCellInSeconds": TIMEOUT,
            "args": {
                "pipe_name": "dag_gold_fato_vendas",
                "note_name": NOTEBOOK_FATO_VENDAS,
                "schema_atcual": "gold"
            },
            # Aguarda TODAS as 3 dimensões
            "dependencies": [
                "dag_gold_dim_clientes",
                "dag_gold_dim_data",
                "dag_gold_dim_produtos"
            ]
        }
    ]
}

# =============================================================================
# VALIDAÇÃO E LOG
# =============================================================================

num_activities = len(dag['activities'])

print("\n🏗️  DAG Medallion Construída:")
print(f"   ✅ Total de activities: {num_activities}")
print("\n   Estrutura de execução:")
print("   1. Bronze (sequencial: 1)")
print("      ↓")
print("   2. Silver (sequencial: 1)")
print("      ↓")
print("   3. Dimensões (paralelo: 3)")
print("      ├─ dim_clientes")
print("      ├─ dim_data")
print("      └─ dim_produtos")
print("         ↓")
print("   4. Fato (sequencial: 1)")
print("      └─ fato_vendas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 🚀 Execução da DAG Medallion
# 
# ### Objetivo:
# 
# Executar DAG completa respeitando dependências, paralelismo.
# 
# ### Como o Fabric Executa:
# 
# 1. **Analisa dependências**: Constrói grafo de execução
# 2. **Executa Bronze**: Primeiro notebook (sem dependências)
# 3. **Aguarda Bronze**: Silver espera conclusão
# 4. **Executa Silver**: Após Bronze concluir
# 5. **Aguarda Silver**: Dimensões esperam
# 6. **Executa Dimensões em PARALELO**: 3 notebooks simultâneos
# 7. **Aguarda TODAS dimensões**: Barreira para Fato
# 8. **Executa Fato**: Após todas dimensões
# 
# ---

# CELL ********************

print("\n" + "="*80)
print("🚀 INICIANDO PIPELINE MEDALLION 🚀")
print("="*80)
print(f"📊 Activities: {num_activities}")
print(f"🔗 Contém dependências configuradas")
print(f"⏳ Aguarde... Execução em progresso...")
print("="*80 + "\n")

print("🚀 Executando DAG Medallion...")

# =============================================================================
# EXECUÇÃO DA DAG
# =============================================================================

start_time = time.time()

try:
    # Executa DAG com dependências
    dag_result = mssparkutils.notebook.runMultiple(
        dag,
        {"displayDAGViaGraphviz": DISPLAY_DAG}
    )
    
    total_duration = time.time() - start_time
    
    print("\n" + "="*40)
    print("✅ PIPELINE MEDALLION CONCLUÍDO")
    print("="*40)
    print(f"⏱️  Tempo total: {str(timedelta(seconds=int(total_duration)))} ({total_duration:.2f}s)")
    print("="*40 + "\n")
    
except Exception as e:
    fail_duration = time.time() - start_time
    
    print(f"❌ FALHA CRÍTICA: {str(e)}")
    
    print("\n" + "="*80)
    print("❌ ERRO CRÍTICO NO PIPELINE")
    print("="*80)
    print(f"Erro: {str(e)}")
    print(f"Tempo até falha: {fail_duration:.2f}s")
    print("="*80)
    
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 📊 Processamento de Resultados
# 
# ### Objetivo:
# 
# Extrair e consolidar resultados de todas as activities (Bronze, Silver, Dimensões, Fato).
# 
# ### Status Possíveis:
# 
# - **Succeeded**: Activity concluída com sucesso
# 
# ---

# CELL ********************

import json
import ast

print("📊 Monta variáveis para relatório...")

expected_activities = [
    "dag_bronze", "dag_silver", "dag_gold_dim_clientes", 
    "dag_gold_dim_data", "dag_gold_dim_produtos", "dag_gold_fato_vendas"
]

results = {}

for activity_name in expected_activities:
    if activity_name in dag_result:
        val = dag_result[activity_name]
        
        # Extrair a string de saída (pode estar dentro de 'exitVal' ou ser a própria string)
        exit_content = val.get("exitVal", str(val)) if isinstance(val, dict) else str(val)
        
        try:
            # Tenta literal_eval primeiro, pois aceita aspas simples (formato Python)
            # Isso resolve: {'success': True} que o json.loads rejeita
            r = ast.literal_eval(exit_content)
        except:
            try:
                # Fallback para JSON caso o conteúdo venha com aspas duplas
                r = json.loads(exit_content)
            except Exception as e:
                r = {"success": False, "error": f"Erro de leitura: {str(e)}", "status": "Failed"}
        
        results[activity_name] = r
    else:
        results[activity_name] = {"success": False, "error": "Não executado", "status": "Skipped"}

# Reatribuindo para as variáveis do seu relatório
bronze_result = results.get("dag_bronze", {})
silver_result = results.get("dag_silver", {})
dim_clientes_result = results.get("dag_gold_dim_clientes", {})
dim_data_result = results.get("dag_gold_dim_data", {})
dim_produtos_result = results.get("dag_gold_dim_produtos", {})
fato_vendas_result = results.get("dag_gold_fato_vendas", {})

print(f"✅ Variáveis criadas com Sucesso!!!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 📋 Relatório Final Consolidado
# 
# Relatório completo do pipeline Medallion com todas as métricas e status.
# 
# ### Seções:
# 
# 1. **Resumo Executivo**: Totais e taxa de sucesso
# 2. **Performance**: Tempo total e por camada
# 3. **Status por Camada**: Bronze, Silver, Gold (Dim + Fato)
# 4. **Dependências**: Quais aguardaram quais
# 5. **Conclusão**: Status final do pipeline
# 
# ---

# CELL ********************

# =============================================================================
# RELATÓRIO FINAL
# =============================================================================

print("\n" + "="*80)
print("📊 RELATÓRIO FINAL - PIPELINE MEDALLION")
print("="*80)

# =============================================================================
# SEÇÃO 1: RESUMO EXECUTIVO
# =============================================================================

# Recalcular sucessos/falhas baseado no objeto processado
successes = sum(1 for r in results.values() if r.get("success") == True)
failures = len(results) - successes

print("\n📈 RESUMO EXECUTIVO:")
print("-" * 80)
print(f"   Total de activities: {len(results)}")
print(f"   ✅ Sucessos: {successes}")
print(f"   ❌ Falhas: {failures}")
print(f"   Taxa de sucesso: {(successes/len(results)*100):.1f}%")

# =============================================================================
# SEÇÃO 2: PERFORMANCE
# =============================================================================

print("\n⚡ PERFORMANCE:")
print("-" * 80)
print(f"   Tempo total: {str(timedelta(seconds=int(total_duration)))} ({total_duration:.2f}s)")

# Tempo por camada (se disponível)
if bronze_result.get("duration"):
    print(f"   Bronze: {bronze_result['duration']:.2f}s")
if silver_result.get("duration"):
    print(f"   Silver: {silver_result['duration']:.2f}s")

# Tempo das dimensões (paralelo)
dim_durations = []
if dim_clientes_result.get("duration"):
    dim_durations.append(dim_clientes_result['duration'])
if dim_data_result.get("duration"):
    dim_durations.append(dim_data_result['duration'])
if dim_produtos_result.get("duration"):
    dim_durations.append(dim_produtos_result['duration'])

if dim_durations:
    print(f"   Dimensões (paralelo): {max(dim_durations):.2f}s (max de {len(dim_durations)} dimensões)")

if fato_vendas_result.get("duration"):
    print(f"   Fato Vendas: {fato_vendas_result['duration']:.2f}s")

# =============================================================================
# SEÇÃO 3: STATUS POR CAMADA
# =============================================================================

print("\n📊 STATUS POR CAMADA:")
print("-" * 80)

# BRONZE
print("\n   🥉 BRONZE:")
if bronze_result.get("success"):
    print(f"      Status: ✅ Succeeded")
    if bronze_result.get("records"):
        print(f"      Registros: {bronze_result['records']:,}")
else:
    status = bronze_result.get("status", "Failed")
    error = bronze_result.get("error", "Unknown error")
    print(f"      Status: ❌ {status}")
    print(f"      Erro: {error}")

# SILVER
print("\n   🥈 SILVER:")
if silver_result.get("success"):
    print(f"      Status: ✅ Succeeded")
    if silver_result.get("records"):
        print(f"      Registros: {silver_result['records']:,}")
else:
    status = silver_result.get("status", "Failed")
    error = silver_result.get("error", "Unknown error")
    print(f"      Status: ❌ {status}")
    print(f"      Erro: {error}")

# GOLD - DIMENSÕES
print("\n   🥇 GOLD - DIMENSÕES (Processadas em paralelo):")

# Clientes
print("\n      📊 dim_clientes:")
if dim_clientes_result.get("success"):
    print(f"         Status: ✅ Succeeded")
    if dim_clientes_result.get("records"):
        print(f"         Registros: {dim_clientes_result['records']:,}")
else:
    status = dim_clientes_result.get("status", "Failed")
    error = dim_clientes_result.get("error", "Unknown error")
    print(f"         Status: ❌ {status}")
    print(f"         Erro: {error}")

# Data
print("\n      📅 dim_data:")
if dim_data_result.get("success"):
    print(f"         Status: ✅ Succeeded")
    if dim_data_result.get("records"):
        print(f"         Registros: {dim_data_result['records']:,}")
else:
    status = dim_data_result.get("status", "Failed")
    error = dim_data_result.get("error", "Unknown error")
    print(f"         Status: ❌ {status}")
    print(f"         Erro: {error}")

# Produtos
print("\n      📦 dim_produtos:")
if dim_produtos_result.get("success"):
    print(f"         Status: ✅ Succeeded")
    if dim_produtos_result.get("records"):
        print(f"         Registros: {dim_produtos_result['records']:,}")
else:
    status = dim_produtos_result.get("status", "Failed")
    error = dim_produtos_result.get("error", "Unknown error")
    print(f"         Status: ❌ {status}")
    print(f"         Erro: {error}")

# GOLD - FATO
print("\n   🥇 GOLD - FATO:")
print("\n      💰 fato_vendas:")
if fato_vendas_result.get("success"):
    print(f"         Status: ✅ Succeeded")
    if fato_vendas_result.get("records"):
        print(f"         Registros: {fato_vendas_result['records']:,}")
else:
    status = fato_vendas_result.get("status", "Failed")
    error = fato_vendas_result.get("error", "Unknown error")
    print(f"         Status: ❌ {status}")
    print(f"         Erro: {error}")

# =============================================================================
# SEÇÃO 4: ESTRUTURA DE DEPENDÊNCIAS
# =============================================================================

print("\n🔗 ESTRUTURA DE DEPENDÊNCIAS:")
print("-" * 80)
print("   bronze → (sem dependências)")
print("   silver → aguardou: bronze")
print("   gold_dim_clientes → aguardou: silver")
print("   gold_dim_data → aguardou: silver")
print("   gold_dim_produtos → aguardou: silver")
print("   gold_fato_vendas → aguardou: gold_dim_clientes, gold_dim_data, gold_dim_produtos")

# =============================================================================
# SEÇÃO 5: STATUS FINAL
# =============================================================================

print("\n" + "="*80)

if failures == 0:
    print("🎉 PIPELINE MEDALLION CONCLUÍDO COM SUCESSO TOTAL!")
    print("="*80)
    print(f"✅ Todas as {len(results)} activities executadas com sucesso")
    print(f"⏱️  Tempo total: {str(timedelta(seconds=int(total_duration)))}")
    print("\n📊 Camadas processadas:")
    print("   ✅ Bronze (Transient → Bronze)")
    print("   ✅ Silver (Bronze → Silver)")
    print("   ✅ Gold - Dimensões (Silver → Dimensões)")
    print("   ✅ Gold - Fato (Dimensões → Fato)")
    
    print(f"🎉 Pipeline Medallion: Sucesso total em {total_duration:.2f}s")
    
else:
    print("⚠️  PIPELINE MEDALLION CONCLUÍDO COM FALHAS")
    print("="*80)
    print(f"✅ Sucessos: {successes}/{len(results)}")
    print(f"❌ Falhas: {failures}/{len(results)}")
    print(f"\n🔍 RECOMENDAÇÕES:")
    print("   1. Revisar erros nas activities que falharam")
    print("   2. Verificar logs individuais de cada notebook")
    print("   3. Corrigir problemas identificados")
    print("   4. Re-executar pipeline completo")
    
    print(f"❌ Pipeline com {failures} falhas")

print("="*80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📊 Monitoramento: Log de Execução do Pipeline Medallion


# CELL ********************

import json
import time

# 1. Empacota o dicionário local em uma string JSON
results_json = json.dumps(results)

# Configurações do Retry
max_tentativas = 1
tempo_espera_segundos = 15

print("🚀 Disparando logs para o notebook de monitoramento...")

for tentativa in range(1, max_tentativas + 1):
    try:
        print(f"▶️ Executando notebook de log (Tentativa {tentativa}/{max_tentativas})...")
        
        # Chama o outro notebook
        mssparkutils.notebook.run("notebook_monitoramento_log", 60, {"dag_results_raw": results_json})
        
        # Se chegou nesta linha sem dar erro, o run() funcionou. 
        print("✅ Envio de logs finalizado com sucesso!!!")
        
        # O comando 'break' interrompe o loop for imediatamente, evitando as próximas tentativas.
        break 

    except Exception as e:
        print(f"⚠️ Falha na tentativa {tentativa}: {e}")
        
        # Se ainda não for a última tentativa, aguarda antes de tentar de novo
        if tentativa < max_tentativas:
            print(f"⏳ Aguardando {tempo_espera_segundos} segundos antes da próxima tentativa...")
            time.sleep(tempo_espera_segundos)
        else:
            print("❌ Todas as tentativas falharam. O log não foi gravado na tabela.")
            # Descomente a linha abaixo se você quiser que o Notebook Principal TAMBÉM 
            # dê erro/falhe caso o envio de log não funcione de jeito nenhum.
            # raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM TARN_LH_DEV.monitoramento.tb_monitoramento_log LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# try:
#     print("🚀 Enviando logs para monitoramento.tb_monitoramento_log...")

#     # Transformamos o dicionário em uma String JSON
#     results_json = json.dumps(results)
#     mssparkutils.notebook.run("notebook_monitoramento_log", 60, {"dag_results_raw": results_json})

#     print("✅ Logs para monitoramento.tb_monitoramento_log sucesso!!! ✅")

# except Exception as e:
#     print(f"⚠️ Aviso: Falha ao gravar log de monitoramento: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## 📖 Documentação de Uso
# 
# ### Pré-requisitos:
# 
# **Notebooks necessários no workspace**:
# 1. `notebook_bronze` - Ingestão Transient → Bronze
# 2. `notebook_silver` - Transformação Bronze → Silver
# 3. `notebook_gold_dim_clientes` - Dimensão Clientes
# 4. `notebook_gold_dim_data` - Dimensão Data
# 5. `notebook_gold_dim_produtos` - Dimensão Produtos
# 6. `notebook_gold_fato_vendas` - Fato Vendas
# 
# **Schemas necessários**:
# ```sql
# CREATE SCHEMA IF NOT EXISTS bronze;
# CREATE SCHEMA IF NOT EXISTS silver;
# CREATE SCHEMA IF NOT EXISTS gold;
# CREATE SCHEMA IF NOT EXISTS monitoramento;
# ```
# 
# ### Workflow de Execução:
# 
# 1. **Configurar** 
# 2. **Executar** Run All
# 3. **Acompanhar** via visualização DAG
# 4. **Validar** relatório final
