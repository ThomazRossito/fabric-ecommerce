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

# # 📊 Gerador de Dados Mock - Pipeline Medallion

# MARKDOWN ********************

# ## Descrição
# 
# Gerador de dados mock (CSV) para simular pipeline completo Medallion.
# 
# ### Dados Gerados:
# 
# 1. **clientes.csv** - 1000 clientes fictícios
# 2. **produtos.csv** - 100 produtos com categorias
# 3. **vendas.csv** - 5000 vendas com relacionamentos
# 
# ### Características:
# 
# - ✅ **Relacionamentos válidos**: FKs existem nas tabelas pai
# - ✅ **Dados realistas**: Nomes, categorias, preços plausíveis
# - ✅ **Duplicatas intencionais**: Para testar MERGE no Silver
# - ✅ **Datas variadas**: 2023-2025 para dimensão temporal
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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, when, expr
from datetime import datetime, timedelta
import random

print("="*27)
print("📊 GERADOR DE DADOS MOCK 📊")
print("="*27)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⚙️ Configurações

# CELL ********************

# Diretório de saída (Transient)
output_path = "Files/data/transient/ecommerce"

# Quantidades
NUM_CLIENTES = 1000
NUM_PRODUTOS = 100
NUM_VENDAS = 5000

print(f"📁 Output: {output_path}")
print(f"👥 Clientes: {NUM_CLIENTES}")
print(f"📦 Produtos: {NUM_PRODUTOS}")
print(f"💰 Vendas: {NUM_VENDAS}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 👥 Gerar Clientes
# 
# Cria arquivo `clientes.csv` com 1000 registros.
# 
# ### Colunas:
# - `cliente_id`: ID único (C0001 - C1000)
# - `nome`: Nome fictício
# - `email`: Email baseado no nome
# - `cidade`: Cidade brasileira
# - `estado`: UF
# - `data_cadastro`: 2023-2025


# CELL ********************

# Listas de nomes e cidades
nomes = ["Ana Silva", "João Santos", "Maria Oliveira", "Pedro Costa", "Carla Souza",
         "Lucas Ferreira", "Juliana Lima", "Ricardo Alves", "Fernanda Rocha", "Bruno Martins"]

cidades = [
    ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Brasília", "DF"),
    ("Salvador", "BA"), ("Fortaleza", "CE"), ("Recife", "PE"), ("Manaus", "AM")
]

# Criar DataFrame
clientes_data = []
for i in range(1, NUM_CLIENTES + 1):
    nome = random.choice(nomes)
    cidade, estado = random.choice(cidades)
    
    clientes_data.append((
        f"C{i:04d}",  # C0001, C0002, ...
        nome,
        f"{nome.lower().replace(' ', '.')}@email.com",
        cidade,
        estado,
        (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
    ))

df_clientes = spark.createDataFrame(
    clientes_data,
    ["cliente_id", "nome", "email", "cidade", "estado", "data_cadastro"]
)

# Adicionar duplicatas intencionais (para testar MERGE)
duplicatas = df_clientes.limit(50)  # 50 clientes duplicados
df_clientes_com_dup = df_clientes.union(duplicatas)

# Salvar
write_data(df_clientes_com_dup, "csv", "overwrite", f"{output_path}/clientes")

print(f"✅ Clientes: {df_clientes_com_dup.count()} registros (com 50 duplicatas)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Gerar Produtos
# 
# Cria arquivo `produtos.csv` com 100 produtos.
# 
# ### Colunas:
# - `produto_id`: ID único (P001 - P100)
# - `nome`: Nome do produto
# - `categoria`: Eletrônicos, Livros, Casa, Moda, Esportes
# - `preco`: R$ 10.00 - R$ 5000.00


# CELL ********************

# Produtos por categoria
produtos_por_categoria = {
    "Eletrônicos": ["Smartphone", "Notebook", "Tablet", "Smartwatch", "Fone Bluetooth"],
    "Livros": ["Romance", "Ficção Científica", "Biografia", "Técnico", "Infantil"],
    "Casa": ["Panela", "Jogo de Cama", "Aspirador", "Luminária", "Quadro Decorativo"],
    "Moda": ["Camiseta", "Calça Jeans", "Tênis", "Bolsa", "Relógio"],
    "Esportes": ["Bola Futebol", "Raquete Tênis", "Bicicleta", "Esteira", "Halteres"]
}

# Criar DataFrame
produtos_data = []
produto_id = 1

for categoria, items in produtos_por_categoria.items():
    for _ in range(20):  # 20 produtos por categoria = 100 total
        nome = random.choice(items)
        preco = round(random.uniform(10.0, 5000.0), 2)
        
        produtos_data.append((
            f"P{produto_id:03d}",
            f"{nome} {random.choice(['Premium', 'Pro', 'Basic', 'Plus', 'Deluxe'])}",
            categoria,
            preco
        ))
        produto_id += 1

df_produtos = spark.createDataFrame(
    produtos_data,
    ["produto_id", "nome", "categoria", "preco"]
)

# Adicionar duplicatas
duplicatas_prod = df_produtos.limit(20)
df_produtos_com_dup = df_produtos.union(duplicatas_prod)

# Salvar
write_data(df_produtos_com_dup, "csv", "overwrite", f"{output_path}/produtos")

print(f"✅ Produtos: {df_produtos_com_dup.count()} registros (com 20 duplicatas)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 💰 Gerar Vendas
# 
# Cria arquivo `vendas.csv` com 5000 vendas.
# 
# ### Colunas:
# - `venda_id`: ID único (V00001 - V05000)
# - `data_venda`: 2023-2025
# - `cliente_id`: FK para clientes
# - `produto_id`: FK para produtos
# - `quantidade`: 1-10
# - `valor_total`: quantidade * preco


# CELL ********************

# Coletar IDs válidos
clientes_ids = [row.cliente_id for row in df_clientes.select("cliente_id").collect()]
produtos_ids = [row.produto_id for row in df_produtos.select("produto_id").collect()]
produtos_precos = {row.produto_id: row.preco for row in df_produtos.select("produto_id", "preco").collect()}

# Criar vendas
vendas_data = []
for i in range(1, NUM_VENDAS + 1):
    cliente_id = random.choice(clientes_ids)
    produto_id = random.choice(produtos_ids)
    quantidade = random.randint(1, 10)
    preco_unitario = produtos_precos[produto_id]
    valor_total = round(quantidade * preco_unitario, 2)
    data_venda = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
    
    vendas_data.append((
        f"V{i:05d}",
        data_venda,
        cliente_id,
        produto_id,
        quantidade,
        valor_total
    ))

df_vendas = spark.createDataFrame(
    vendas_data,
    ["venda_id", "data_venda", "cliente_id", "produto_id", "quantidade", "valor_total"]
)

# Adicionar duplicatas
duplicatas_vendas = df_vendas.limit(100)
df_vendas_com_dup = df_vendas.union(duplicatas_vendas)

# Salvar
write_data(df_vendas_com_dup, "csv", "overwrite", f"{output_path}/vendas")

print(f"✅ Vendas: {df_vendas_com_dup.count()} registros (com 100 duplicatas)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📋 Resumo Geral
# 
# Dados mock gerados com sucesso!

# CELL ********************

df_clientes_csv = read_data(f"{output_path}/clientes", "csv")
df_produtos_csv = read_data(f"{output_path}/produtos", "csv")
df_vendas_csv = read_data(f"{output_path}/vendas", "csv")

print("\n" + "="*60)
print("📊 RESUMO DOS DADOS GERADOS 📊")
print("="*60)
print(f"\n📁 Diretório: {output_path}")
print(f"\n📋 Arquivos criados:")
print(f"   1. clientes.csv - {df_clientes_csv.count():,} registros")
print(f"   2. produtos.csv - {df_produtos_csv.count():,} registros")
print(f"   3. vendas.csv - {df_vendas_csv.count():,} registros")
print(f"\n✅ Relacionamentos:")
print(f"   - Vendas → Clientes (cliente_id)")
print(f"   - Vendas → Produtos (produto_id)")
print(f"\n🔄 Duplicatas intencionais para testar MERGE no Silver:")
print(f"   - Clientes: 50 duplicatas")
print(f"   - Produtos: 20 duplicatas")
print(f"   - Vendas: 100 duplicatas")
print("\n🎯 Pronto para pipeline Medallion!")
print("="*60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.session.stop()
spark.stop
mssparkutils.notebook.exit("exit note mock")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
