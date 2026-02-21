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

# CELL ********************

def read_data(path_table, type):
    if type ==  "csv":
        df = (spark.read
                   .format("csv")
                   .option("header", "true")
                   .load(path_table))
    if type ==  "delta":
        df = (spark.read
                   .format("delta")
                   .load(path_table))
    if type ==  "table":
        df = (spark.table(path_table))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_data(df, type, mode_ingest, path_table):
    if type ==  "csv":
        (df.coalesce(1)
           .write
           .format(type)
           .mode(mode_ingest)
           .option("header", "true")
           .save(path_table))
    if type ==  "delta":
        (df.write
           .format(type)
           .mode(mode_ingest)
           .option("mergeSchema", "true")
           .option("overwriteSchema", "true")
           .saveAsTable(path_table))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_env():
    # 1. Busca automaticamente TODOS os schemas do catálogo atual
    schemas_df = spark.sql("SHOW SCHEMAS")

    # 2. Itera sobre cada resultado encontrado (sem filtros manuais)
    for schema_row in schemas_df.collect():
        # Pega o namespace completo (Ex: TARN_DEV.TARN_LH_DEV.bronze)
        full_schema_name = schema_row.namespace
        
        print(end="\n")
        print(f"--- Verificando Schema: {full_schema_name} ---")
        
        try:
            # 3. Lista as tabelas dentro do schema atual
            tables_df = spark.sql(f"SHOW TABLES IN {full_schema_name}")
            
            # Se houver tabelas, inicia o loop de exclusão
            if tables_df.count() > 0:
                for table_row in tables_df.collect():
                    table_name = table_row.tableName
                    
                    # Monta o caminho completo para garantir o drop correto
                    full_table_path = f"{full_schema_name}.{table_name}"
                    
                    # Executa o DROP
                    spark.sql(f"DROP TABLE IF EXISTS {full_table_path}")
                    print(f"   -> Tabela removida: {full_table_path}")
            else:
                print(f"   -> Nenhuma tabela encontrada em {full_schema_name}")

        except Exception as e:
            # Bloco de segurança caso tente acessar um schema de sistema ou sem permissão
            print(f"   -> Pulei o schema {full_schema_name}. Motivo: {e}")

    print(end="\n")
    try:
        ## Clean Path
        path_ecommerce = "abfss://TARN_DEV@onelake.dfs.fabric.microsoft.com/TARN_LH_DEV.Lakehouse/Files/data/transient"
        mssparkutils.fs.rm(path_ecommerce, True)

        print(f"-> Path ecommerce deletado")
    except: 
        print("Não há Path para ser exluido!!!")

    print("Processo finalizado para todos os schemas encontrados.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
