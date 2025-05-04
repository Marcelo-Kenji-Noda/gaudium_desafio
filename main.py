from datetime import datetime, date
from pathlib import Path
import os
import tomllib

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import hash, min, max, day, year, dayofweek, month

if __name__ == '__main__':
    today = date.today()
    BASE_DIR = Path(__file__).resolve().parent
    with open(os.path.join(BASE_DIR,"config.toml"), "rb") as file:
        cfg = tomllib.load(file)
        
    spark = SparkSession.builder.appName("Desafio Gaudium").getOrCreate()
    
    df = spark.read.csv(os.path.join(BASE_DIR, cfg['input_dir'], cfg['input_file']), header=True, inferSchema=True)
    
    ##### Cliente DF
    if cfg['clientes']['consolidate']:
        clientes_df = (
            df.select(["nome_cliente","cidade","estado"])
            .drop_duplicates(subset=cfg['clientes']['unique_identifiers_cols'])
            .withColumn("id_cliente", hash("nome_cliente","cidade","estado"))
        )
        clientes_pd_df = clientes_df.toPandas()
        
        output_path = os.path.join(BASE_DIR, cfg['output_dir'], cfg['clientes']['output_filepath'])
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        clientes_pd_df.to_csv(os.path.join(output_path, f"clientes_{today.strftime("%Y%m%d")}.csv"), index=False)
        clientes_pd_df.to_csv(os.path.join(output_path, "clientes.csv"), index=False)

    ##### Produtos
    if cfg['produtos']['consolidate']:
        produtos_df = (
            df.select(["nome_produto","categoria","fabricante"])
            .drop_duplicates(cfg['produtos']['unique_identifiers_cols'])
            .withColumn("id_produto", hash("nome_produto", "fabricante"))
        )
        produtos_pd_df = produtos_df.toPandas()
        
        output_path = os.path.join(BASE_DIR, cfg['output_dir'], cfg['produtos']['output_filepath'])
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        produtos_pd_df.to_csv(os.path.join(output_path, f"produtos_{today.strftime("%Y%m%d")}.csv"), index=False)
        produtos_pd_df.to_csv(os.path.join(output_path, "produtos.csv"), index=False)

    ##### Datas
    if cfg['datas']['consolidate']:
        ini_date = df.select(min("data")).collect()[0][0]
        end_date = df.select(max("data")).collect()[0][0]

        datas_df = spark.sql(
            f"""
            SELECT explode(sequence(to_date('{ini_date.strftime("%Y-%m-%d")}'), to_date('{end_date.strftime("%Y-%m-%d")}'), interval 1 day)) as data
            """
        )

        datas_df = (
            datas_df.withColumn("dia", day('data'))
                .withColumn("mes", month('data'),)
                .withColumn('ano', year('data'))
                .withColumn('dia_da_semana',dayofweek('data')) 
        )
        
        datas_pd_df = datas_df.toPandas()
        
        output_path = os.path.join(BASE_DIR, cfg['output_dir'], cfg['datas']['output_filepath'])
        if not os.path.exists(output_path):
            os.mkdir(output_path)
        datas_pd_df.to_csv(os.path.join(output_path, f"datas_{today.strftime("%Y%m%d")}.csv"), index=False)
        datas_pd_df.to_csv(os.path.join(output_path, "datas.csv"), index=False)
    
    #### Consolidação da tabela de vendas fato
    if cfg['vendas_fato']['consolidate']:
        vendas_fato = (
        df.withColumn("id_cliente", hash("nome_cliente","cidade","estado"))
            .withColumn("id_produto", hash("nome_produto", "fabricante"))
            .select(["data","id_cliente","id_produto","qtd_vendida","valor_total"])
        )
        vendas_fato_pd_df = vendas_fato.toPandas()
        
        output_path = os.path.join(BASE_DIR, cfg['output_dir'], cfg['vendas_fato']['output_filepath'])
        
        if not os.path.exists(output_path):
            os.mkdir(output_path)
            
        vendas_fato_pd_df.to_csv(os.path.join(output_path, f"vendas_fato_{today.strftime("%Y%m%d")}.csv"), index=False)
        vendas_fato_pd_df.to_csv(os.path.join(output_path, "vendas_fato.csv"), index=False)