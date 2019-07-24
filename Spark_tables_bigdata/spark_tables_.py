import os
from pathlib import Path
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import csv
#
# /home/guilherme/Documents/Spark_tables_bigdata/spark_table_on_txt/files


path = '/home/guilherme/Documents/Spark_tables_bigdata/spark_table_on_txt/files/'
file_domains = ['supersentai']

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


diretorio = Path(path)

criacao = lambda f: f.stat().st_ctime
maisrecente = diretorio.glob('*{0}*.txt'.format(file_domains))
sorted_files = sorted(maisrecente, key=criacao, reverse=True)
print(maisrecente)
  
arquivo = None
try:
    arquivo = str(sorted_files[0])
except:
    print("Nao ha arquivos do dominio {0}".format(file_domains))

tabela = spark.read.option('sep' , ';')\
.option('header', True)\
.option('inferSchema', True)\
.csv(arquivo)\
.cache()
#tabela.show()
tabela = tabela.withColumnRenamed('Série','série')\
.withColumnRenamed('Título em Portuguê','titulo_em_portugues')\
.withColumnRenamed('Título Original','titulo_original')\
.withColumnRenamed('Exibição Original (Japão)','exibicao_oroginal_japao')\
.withColumnRenamed('Temático', 'tematico')\
.withColumnRenamed('Nº de Episódios','qtd_episódios')


tabela.printSchema()
spark.sql('Create Database if not exists  super')
print (spark.catalog.listDatabases())

tabela.write.mode("overwrite")\
.option('header', True)\
.option('path','/home/guilherme/Documents/BigData/spark_table_on_txt/supersentai')\
.saveAsTable('super.supersentai', format='csv')


