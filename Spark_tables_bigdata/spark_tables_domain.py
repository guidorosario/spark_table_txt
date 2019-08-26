import os
from pathlib import Path
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import csv

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
path = 'spark_table_on_txt/files/'
file_domains = ['supersentai','kamenrider','powerrangers']


def powerrangers(tabela): 
  
  tabela = tabela.withColumnRenamed('Título no Brasil','titulo_no_brasil')\
  .withColumnRenamed('Título Original','titulo_original')\
  .withColumnRenamed('Exibição Original (EUA)','exibicao_original_eua')\
  .withColumnRenamed('Canal fechado (BRA)','canal_fechado_bra')\
  .withColumnRenamed('Super Sentai', 'super_sentai')\
  .withColumnRenamed('Tema', 'tema')\
  .withColumnRenamed('Temporada', 'temporada')\
  .withColumnRenamed('Episódios', 'episodios')

  tabela.write.mode("overwrite")\
  .option('header', True)\
  .option('path','/home/guilherme/Documents/BigData/spark_table_on_txt/supersentai')\
  .saveAsTable('tokusatsu.powerrangers', format='csv')

def supersentai(tabela):
  
  tabela = spark.read.option('sep' , ';')\
  .option('header', True)\
  .option('inferSchema', True).csv(arquivo)\
  .cache()

  tabela = tabela.withColumnRenamed('Série','série')\
  .withColumnRenamed('Título em Portuguê','titulo_em_portugues')\
  .withColumnRenamed('Título Original','titulo_original')\
  .withColumnRenamed('Exibição Original (Japão)','exibicao_oroginal_japao')\
  .withColumnRenamed('Temático', 'tematico')\
  .withColumnRenamed('Nº de Episódios','qtd_episódios')
  

  tabela.write.mode("overwrite")\
  .option('header', True)\
  .option('path','/home/guilherme/Documents/BigData/spark_table_on_txt/supersentai')\
  .saveAsTable('tokusatsu.supersentai', format='csv')

def kamenrider(tabela): 
  
  tabela = tabela.withColumnRenamed('Título Original','titulo_original')\
  .withColumnRenamed('Exibição Original (Japão)','exibicao_original_japao')\
  .withColumnRenamed('Série','serie')\
  .withColumnRenamed('Nº de Episódios', 'qtd_episodios')

  tabela.write.mode("overwrite")\
  .option('header', True)\
  .option('path','/home/guilherme/Documents/BigData/spark_table_on_txt/supersentai')\
  .saveAsTable('tokusatsu.kamenrider', format='csv')


def get_most_recent_file(path,file_domain) :
  
  diretorio = Path(path)
  criacao = lambda f: f.stat().st_ctime
  maisrecente = diretorio.glob('*{0}*.txt'.format(file_domain))
  sorted_files = sorted(maisrecente, key=criacao, reverse=True)

  result = None
  try:
    result = str(sorted_files[0])
  except:
    print("Nao ha arquivos do dominio {0}".format(file_domain))
    
  return result



if __name__ == "__main__":

  spark.sql('Create Database if not exists  tokusatsu')

  for domain in file_domains:

    arquivo = get_most_recent_file(path,domain)

    tabela = spark.read.option('sep' , ';')\
    .option('header', True)\
    .option('inferSchema', True)\
    .csv(arquivo)\
    .cache()



    if domain == 'supersentai':
      print('Super Sentai', arquivo)
      supersentai(tabela)

    elif domain == 'kamenrider':
      print('Kamen Rider', arquivo)
      kamenrider(tabela)

    elif domain == 'powerrangers':
      print('Power Rangers ' , arquivo)
      powerrangers(tabela)

    
    else:
      pass
