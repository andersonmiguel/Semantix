#Import das Bibliotecas                                                                       
from pyspark.sql import * 
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime                                                     
#from operator import add        
#from pyspark.conf import SparkConf
#from pyspark.context import SparkContext


#Define Schema
NASASchemaPandas = {'HOST': str,
                    'dash1': str,
                    'dash2': str,
                    'TIMESTAMP': str,
                    'REQUEST': str,
                    'RETURN_CODE': str,
                    'BYTES_AMOUNT': str}
NASASchema = StructType([
  StructField('HOST', StringType(), True),
  StructField('dash1', StringType(), True),
  StructField('dash2', StringType(), True),
  StructField('TIMESTAMP', StringType(), True),
  StructField('REQUEST', StringType(), True),
  StructField('RETURN_CODE', StringType(), True),
  StructField('BYTES_AMOUNT', StringType(), True)])

#ReadFile
dfNasaAccessLog = spark.read.format('csv').load("/mnt/consumezone/Brazil/SellOut/tmp/NASA/NASA_access_log", quote='"', delimiter=' ',header=True, schema=NASASchema)

                         
###
#1. Número de hosts únicos.

dfUniqueHosts = dfNasaAccessLog.select(dfNasaAccessLog.HOST).distinct()
dfUniqueHosts = dfUniqueHosts.count()
print("Número de hosts únicos")
print(dfUniqueHosts)

###
#2. O total de erros 404.

dfErrors404 = dfNasaAccessLog.select(dfNasaAccessLog.RETURN_CODE).where(dfNasaAccessLog.RETURN_CODE == '404')
dfErrors404 = dfErrors404.count()
print("O total de erros 404")
print(dfErrors404)

###
#3. Os 5 URLs que mais causaram erro 404.
dfErrors404Url = dfNasaAccessLog.select(dfNasaAccessLog.RETURN_CODE,
                                        dfNasaAccessLog.REQUEST
                                       ).where(dfNasaAccessLog.RETURN_CODE == '404')
dfErrors404Url = dfErrors404Url.groupBy("REQUEST").agg(count(dfErrors404Url.RETURN_CODE).alias('#404Error'))
print("O total de erros 404 por dia")
display(((dfErrors404Url).sort(desc('#404Error'))).limit(5))


###
#4. Quantidade de erros 404 por dia.
dfErrors404Dia = dfNasaAccessLog.select(dfNasaAccessLog.RETURN_CODE,
                                        dfNasaAccessLog.TIMESTAMP.substr(1,10).alias('dia')
                                       ).where(dfNasaAccessLog.RETURN_CODE == '404')
dfErrors404Dia = dfErrors404Dia.groupBy("dia").agg(count(dfErrors404Dia.RETURN_CODE).alias('#404Error'))
print("O total de erros 404 por dia")
display(dfErrors404Dia)

###
#5. O total de bytes retornados.
dfBytesAmount = dfNasaAccessLog.groupBy().agg(sum(dfNasaAccessLog.BYTES_AMOUNT).alias('BYTES_AMOUNT'))
print("O total de bytes retornados")
display(dfBytesAmount)
