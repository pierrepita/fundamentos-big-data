import pyspark
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch(http_compress=True)

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = pyspark.SparkContext(master = "spark://node1:7077", appName='es-query')

spark = SparkSession(sc)

# Gerando query

content = { "query": { "term": { "numerodn": "72392109" } },}

# Submetendo query
res = es.search(index="dnpba_2017_airflow", body=content)

# colocando resultado em um DataFrame pandas
pd_es = pd.DataFrame([res['hits']['hits'][0]['_source']])

# Exportando DataFrame pandas para DataFrame spark
df_es = spark.createDataFrame(pd_es)

# Escrevendo resultado no HDFS, na pasta 'datasets'
df_es.write.parquet('hdfs://node1:9000/datasets/results-DNPBA2017-query-airflow')




