import pyspark

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = pyspark.SparkContext(master = "spark://node1:7077", appName='reading-and-slicing')

spark = SparkSession(sc)

# Lendo arquivo
df = spark.read.csv('hdfs://node1:9000/raw/DNPBA2017.csv', header=True)

# Separando variaveis de interesse
df = df.select(['NUMERODN', 'CODMUNNASC', 'DTNASC', 'SEXO', 'RACACOR', 'PESO', 'RACACORMAE'])

# Escrevendo resultado
df.write.parquet('hdfs://node1:9000/raw/DNPBA2017-airflow')


