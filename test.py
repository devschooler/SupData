from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

working_directory = 'jars/*'

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri=mongodb://127.0.0.1/SupinfoDB.students") \
    .config("spark.mongodb.output.uri=mongodb://127.0.0.1/SupinfoDB.students") \
    .config('spark.driver.extraClassPath', working_directory) \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").load()
lama = df.count()
print(lama)