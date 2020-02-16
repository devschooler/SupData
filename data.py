import pymongo
from pymongo import MongoClient

import findspark
findspark.init()
import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
conf = SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")

from pyspark.sql import SparkSession
working_directory = 'jars/*'
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


working_directory = 'jars/*'

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri=mongodb://127.0.0.1/SupinfoDB.students") \
    .config("spark.mongodb.output.uri=mongodb://127.0.0.1/SupinfoDB.students") \
    .config('spark.driver.extraClassPath', working_directory) \
    .getOrCreate()


client=MongoClient('mongodb://localhost:27017/')
db = client["SupinfoDB"]
studentsCollection = db["students"]

result1 = studentsCollection.find_one({"name": "LAMA"})
print(result1)


df = spark.read.format("com.mongodb.sparK.sql.DefaultSource").option("uri","mongodb://127.0.0.1/Supinfo.students").load()

df.count()