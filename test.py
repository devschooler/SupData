from __future__ import division

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
total_students = df.count()
#print(lama)

pipeline = "{'$match' : {'name': 'LAMA'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
#print(df)
#counter = df.collect()
#print(counter)

#what is the impact of student fair on recruitments

pipeline = "{'$match' : {'hired after student fair': true}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
number_hired_after_fair = df.count()

data =  (number_hired_after_fair / total_students ) * 100
data_str = str(data)
fair_percent = "%.1f" % data

print(number_hired_after_fair , 'students are hired after student fair, representing ', fair_percent,'percent of the students')
