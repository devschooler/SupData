from __future__ import division
import pymongo
from pymongo import MongoClient

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

#total students
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").load()
total_students = df.count()



#5 what is the impact of student fair on recruitments

pipeline = "{'$match' : {'hired after student fair': true}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
number_hired_after_fair = df.count()

data =  (number_hired_after_fair / total_students ) * 100
data_str = str(data)
fair_percent = "%.1f" % data

print(number_hired_after_fair , 'students are hired after student fair, representing ', fair_percent,'percent of the students')

#1 succes depending on region

cluster = pymongo.MongoClient("mongodb://localhost:27017/")

#calcul total number of paris student 
pipeline = "{'$match' : {'campus': 'Paris'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
parisStudentNumber = df.count()
print(parisStudentNumber)

#Calcul moyenne etudiants paris 

pipeline = [{'$match' : {'campus': 'Paris'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$overall average' }}}, 
            ]

          #  {'$group':{'_id':{'myid':'$myid'}, 'record':{'$first':'$$ROOT'}}}, 

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()

total_notes_paris = df.collect()
print(total_notes_paris)
stringed_notes = str(total_notes_paris[0])
total_str_notes = stringed_notes[24:28]
total_notes = int(total_str_notes)
print(type(total_notes))

moyenne_paris = total_notes / parisStudentNumber 
print( 'la moyenne de paris est ' , moyenne_paris)



