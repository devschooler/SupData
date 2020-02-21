from __future__ import division
import pymongo
from pymongo import MongoClient

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import pandas_datareader.data as web
import datetime
import dash
import dash_core_components as dcc
import dash_html_components as html

import dash
import dash_core_components as dcc
import dash_html_components as html


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



#1 succes depending on region

cluster = pymongo.MongoClient("mongodb://localhost:27017/")

#calcul total number of paris student 
pipeline = "{'$match' : {'campus': 'Paris'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
parisStudentNumber = df.count()
#print(parisStudentNumber)

#Calcul moyenne etudiants paris 

pipeline = [{'$match' : {'campus': 'Paris'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$overall average' }}}, 
            ]

          #  {'$group':{'_id':{'myid':'$myid'}, 'record':{'$first':'$$ROOT'}}}, 

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()

total_notes_paris = df.collect()
#print(total_notes_paris)
stringed_notes = str(total_notes_paris[0])
total_str_notes = stringed_notes[24:28]
total_notes = int(total_str_notes)
#print(type(total_notes))

moyenne_paris = total_notes / parisStudentNumber 
print( 'la moyenne de paris est ' , moyenne_paris)

#calcul total number of Lyon student 
pipeline = "{'$match' : {'campus': 'Lyon'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
lyonstudentNumber = df.count()

pipeline = [{'$match' : {'campus': 'Lyon'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$overall average' }}}, 
            ]

          #  {'$group':{'_id':{'myid':'$myid'}, 'record':{'$first':'$$ROOT'}}}, 

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()

total_notes_lyon = df.collect()
#print(total_notes_lyon)
stringed_notes_lyon = str(total_notes_lyon[0])
total_str_notes_lyon = stringed_notes_lyon[23:27]
#print(total_str_notes_lyon)
total_notes_lyon = int(total_str_notes_lyon)
#print(type(total_notes_lyon))

moyenne_lyon = total_notes_lyon / lyonstudentNumber 
print( 'la moyenne de Lyon est ' , moyenne_lyon)



#calcul total number of paris Rennes
pipeline = "{'$match' : {'campus': 'Rennes'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
rennesStudentNumber = df.count()
#print(rennesStudentNumber)

#Calcul moyenne etudiants Rennes

pipeline = [{'$match' : {'campus': 'Rennes'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$overall average' }}}, 
            ]

          #  {'$group':{'_id':{'myid':'$myid'}, 'record':{'$first':'$$ROOT'}}}, 

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()

total_notes_rennes = df.collect()
#print(total_notes_rennes)
stringed_notes_rennes = str(total_notes_rennes[0])
total_str_notes_rennes = stringed_notes_rennes[25:28]
#print(total_str_notes_rennes)
total_notes_rennes = int(total_str_notes_rennes)

moyenne_rennes = total_notes_rennes / rennesStudentNumber 
print( 'la moyenne de Rennes est ' , moyenne_rennes)


#calcul total number of Canada student 
pipeline = "{'$match' : {'campus': 'Canada'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
canadaStudentNumber = df.count()
#print(canadaStudentNumber)

#Calcul moyenne etudiants Canada

pipeline = [{'$match' : {'campus': 'Canada'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$overall average' }}}, 
            ]

          #  {'$group':{'_id':{'myid':'$myid'}, 'record':{'$first':'$$ROOT'}}}, 

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()

total_notes_canada = df.collect()
#print(total_notes_canada)
stringed_canada = str(total_notes_canada[0])
total_str_notes_canada = stringed_canada[25:29]
#print(total_notes_canada)
total_notes_canada = int(total_str_notes_canada)
#print(type(total_notes))

moyenne_canada = total_notes_canada / canadaStudentNumber 
print( 'la moyenne du canada est ' , moyenne_canada)



#calcul total number of Marseille student 
pipeline = "{'$match' : {'campus': 'Marseille'}}"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
marseilleStudentNumber = df.count()
#print(canadaStudentNumber)

#Calcul moyenne etudiants marseille

pipeline = [{'$match' : {'campus': 'Marseille'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$overall average' }}}, 
            ]

          #  {'$group':{'_id':{'myid':'$myid'}, 'record':{'$first':'$$ROOT'}}}, 

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()

total_notes_marseille = df.collect()
#print(total_notes_marseille)
stringed_marseille = str(total_notes_marseille[0])
total_str_notes_marseille = stringed_marseille[28:32]
#print(total_notes_marseille)
total_notes_marseille = int(total_str_notes_marseille)
#print(type(total_notes))

moyenne_marseille = total_notes_marseille / marseilleStudentNumber 
print( 'la moyenne de Marseille est ' , moyenne_marseille)
app = dash.Dash()

app.layout = html.Div(children=[
    html.H1(children='SupData Dashboard'),
  dcc.Graph(
        id='example',
        figure={
            'data': [
                {'x': ["Campus de Rennes","Campus de Marseille","Campus du Canada","Campus de Lyon","Campus de Paris"], 'y': [moyenne_rennes,moyenne_marseille,moyenne_canada,moyenne_lyon,moyenne_paris], 'type': 'bar', 'name': 'moyenne etudiants'},
            ],
            'layout': {
                'title': 'Success of the students'
            }
        }
    )
])






if __name__ == '__main__':
    app.run_server(debug=True)