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
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd


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
fair_disp1 = str(fair_percent)
fair_display = fair_disp1 + " pourcent des etudiants sont engages apres un job forum Supinfo"



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

#calcul time after hired Paris 


pipeline = [{'$match' : {'campus': 'Paris'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$time after hired' }}}, 
            ]


df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
total_month_paris = df.collect()
#print(total_month_paris)
stringed_months_paris = str(total_month_paris[0])
#print(stringed_months_paris)
total_str_month_paris = stringed_months_paris[24:28]
time_after_hired_paris = int(total_str_month_paris)
#print('total mois paris ' , time_after_hired_paris)

#Time after hired Lyon

pipeline = [{'$match' : {'campus': 'Lyon'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$time after hired' }}}, 
            ]


df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
total_month_lyon = df.collect()
#print(total_month_lyon)
stringed_months_lyon = str(total_month_lyon[0])
#print(stringed_months_lyon)
total_str_month_lyon = stringed_months_lyon[23:26]
time_after_hired_lyon= int(total_str_month_lyon)
#print('total mois  lyon' , time_after_hired_lyon)

#Time after hired Canada

pipeline = [{'$match' : {'campus': 'Canada'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$time after hired' }}}, 
            ]


df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
total_month_canada = df.collect()
#print(total_month_canada)
stringed_months_canada= str(total_month_canada[0])
#print(stringed_months_canada)
total_str_month_canada = stringed_months_canada[25:29]
time_after_hired_canada= int(total_str_month_canada)
#print('total mois  canada' , time_after_hired_canada)

#Time after hired Rennes
pipeline = [{'$match' : {'campus': 'Rennes'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$time after hired' }}}, 
            ]


df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
total_month_rennes = df.collect()
#print(total_month_rennes)
stringed_months_rennes= str(total_month_rennes[0])
#print(stringed_months_rennes)
total_str_month_rennes = stringed_months_rennes[25:28]
time_after_hired_rennes= int(total_str_month_rennes)
#print('total mois  rennes' , time_after_hired_rennes)

#Time after hired Marseille
pipeline = [{'$match' : {'campus': 'Marseille'}},
            {'$group':{ '_id': '$campus', 'total': { '$sum': '$time after hired' }}}, 
            ]


df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
total_month_marseille = df.collect()
#print(total_month_marseille)
stringed_months_marseille= str(total_month_marseille[0])
#print(stringed_months_marseille)
total_str_month_marseille = stringed_months_marseille[28:32]
time_after_hired_marseille= int(total_str_month_marseille)
#print('total mois  marseille' , time_after_hired_marseille)

total_months_after_hired = time_after_hired_canada + time_after_hired_lyon + time_after_hired_marseille + time_after_hired_paris + time_after_hired_rennes
#print(total_months_after_hired)
average_length_hired = total_months_after_hired / total_students
print('les eleves sont engages en moyenne apres ', average_length_hired , 'mois')

#moyenne embauche
average_hired_canada = time_after_hired_canada / canadaStudentNumber 
average_hired_lyon = time_after_hired_lyon / lyonstudentNumber
average_hired_marseille = time_after_hired_marseille / marseilleStudentNumber
average_hired_paris = time_after_hired_paris / parisStudentNumber
average_hired_rennes = time_after_hired_rennes / rennesStudentNumber
avg_hired = 'Les etudiants sont embauches en moyenne ' 
avg_hired2 =  ' mois apres leur diplome '
avg_hired3 = str("%.1f" % average_length_hired)
avg_hired_to_display = avg_hired + avg_hired3 + avg_hired2
print(avg_hired_to_display)


#Contrats pro par region 
#Paris
pipeline = "{'$match' : {'campus': 'Paris','internship': true}}"

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
internship_paris = df.count()

#Lyon
pipeline = "{'$match' : {'campus': 'Lyon','internship': true}}"

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
internship_Lyon = df.count()

#Rennes
pipeline = "{'$match' : {'campus': 'Rennes','internship': true}}"

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
internship_Rennes = df.count()

#Canada
pipeline = "{'$match' : {'campus': 'Canada','internship': true}}"

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
internship_Canada = df.count()

#Marseille
pipeline = "{'$match' : {'campus': 'Marseille','internship': true}}"

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://127.0.0.1/SupinfoDB.students").option("pipeline",pipeline).load()
internship_Marseille = df.count()





app = dash.Dash()




app.layout = html.Div([
    html.H1('Supinfo Big Data Dashboard'),
    dcc.Tabs(id="tabs-example", value='tab-1-example', children=[
        dcc.Tab(label='Indicateurs de reussite', value='tab-1'),
        dcc.Tab(label='Impact des job forum', value='tab-2'),
        dcc.Tab(label='Impact de Supinfo sur embauche', value='tab-3'),
        dcc.Tab(label='Densite eleves par campus', value='tab-4'),
        dcc.Tab(label='Statistiques Contrat Pro', value='tab-5'),


    ]),
    html.Div(id='tabs-content-example')
])
not_hired_after_fair = total_students - number_hired_after_fair
df = pd.DataFrame(dict(Impact=["Non engage apres job forum","engage apres job forum"], Job_Forum=[not_hired_after_fair,number_hired_after_fair]))


@app.callback(Output('tabs-content-example', 'children'),
              [Input('tabs-example', 'value')])
def render_content(tab):
    if tab == 'tab-1':
        return html.Div([
         #   html.H3('Moyenne des eleves selon leur campus'),
        dcc.Graph(
        id='example',
        figure={
            'data': [
                {'x': ["Campus de Rennes","Campus de Marseille","Campus du Canada","Campus de Lyon","Campus de Paris"], 'y': [moyenne_rennes,moyenne_marseille,moyenne_canada,moyenne_lyon,moyenne_paris], 'type': 'bar', 'name': 'moyenne etudiants'},
            ],
            'layout': {
                'title': 'Moyenne des eleves selon leur campus'
            }
        }
    ),

        ])



    elif tab == 'tab-2':
        return html.Div([
            html.H3(fair_display),
            dcc.Graph(
                id='graph-1-tabs',
             figure = px.pie(df, values='Job_Forum', names='Impact')
            )
        ]),

    elif tab == 'tab-3':
        return html.Div([ 
           # html.H3('Les eleves sont embauches en moyenne  mois apres leur diplome',{{average_length_hired}} ),
            #html.H3(avg_hired_to_display),

            dcc.Graph(
        id='example',
        figure={
            'data': [
                {'x': ["Campus de Rennes","Campus de Marseille","Campus du Canada","Campus de Lyon","Campus de Paris"], 'y': [average_hired_rennes,average_hired_marseille,average_hired_canada,average_hired_lyon,average_hired_paris], 'type': 'bar', 'name': 'moyenne etudiants'},
            ],
            'layout': {
                'title': avg_hired_to_display
            }
        }
    ),
        ])
    elif tab == 'tab-4':
        return html.Div([ 
           # html.H3('Les eleves sont embauches en moyenne  mois apres leur diplome',{{average_length_hired}} ),
            #html.H3(avg_hired_to_display),

            dcc.Graph(
        id='example',
        figure={
            'data': [
                {'x': ["Campus de Rennes","Campus de Marseille","Campus du Canada","Campus de Lyon","Campus de Paris"], 'y': [rennesStudentNumber,marseilleStudentNumber,canadaStudentNumber,lyonstudentNumber,parisStudentNumber], 'type': 'bar', 'name': 'Etudiants par campus'},
            ],
            'layout': {
                'title': "Nombre etudiants par campus"
            }
        }
    ),
        ]) 

    elif tab == 'tab-5':
        return html.Div([ 
           # html.H3('Les eleves sont embauches en moyenne  mois apres leur diplome',{{average_length_hired}} ),
            #html.H3(avg_hired_to_display),

            dcc.Graph(
        id='example',
        figure={
            'data': [
                {'x': ["Campus de Rennes","Campus de Marseille","Campus du Canada","Campus de Lyon","Campus de Paris"], 'y': [internship_Rennes,internship_Marseille,internship_Canada,internship_Lyon,internship_paris], 'type': 'bar', 'name': 'Etudiants par campus'},
            ],
            'layout': {
                'title': "Nombre de contrat pro par campus"
            }
        }
    ),
        ])       




if __name__ == '__main__':
    app.run_server(debug=True)