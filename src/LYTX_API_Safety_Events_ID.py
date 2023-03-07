# import requests module
import requests
import json
import math
from sqlalchemy import create_engine, null
import urllib
import pyodbc
import pandas as pd
from datetime import datetime,date, timedelta
import os
import codecs
import csv
import re
from pandas.io.json import json_normalize



print(datetime.now())

###10/13/2022 - Andy - new - Call event API. loop through total pages for start/end date entered and save JSON file for further processing
 
headers = {
    'accept': "application/json",
    'x-apikey': "AZKPtaz4QsRxT66E01W26OvEQF4ncfmC"
    }

url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events/'
url2 = '?idOption=id&includeSubgroups=true'


# Settings----------------------------
TargetServer = 'nfiv-sqldw-01d'
SchemaName = 'stage.'
TargetDb = 'LYTXDriveCam'
TableName = 'tbLYTXEventsData'
UserName = 'BI_ETLUser'
Password = 'BI_ETLUser'

#settings
JSONSourceFolderPath = '\\\\nfii\\root\\Interfaces\\BI\\Dev\\LYTX\\JSON\\Event\\'

# files = []

# for file in os.listdir(JSOnFolderPath):
#     if file.endswith(".json"):
#         print('test')
#         files.append(os.path.join(JSOnFolderPath, file))

# data_list = []
# for file in os.listdir(JSOnFolderPath):

#     #If file is a json, construct it's full path and open it, append all json data to list
#     if 'json' in file:
#         json_path = os.path.join(JSOnFolderPath, file)
#         json_data = pd.read_json(json_path, lines=True)
#         #eventIDS = json_data.values['eventIDS']
#         data_list.append(json_data)
#         #print(eventIDS)

# print(data_list)

# f_all = pd.DataFrame.from_records( data_list )

# print(f_all)
# print(type(data_list))
# print(type(f_all))


 
# json_data = []
# for file in os.listdir(JSOnFolderPath):
#     print(file)
#     with open(JSOnFolderPath+file) as file:
#         data = json.load(file)
#     json_data.append( json_normalize(data['eventIDs']) )

# df_all = pd.DataFrame.from_records( json_data )

# print(df_all)


files = []
dfAllEventDetails = pd.DataFrame()
dfAllBehaviors = pd.DataFrame()
dfAllNotes = pd.DataFrame()

for file in os.listdir(JSONSourceFolderPath):
    if file.endswith(".json"):
        #print(file)
        #files.append(os.path.join(JSONSourceFolderPath, file))
        with open(JSONSourceFolderPath+file) as currfile:
            print(currfile)
            print(datetime.now())
            EventsDict = json.load(currfile)
            #print(type(data))
            #print('hello')
            #print(data['eventsIds'])
            for x in EventsDict["eventsIds"]:
                currentUrl = url + x + url2
                # print(x)
                #print(currentUrl)
                response = requests.get(currentUrl,headers=headers,timeout=1000)
                EventsDetailDict = response.json()
                # print(EventsDetailDict)
                # print(type(EventsDetailDict))
                dfBehavior  = pd.json_normalize(EventsDetailDict,record_path=['behaviors'],meta='id',record_prefix='bhv_',errors='ignore')
                dfNotes  = pd.json_normalize(EventsDetailDict,record_path=['notes'],meta='id',record_prefix='en_',errors='ignore')
                for item,value in EventsDetailDict.items():
                    if value is null:
                        EventsDetailDict[item] = "no value"
                dfEventDetail = pd.json_normalize(EventsDetailDict)
                dfEventDetail.drop('behaviors', axis=1, inplace=True)
                dfEventDetail.drop('notes', axis=1, inplace=True)
                dfAllEventDetails = pd.concat([dfAllEventDetails,dfEventDetail])
                dfAllBehaviors = pd.concat([dfAllBehaviors,dfBehavior])
                dfAllNotes = pd.concat([dfAllNotes,dfNotes])
        #os.remove(JSONSourceFolderPath+file)

         #data = json.load(JSOnFolderPath)


Params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=' + TargetServer + ';DATABASE=' + TargetDb + ';UID=' + UserName + ';PWD=' + Password)
ConnStr = 'mssql+pyodbc:///?odbc_connect={}'.format(Params)
Engine = create_engine(ConnStr,fast_executemany=True)

sql = 'Truncate Table ' + SchemaName +'EventBehaviors'
with Engine.begin() as dbconn:
    dbconn.execute(sql)
sql = 'Truncate Table ' + SchemaName +'EventNotes'
with Engine.begin() as dbconn:
    dbconn.execute(sql)
sql = 'Truncate Table ' + SchemaName +'Events'
with Engine.begin() as dbconn:
    dbconn.execute(sql)

if dfAllBehaviors is not None:
     dfAllBehaviors.to_sql('EventBehaviors', con=Engine, schema=SchemaName, if_exists='append', index=False)
if dfAllNotes is not None:
    dfAllNotes.to_sql('EventNotes', con=Engine, schema=SchemaName, if_exists='append', index=False)
# # Load the Data in DataFrame into Table
dfAllEventDetails.to_sql('Events', con=Engine, schema=SchemaName, if_exists='append', index=False)
    
print(datetime.now())


# print(dfAllEventDetails)
# print(dfAllBehaviors)
# print(dfAllNotes)
# print(type(EventsDict))