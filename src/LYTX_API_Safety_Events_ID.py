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


 
headers = {
    'accept': "application/json",
    'x-apikey': os.getenv('PYTHON_LYTX_API_KEY')
    }

url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events/'
url2 = '?idOption=id&includeSubgroups=true'


# Settings----------------------------
TargetServer = os.getenv('PYTHON_DW_01_SERVER')
SchemaName = 'stage.'
TargetDb = 'LYTXDriveCam'
UserName = os.getenv('PYTHON_DW_01_USER')
Password = os.getenv('PYTHON_DW_01_PASS')

#settings
JSONSourceFolderPath = os.getenv('PYTHON_LYTX_FOLDER_EVENT')

#Log Variable
LogMessage = 'Process Starting'
LogMessageType = 'Message'
ProcessName  = 'LYTX_API_Safety_Events_ID.py'

files = []
dfAllEventDetails = pd.DataFrame()
dfAllBehaviors = pd.DataFrame()
dfAllNotes = pd.DataFrame()

#db conn
Params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=' + TargetServer + ';DATABASE=' + TargetDb + ';UID=' + UserName + ';PWD=' + Password)
ConnStr = 'mssql+pyodbc:///?odbc_connect={}'.format(Params)
Engine = create_engine(ConnStr,fast_executemany=True)

try:
    
    with Engine.begin() as dbconn:
        dbconn.execute("""EXEC	 [LYTXDriveCam].[dbo].[P_PROCESSLOG_INSERT]
		@LOGTYPE = 'PYTHON',
		@LogProcessName = '{0}',
		@LOGMESSAGE = '{1}',
		@LoggerID = '{2}',
		@BatchID = 'none',
		@MessageType = '{3}' """.format(ProcessName,LogMessage,ProcessName,LogMessageType))
        
    for file in os.listdir(JSONSourceFolderPath):
        if file.endswith(".json"):
            with open(JSONSourceFolderPath+file) as currfile:
                print(currfile)
                print(datetime.now())
                EventsDict = json.load(currfile)
                for x in EventsDict["eventsIds"]:
                    currentUrl = url + x + url2
                    response = requests.get(currentUrl,headers=headers,timeout=1000)
                    EventsDetailDict = response.json()
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
            os.remove(JSONSourceFolderPath+file)

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

except Exception  as e:
    LogMessage = str(e).replace("'", "")[:8000]
    LogMessageType = 'Error'
else:
    LogMessage = 'Compelted Successfully'
    LogMessageType = 'Success'
finally:
    with Engine.begin() as dbconn:
        dbconn.execute("""EXEC	 [LYTXDriveCam].[dbo].[P_PROCESSLOG_INSERT]
		@LOGTYPE = 'PYTHON',
		@LogProcessName = '{0}',
		@LOGMESSAGE = '{1}',
		@LoggerID = '{2}',
		@BatchID = 'none',
		@MessageType = '{3}' """.format(ProcessName,LogMessage,ProcessName,LogMessageType))
    
 
 