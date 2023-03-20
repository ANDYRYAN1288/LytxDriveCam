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
 
headers = {
    'accept': "application/json",
    'x-apikey': os.getenv('PYTHON_LYTX_API_KEY')
    }

url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events?limit=1000&includeSubgroups=true&sortBy=lastUpdatedDate&sortDirection=desc&dateOption=lastUpdatedDate&to=2022-10-03T00%3A00%3A00.00Z&from=2022-10-01T00%3A00%3A00.00Z&page='


# DB Settings----------------------------
TargetServer = os.getenv('PYTHON_DW_01_SERVER')
SchemaName = 'stage.'
TargetDb = 'LYTXDriveCam'
UserName = os.getenv('PYTHON_DW_01_USER')
Password = os.getenv('PYTHON_DW_01_PASS')
controlname  = 'REST_API_SAFETY_EVENTS'

###date example 2022-10-10T00:00:00.00Z to 2022-10-11T00:00:00.00Z
#2022-10-10T00:00:00.00Z
#2022-10-13T11:54:00.00Z
#2021-03-16T16:19:58.80Z
#2022-10-13T13:57:10.171552
#2022-10-13T11:54:00.00Z
#2022-10-14T00:00:00.00Z

#settings
timezone = '.00Z'
timeformat = 'T00:00:00.00Z'
JSOnFolderPath = os.getenv('PYTHON_LYTX_FOLDER_EVENT')

 #Log Variable
LogMessage = 'Process Starting'
LogMessageType = 'Message'
ProcessName  = 'LYTX_API_Safety_Events.py'

url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events?limit=1000&includeSubgroups=true&sortBy=lastUpdatedDate&sortDirection=desc&dateOption=lastUpdatedDate&to={0}&from={1}&page='.format(toDate,lastReadDate)

 
today = date.today() + timedelta(days=1)
toDate = str(today)+timeformat

now = datetime.now()

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
        
    query = "SELECT LASTREADDATE FROM dbo.F_GetControlRecord('{0}')".format(controlname)
    dfLastRead = pd.read_sql(query,Engine)
    lastReadDate = dfLastRead['LASTREADDATE']
    lastReadDate = lastReadDate.values[0]
    lastReadDate = pd.Timestamp(lastReadDate)
    lastReadDate = datetime.isoformat(lastReadDate)
    lastReadDate = str(lastReadDate + timezone)
    
    startPage = 1
    response = requests.get(url + str(startPage),headers=headers)
    data = response.json()
    totalCount = data['totalCount']
    numPages = math.ceil(totalCount/1000)
    print(numPages)
    print(totalCount)
    with open('{0}event1.json'.format(JSOnFolderPath), 'wb') as f:
        f.write(response.content)
    f.close()

    for page in range(2,numPages+1):
        response = requests.get(url + str(page),headers=headers)
        with open('{0}event{1}.json'.format(JSOnFolderPath,page), 'wb') as f:
            f.write(response.content)
        f.close()
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

 