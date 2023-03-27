# import requests module
import requests
import json
import math
from sqlalchemy import create_engine, null
from sqlalchemy import Text as Text
import urllib
import pyodbc
import pandas as pd
from datetime import datetime,date, timedelta
import os
#test andy 2
print('1')
print('2')
print('3')
headers = {
    'accept': "application/json",
    'x-apikey': os.getenv('PYTHON_LYTX_API_KEY'),
    'content-type': "application/json"
    }
 
# Settings----------------------------

TargetServer = os.getenv('PYTHON_DW_01_SERVER')
SchemaName = 'stage.'
TargetDb = 'LYTXDriveCam'
UserName = os.getenv('PYTHON_DW_01_USER')
Password = os.getenv('PYTHON_DW_01_PASS')

#Log Variable
LogMessage = 'Process Starting'
LogMessageType = 'Message'
ProcessName  = 'LYTX_API_Behaviors.py'
 
 

url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events/behaviors'

#db conn
Params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=' + TargetServer + ';DATABASE=' + TargetDb + ';UID=' + UserName + ';PWD=' + Password)
ConnStr = 'mssql+pyodbc:///?odbc_connect={}'.format(Params)
Engine = create_engine(ConnStr,fast_executemany=True)


try:
    #Log Start
    with Engine.begin() as dbconn:
        dbconn.execute("""EXEC	 [LYTXDriveCam].[dbo].[P_PROCESSLOG_INSERT]
		@LOGTYPE = 'PYTHON',
		@LogProcessName = '{0}',
		@LOGMESSAGE = '{1}',
		@LoggerID = '{2}',
		@BatchID = 'none',
		@MessageType = '{3}' """.format(ProcessName,LogMessage,ProcessName,LogMessageType))
    
    startPage = 1
    response = requests.get(url,headers=headers)
    data = response.json()
    print(data)

    dfBehaviors = pd.json_normalize(data)
    
    print(dfBehaviors)

    sql = 'Truncate Table ' + SchemaName +'Behaviors'
    with Engine.begin() as dbconn:
        dbconn.execute(sql)

    if dfBehaviors is not None:
        dfBehaviors.to_sql('Behaviors', con=Engine, schema=SchemaName, if_exists='append', index=False)
        
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