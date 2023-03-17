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
    'x-apikey': os.getenv('PYTHON_LYTX_API_KEY'),
    'content-type': "application/json"
    }




# Settings----------------------------
TargetServer = os.getenv('PYTHON_DW_01_SERVER')
SchemaName = 'stage.'
TargetDb = 'LYTXDriveCam'
TableName = 'tbLYTXEventsData'
UserName = os.getenv('PYTHON_DW_01_USER')
Password = os.getenv('PYTHON_DW_01_PASS')



url = 'https://lytx-api.prod7.lv.lytx.com/vehicles/types'


startPage = 1
response = requests.get(url,headers=headers)
data = response.json()
print(data)


 

dfVehicleTypes  = pd.json_normalize(data)
 
 
print(dfVehicleTypes)

 


Params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=' + TargetServer + ';DATABASE=' + TargetDb + ';UID=' + UserName + ';PWD=' + Password)
ConnStr = 'mssql+pyodbc:///?odbc_connect={}'.format(Params)
Engine = create_engine(ConnStr,fast_executemany=True)

sql = 'Truncate Table ' + SchemaName +'VehicleTypes'
with Engine.begin() as dbconn:
    dbconn.execute(sql)
 
if dfVehicleTypes is not None:
     dfVehicleTypes.to_sql('VehicleTypes', con=Engine, schema=SchemaName, if_exists='append', index=False)
