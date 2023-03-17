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

 
url = 'https://lytx-api.prod7.lv.lytx.com/vehicles/all?limit=1000&includeSubgroups=true&page='


startPage = 1
response = requests.get(url + str(startPage),headers=headers)
data = response.json()
#print(data)

totalCount = data['totalResults']
numPages = math.ceil(totalCount/1000)
print(numPages)
print(totalCount)

 

dfVehicle  = pd.json_normalize(data,record_path=['vehicles'],errors='ignore')
 
 
#print(dfVehicle)


for page in range(2,numPages+1):
    #print(page)
    print(url + str(page))
    response = requests.get(url + str(page),headers=headers)
    data = response.json()
    #print(data)
    dfVehicle2  = pd.json_normalize(data,record_path=['vehicles'],errors='ignore')
    dfVehicle = pd.concat([dfVehicle,dfVehicle2])

 


Params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=' + TargetServer + ';DATABASE=' + TargetDb + ';UID=' + UserName + ';PWD=' + Password)
ConnStr = 'mssql+pyodbc:///?odbc_connect={}'.format(Params)
Engine = create_engine(ConnStr,fast_executemany=True)

sql = 'Truncate Table ' + SchemaName +'Vehicles'
with Engine.begin() as dbconn:
    dbconn.execute(sql)
 
if dfVehicle is not None:
     dfVehicle.to_sql('Vehicles', con=Engine, schema=SchemaName, if_exists='append', index=False)
