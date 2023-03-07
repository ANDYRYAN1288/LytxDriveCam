# import requests module
import requests
import json
import math
from sqlalchemy import create_engine, null
import urllib
import pyodbc
import pandas as pd
from datetime import datetime,date, timedelta


headers = {
    'accept': "application/json",
    'x-apikey': "AZKPtaz4QsRxT66E01W26OvEQF4ncfmC",
    'content-type': "application/json"
    }




# Settings----------------------------
TargetServer = 'nfiv-sqldw-01d'
SchemaName = 'stage.'
TargetDb = 'LYTXDriveCam'
TableName = 'tbLYTXEventsData'
UserName = 'BI_ETLUser'
Password = 'BI_ETLUser'



#url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events?limit=1000&includeSubgroups=true&sortBy=lastUpdatedDate&sortDirection=desc&dateOption=lastUpdatedDate&to={0}&from={1}&page='.format(toDate,lastReadDate)


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
