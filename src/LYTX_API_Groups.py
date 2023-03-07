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


url = 'https://lytx-api.prod7.lv.lytx.com/groups?limit=100&includeSubgroups=true&groupOption=id&page='


startPage = 1
response = requests.get(url + str(startPage),headers=headers)
data = response.json()
print(data)

totalCount = data['totalCount']
numPages = math.ceil(totalCount/100)
print(numPages)
print(totalCount)
 

dfGroups  = pd.json_normalize(data,record_path=['groups'],errors='ignore')

 
print(dfGroups)

for page in range(2,numPages+1):
    print(page)
    response = requests.get(url + str(page),headers=headers)
    data = response.json()
    print(data)
    dfGroup2  = pd.json_normalize(data,record_path=['groups'],errors='ignore')
    dfGroups = pd.concat([dfGroups,dfGroup2])
    


Params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=' + TargetServer + ';DATABASE=' + TargetDb + ';UID=' + UserName + ';PWD=' + Password)
ConnStr = 'mssql+pyodbc:///?odbc_connect={}'.format(Params)
Engine = create_engine(ConnStr,fast_executemany=True)

sql = 'Truncate Table ' + SchemaName +'Groups'
with Engine.begin() as dbconn:
    dbconn.execute(sql)
 
if dfGroups is not None:
     dfGroups.to_sql('Groups', con=Engine, schema=SchemaName, if_exists='append', index=False)