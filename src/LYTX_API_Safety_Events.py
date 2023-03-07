# import requests module
import requests
import json
import math
from sqlalchemy import create_engine, null
import urllib
import pyodbc
import pandas as pd
from datetime import datetime,date, timedelta

###10/13/2022 - Andy - new - Call event API. loop through total pages for start/end date entered and save JSON file for further processing
 
headers = {
    'accept': "application/json",
    'x-apikey': "AZKPtaz4QsRxT66E01W26OvEQF4ncfmC"
    }

url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events?limit=1000&includeSubgroups=true&sortBy=lastUpdatedDate&sortDirection=desc&dateOption=lastUpdatedDate&to=2022-10-03T00%3A00%3A00.00Z&from=2022-10-01T00%3A00%3A00.00Z&page='


# DB Settings----------------------------
TargetServer = 'nfiv-sqldw-01d'
SchemaName = 'stage.'
TargetDb = 'LYTXDriveCam'
TableName = 'tbLYTXEventsData'
UserName = 'BI_ETLUser'
Password = 'BI_ETLUser'
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
JSOnFolderPath = '\\\\nfii\\root\\Interfaces\\BI\\Dev\\LYTX\\JSON\\Event\\'
print(JSOnFolderPath)

 

# today  = datetime.utcnow()
# print(today)
# #today = datetime.
# print(today)
# today = datetime.isoformat(today)
# print(today)

today = date.today() + timedelta(days=1)
toDate = str(today)+timeformat

now = datetime.now()

Params = urllib.parse.quote_plus(r'DRIVER={SQL Server};SERVER=' + TargetServer + ';DATABASE=' + TargetDb + ';UID=' + UserName + ';PWD=' + Password)
ConnStr = 'mssql+pyodbc:///?odbc_connect={}'.format(Params)
Engine = create_engine(ConnStr,fast_executemany=True)

 
#with Engine.begin() as dbconn:
query = "SELECT LASTREADDATE FROM dbo.F_GetControlRecord('{0}')".format(controlname)
print(query)
dfLastRead = pd.read_sql(query,Engine)
lastReadDate = dfLastRead['LASTREADDATE']
lastReadDate = lastReadDate.values[0]
print(type(lastReadDate))
lastReadDate = pd.Timestamp(lastReadDate)
print(type(lastReadDate))
lastReadDate = datetime.isoformat(lastReadDate)
print(lastReadDate)
#print(dfLastRead)
lastReadDate = str(lastReadDate + timezone)
print(lastReadDate)
 
print("TEST")
url = 'https://lytx-api.prod7.lv.lytx.com/video/safety/events?limit=1000&includeSubgroups=true&sortBy=lastUpdatedDate&sortDirection=desc&dateOption=lastUpdatedDate&to={0}&from={1}&page='.format(toDate,lastReadDate)

print(url)
startPage = 1
response = requests.get(url + str(startPage),headers=headers)
data = response.json()
print(data) 
totalCount = data['totalCount']
numPages = math.ceil(totalCount/1000)
print(numPages)
print(totalCount)
with open('{0}event1.json'.format(JSOnFolderPath), 'wb') as f:
    f.write(response.content)
f.close()

for page in range(2,numPages+1):
    print(page)
    response = requests.get(url + str(page),headers=headers)
    with open('{0}event{1}.json'.format(JSOnFolderPath,page), 'wb') as f:
        f.write(response.content)
    f.close()



 