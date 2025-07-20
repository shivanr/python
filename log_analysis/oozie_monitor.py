import os,sys,re
import urllib.request
import requests
from  requests import codes
from bs4 import BeautifulSoup
import json
from requests.auth import HTTPBasicAuth
import urllib3
from datetime import datetime,timedelta,date
import pandas as pd
import subprocess

url = "https://your_host/oozie/v1/jobs?len=5&filter=status%3DKILLED&order=desc"
url2 = "https://your_host:11443/oozie/v2/job/"
urllib3.disable_warnings()
http = urllib3.PoolManager()

res = "curl --negotiate -u user_id:'91' {0}".format(url)
out = subprocess.check_output(res ,shell = True)
out1 = out.decode('utf-8')
#out3 = re.sub("(\\[{)","{",out1)
#out4 = re.sub("(}\\])","}",out3)
out2 = json.loads(out1)
#print(out2)
out3 = out2['workflows']
#print(type(out5))
#resp = re.sub("([{)"|"(}])","",out3)
#print(out3)
listn = []
#list1 = []

for i in out3:
    #print(i)
    list1 = []
    appName = i['appName']
    wid = i['id']
    status = i['status']
    user = i['user']
    startTime = i['startTime']
    endTime = i['endTime']
    list1.append(appName)
    list1.append(wid)
    list1.append(status)
    list1.append(user)
    list1.append(startTime)
    list1.append(endTime)
    listn.append(list1)
#print(listn)
headers = ['AppName','Workflow_id','Status','User','startTime','endTime']
oozie_pd = pd.DataFrame(listn,columns = headers,index=None)
#print(oozie_pd)

wf_id = oozie_pd['Workflow_id']#.to_string(index=False)
print(type(wf_id))

for i in wf_id:
    oozie_url = url2 + i
    print(oozie_url)
