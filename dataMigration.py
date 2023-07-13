import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import json
import requests
import os
import datetime
import time
import statistics
import math
import sys
import itertools
import traceback
import inspect
import sys

import platform
version = platform.python_version().split(".")[0]
if version == "3":
    import app_config.app_config as cfg
elif version == "2":
    import app_config as cfg
config = cfg.getconfig()



def tr():
    print(traceback.format_exc())

prodmeta = 'http://13.251.5.125/exactapi'
prodKairos  = 'http://13.68.199.3/kairosapi/api/v1/datapoints/query'
try:
    unitsId = sys.argv[1]
except:
    print("pass unitsId")
    exit()

# print(unitsId)
class dataM:
    def getResponseBody(self,response,word="",printa=False):
        try:
            if(response.status_code==200):
                if printa:
                    print(f"Got {word} successfully.....")

                body = json.loads(response.content)
                if type(body) != list:
                    body = [body]
                
            else:
                body =[]
                print(f"Did not get{word} successfully.....")
                print(response.status_code)
                # print(response.content)
            return body
        except:
            print(traceback.format_exc())

    def getTagmeta(self,unitsId):
        try:
            query = {"unitsId":unitsId}
            urlQuery = prodmeta + '/tagmeta?filter={"where":' + json.dumps(query) + ',"fields":["dataTagId"]}'
            response = requests.get(urlQuery)
            word = "tagmeta"
            body = self.getResponseBody(response,word,1)
            return body
        except:
            tr()

    def getForms(self,unitsId):
        try:
            urlQuery = prodmeta + "/units/" + unitsId + "/forms"
            print(urlQuery)
            response = requests.get(urlQuery)
            word = "forms body"
            body = self.getResponseBody(response,word,1)
            return body
        except:
            tr()


    def getValuesV2(self,tagList,startTime, endTime):
        try:
            url = prodKairos

            metrics = []
            for tag in tagList:
                tagDict = {
                    "tags":{},
                    "name":tag
                }
                metrics.append(tagDict)
                
            query ={
                "metrics":metrics,
                "plugins": [],
                "cache_time": 0,
                "start_absolute": startTime,
                "end_absolute": endTime
                
            }
        #     print(json.dumps(query,indent=4))
            res=requests.post(url=url, json=query)
            values=json.loads(res.content)
            finalDF = pd.DataFrame()
            for i in values["queries"]:
                df = pd.DataFrame(i["results"][0]["values"],columns=["time",i["results"][0]["name"]])

                try:
                    finalDF = pd.concat([finalDF,df.set_index("time")],axis=1)
                except Exception as e:
                    print(e)
                    finalDF = pd.concat([finalDF,df],axis=1)
                
            # finalDF.dropna(inplace=True)
            # finalDF.interpolate(inplace=True,limit_direction="both")

            finalDF.reset_index(inplace=True)

            # print(dates)
            return finalDF
        except Exception as e:
            print(traceback.format_exc())
            return pd.DataFrame()
    

    def postOnKairos(self,df,dataTagId):
        try:
            print("*"*30,str(dataTagId),"*"*30)
            postUrl = config["api"]["datapoints"]
            reqDataPoints = 20000
            print(f"len of df {len(df)}")
            for i in range(0,len(df),reqDataPoints):
                print(i)
                new_df =  df[["time",dataTagId]][i:i+reqDataPoints]
                new_df.dropna(inplace=True,axis=0)

                if len(new_df) > 0:

                    postArray = new_df[["time",dataTagId]].values.tolist()
                    print(f"len of post array {len(postArray)}")
                    postBody = [{
                        "name":dataTagId,
                        "datapoints":postArray,
                        "tags":{"type":"derived"}
                    }]

                    res = requests.post(postUrl,json=postBody)
                
                    if res.status_code == 200 or res.status_code == 204:
                        print("posted on kairos successfully")
                    else:
                        print(res.status_code)
                        print(res.content)
        except:
            print(traceback.format_exc())


    def createBodyForForms(self,formBody):
        try:
            returnLst = []
            for i in formBody:
                if "fields" in i.keys():
                    for feild in i["fields"]:
                        body = {}
                        body["dataTagId"] = feild["dataTagId"]
                        returnLst.append(body)
            # print(returnLst)
            return returnLst
        except:
            tr()


    def mainFunc(self): 
        try:
            count = 0
            tagmetaBody = self.getTagmeta(unitsId)

            formBody = self.getForms(unitsId)
            formBody = self.createBodyForForms(formBody)
            tagmetaBody += formBody

            for tag in tagmetaBody:
                dataTagId = tag["dataTagId"]
                print("*"*30,str(dataTagId),"*"*30)
                print(f"count {count}")
                et = time.time() * 1000
                st = et - 1*1000*60*60*24*365*10
                df = self.getValuesV2([dataTagId],st,et)
                self.postOnKairos(df,dataTagId)
                count += 1
                
        except:
            tr()

dataM().mainFunc()