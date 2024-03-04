import os
import time
from datetime import date, timedelta, datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import pytz

class SunnyPortal:
 
    startDate = None
    endDate = None

    deviceList = {
        '10764334' : 'SensorBox 29', # 1. Tmp[0], 2. Tmp[1], 3. ValNom for Inverter 29
        '10764335' : 'SensorBox 30', # 1. Tmp[0], 2. Tmp[1], 3. ValNom for Inverter 30
        '10764336' : 'SensorBox 31',  # 1. Tmp[0], 2. Tmp[1], 3. ValNom for Inverter 31
        '10764341' : 'Solar Irradiance SensorBox', # 1. TotInsol, 2. Tmp[0], 3. ValNom
 
    }
    channelList = {
        'Measurement.InOut.TotInsol' : 'Solar Irradiance',
        'Measurement.InOut.Tmp[0]' : 'Temperature_1',
        'Measurement.InOut.Tmp[1]' : 'Temperature_2',
        'Measurement.InOut.ValNom' : 'Relative Humidity'
    }
    login_url = "https://auth.sunnyportal.com/auth/realms/SMA/protocol/openid-connect/token"
    login_header =  {
        'Accept' : 'application/json, text/plain, */*',
        'Accept-Encoding' : 'gzip, deflate, br',
        'Accept-Language' : 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Connection' : 'keep-alive',
        'Content-Type' : 'application/x-www-form-urlencoded;charset=UTF-8',
        'Host' : 'auth.sunnyportal.com',
        'Origin' : 'https://ennexos.sunnyportal.com',
        'Referer' : 'https://ennexos.sunnyportal.com/',
        'Sec-Fetch-Dest' : 'empty',
        'Sec-Fetch-Mode' : 'cors',
        'Sec-Fetch-Site' : 'same-site',
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'sec-ch-ua-platform' : 'Windows'
    }

    info_url = "https://uiapi.sunnyportal.com/api/v1/measurements/search"
    

    access_token = ''

    # private properties
    __username = ''
    __password = ''

    def __init__(self, path):
        self.path = path

    def setUserName(self, username):
        self.__username = username

    def setPassword(self, password):
        self.__password = password

    def setStartDate(self, start_year, start_month, start_day):
        self.startDate = datetime(start_year, start_month, start_day, 0, 0, 0)

    def setEndDate(self, end_year, end_month, end_day):
        self.endDate = datetime(end_year, end_month, end_day, 0, 0, 0)

    def ny2utc(self, ny_time):
        return ny_time.astimezone(pytz.utc)

    def utc2ny(self, utc_time):
        return utc_time.astimezone(pytz.timezone('America/New_York'))

    def loginWebsite(self):

        login_payloads = {
            'grant_type': 'password',
            'username': self.__username,
            'password': self.__password,
            'client_id' : 'SPpbeOS'
        }

        response = requests.post(self.login_url, headers=self.login_header, data=login_payloads)
        if(response.status_code==200):
            print("Login Succeed")
        else:
            print("Login Failed")
            return -1

        self.access_token = json.loads(response.text)['access_token']
    
    def requestInfo(self, currentDate):

        begin_time = (self.ny2utc(currentDate)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_time = (self.ny2utc(currentDate+timedelta(days=1))).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        #begin_time = currentDate.strftime("%Y-%m-%dT00:00:00.000Z")
        #end_time = (currentDate+timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000Z")

        info_header = {
            'Accept' : 'application/json, text/plain, */*',
            'Accept-Encoding' : 'gzip, deflate, br',
            'Accept-Language' : 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection' : 'keep-alive',
            'Content-Type' : 'application/json',
            'Authorization' : 'Bearer '+ self.access_token,
            'Host' : 'uiapi.sunnyportal.com',
            'Origin' : 'https://ennexos.sunnyportal.com',
            'Referer' : 'https://ennexos.sunnyportal.com/',
            'Sec-Fetch-Dest' : 'empty',
            'Sec-Fetch-Mode' : 'cors',
            'Sec-Fetch-Site' : 'same-site',
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'sec-ch-ua-platform' : 'Windows'
        }

        info_payloads = {
            'queryItems': [
                {
                    'componentId': '10764334',
                    'channelId': 'Measurement.InOut.ValNom',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Sum'
                },
                {
                    'componentId': '10764335',
                    'channelId': 'Measurement.InOut.ValNom',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Sum'
                },
                {
                    'componentId': '10764336',
                    'channelId': 'Measurement.InOut.ValNom',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Sum'
                },
                {
                    'componentId': '10764341',
                    'channelId': 'Measurement.InOut.ValNom',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Sum'
                },
                {
                    'componentId': '10764334',
                    'channelId': 'Measurement.InOut.Tmp[0]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764335',
                    'channelId': 'Measurement.InOut.Tmp[0]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764336',
                    'channelId': 'Measurement.InOut.Tmp[0]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764341',
                    'channelId': 'Measurement.InOut.Tmp[0]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764341',
                    'channelId': 'Measurement.InOut.TotInsol',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764334',
                    'channelId': 'Measurement.InOut.Tmp[1]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764335',
                    'channelId': 'Measurement.InOut.Tmp[1]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764336',
                    'channelId': 'Measurement.InOut.Tmp[1]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                },
                {
                    'componentId': '10764341',
                    'channelId': 'Measurement.InOut.Tmp[1]',
                    'timezone': 'America/New_York',
                    'aggregate': 'Avg',
                    'multiAggregate': 'Avg'
                }
            ],
            'dateTimeBegin': begin_time,
            'dateTimeEnd': end_time
        }
        
        try:
            measurements = json.loads(requests.post(self.info_url, headers=info_header, json=info_payloads).text)
        except Exception as e:
            measurements = None            
        
        #df = pd.DataFrame(columns=['time', 'inv29_rh', 'inv30_rh', 'inv31_rh', 'rh', 'inv29_temp1', 'inv30_temp1', 'inv31_temp1', 'temp1', 'inv29_temp2', 'inv30_temp2', 'inv31_temp2', 'temp2', 'ir']) 
        #df = pd.DataFrame(columns=['time','device','channel', 'value'])
        df = pd.DataFrame()
        index = 0
        if measurements is None:
            print(currentDate.strftime('%Y-%m-%d'), " data not available")
            return
        else:
            for component in measurements:
                if(len(component['values'])>0):
                    #print(component['componentId'], component['channelId'])
                    new_df = pd.DataFrame()
                    for record in component['values']:
                        #if(component['componentId'] == '10764341' and component['channelId'] == 'Measurement.InOut.Tmp[1]'):
                        #    print(record['value'])
                        if(record['value'] is None):
                            record['value'] = -1    
                        if(index==0):
                            ny_time = self.utc2ny(datetime.strptime(record['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc))
                            #tmp_df = pd.DataFrame({'time':[record['time'][0:10]+" "+record['time'][11:16]],'value':[record['value']]})
                            tmp_df = pd.DataFrame({'time': ny_time.strftime("%Y-%m-%d %H:%M:%S"),'value':[record['value']]}) 
                        else:
                            tmp_df = pd.DataFrame({'value':[record['value']]})
                        new_df = pd.concat([new_df, tmp_df], axis=0)    
                        #new_df = pd.DataFrame({'time':[record['time']],'device':[component['componentId']] , 'channel':[component['channelId']], 'value':[record['value']]})
                        #new_df = pd.DataFrame({'time':[record['time']],'device':[self.deviceList[component['componentId']]] , 'channel':[self.channelList[component['channelId']]], 'value':[record['value']]})
                    index = index + 1
                    #print(component['componentId'], component['channelId'], len(new_df))
                    df = pd.concat([df, new_df], axis=1, ignore_index=True)
            df.columns = ['time', 'inv29_rh', 'inv30_rh', 'inv31_rh', 'rh', 'inv29_temp1', 'inv30_temp1', 'inv31_temp1', 'temp1', 'inv29_temp2', 'inv30_temp2', 'inv31_temp2', 'temp2', 'ir']
            
            filename = "sp_" + currentDate.strftime("%Y-%m-%d")
            try:
                df.to_csv(self.path + filename +".csv", index=False)
                print(filename," has been saved!")
            except Exception as e:
                    print(filename," save Failed: ",e)

    def SunnyPortal(self):

        try:
            self.loginWebsite()
            print("Crediential verified")
        except Exception as e:
            print("Login Error: ", e)


        if(self.startDate is None):
            self.startDate = datetime.now() - timedelta(days=1)
        
        currentDate = self.startDate

        if(self.endDate is None):
            self.endDate = datetime.now() - timedelta(days=1)

        while (currentDate <= self.endDate):
            filename= "sp_" + currentDate.strftime("%Y-%m-%d")
            if(os.path.isfile(self.path+filename+".csv")):
                print(filename, " existed!")
            else:
                try:
                    self.requestInfo(currentDate)
                except Exception as e:
                    print(currentDate.strftime("%Y-%m-%d"), " Request Information Error: ",e)
                    currentDate = currentDate + timedelta(days=1)
                    continue
                

            currentDate = currentDate + timedelta(days=1)

        












