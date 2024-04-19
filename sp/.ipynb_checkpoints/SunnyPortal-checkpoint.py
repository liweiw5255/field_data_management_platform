import os
import time
from datetime import date, timedelta, datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import pytz
import pdb

class SunnyPortal:
 
    startDate = None
    endDate = None
    
    inverterList = {'10249492':'28', '10249486':'29', '10249504':'30'}
    sensorList = {'10764334':'28','10764335':'29','10764336':'30','10764341':'ir'}
    sensorChannelList = ['Measurement.InOut.Tmp[0]', 'Measurement.InOut.Tmp[1]','Measurement.InOut.ValNom']
    inverterChannelList = ['Measurement.GridMs.TotW.Pv', 'Measurement.GridMs.W.phsA','Measurement.GridMs.W.phsB','Measurement.GridMs.W.phsC',
        'Measurement.GridMs.TotVAr','Measurement.GridMs.VAr.phsA','Measurement.GridMs.VAr.phsB','Measurement.GridMs.VAr.phsC',
        'Measurement.GridMs.TotVA', 'Measurement.GridMs.VA.phsA','Measurement.GridMs.VA.phsB','Measurement.GridMs.VA.phsC',
        'Measurement.GridMs.PhV.phsA','Measurement.GridMs.PhV.phsB','Measurement.GridMs.PhV.phsC',
        'Measurement.GridMs.A.phsA','Measurement.GridMs.A.phsB','Measurement.GridMs.A.phsC','Measurement.GridMs.Hz',
        'Measurement.DcMs.Watt[0]','Measurement.DcMs.Watt[1]','Measurement.DcMs.Vol[0]','Measurement.DcMs.Vol[1]',
        'Measurement.DcMs.Amp[0]','Measurement.DcMs.Amp[1]','Measurement.Isolation.LeakRis']
    
    AVGmultiAggregateList = ['Measurement.GridMs.PhV.phsA','Measurement.GridMs.PhV.phsB','Measurement.GridMs.PhV.phsC','Measurement.GridMs.Hz', 'Measurement.DcMs.Vol[0]', 'Measurement.DcMs.Vol[1]','Measurement.InOut.Tmp[0]','Measurement.InOut.Tmp[1]','Measurement.InOut.TotInsol']
    MINmultiAggregateList = ['Measurement.Isolation.LeakRis']
    
    solarIrradiance = ['10764341']
    solarIrradianceChannel = 'Measurement.InOut.TotInsol'
        
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

    def setStartDate(self, startDate):
        self.startDate = startDate
    
    def setEndDate(self, endDate):
        self.endDate = endDate

    def ny2utc(self, ny_time):
        return ny_time.astimezone(pytz.utc)

    def utc2ny(self, utc_time):
        return utc_time.astimezone(pytz.timezone('America/New_York'))

    
    def composePayloads(self, deviceID, begin_time, end_time):
        
        json_structure = {'queryItems': [],
                   'dateTimeBegin': begin_time,
                   'dateTimeEnd': end_time}
        
        if deviceID in self.inverterList:            
         
            for channel in self.inverterChannelList:
                if channel in self.AVGmultiAggregateList:
                    aggregate = 'Avg'
                elif channel in self.MINmultiAggregateList:
                    aggregate = 'Min'
                else:
                    aggregate = 'Sum'
                    
                json_structure['queryItems'].append(
                    {
                        'componentId': deviceID,
                        'channelId': channel,
                        'timezone': 'America/New_York',
                        'aggregate': 'Avg',
                        'multiAggregate': aggregate
                    },)
        
        elif deviceID in self.sensorList:              
            for channel in self.sensorChannelList:
                if channel in self.AVGmultiAggregateList:
                    aggregate = 'Avg'
                elif channel in self.MINmultiAggregateList:
                    aggregate = 'Min'
                else:
                    aggregate = 'Sum'
                    
                json_structure['queryItems'].append(
                    {
                        'componentId': deviceID,
                        'channelId': channel,
                        'timezone': 'America/New_York',
                        'aggregate': 'Avg',
                        'multiAggregate': aggregate
                    },)
        
            if deviceID in self.solarIrradiance:
                 json_structure['queryItems'].append(
                    {
                        'componentId': deviceID,
                        'channelId': self.solarIrradianceChannel,
                        'timezone': 'America/New_York',
                        'aggregate': 'Avg',
                        'multiAggregate': 'Sum'
                    },)
                
        
        else:
            print("Wrong device ID")
            return None
        
        return json_structure #json.dumps(json_structure, indent=4)
        
            
    def composeDataFrame(self, deviceID, currentDate, measurements):
        df = pd.DataFrame()
        time_list = pd.date_range(start=currentDate.replace(hour=0, minute=0),end=currentDate.replace(hour=23, minute=55), freq="5min", tz='US/Eastern')   
        index = 0
        if measurements is None:
            #print(currentDate.strftime('%Y-%m-%d'), " data not available")
            return
        else:
            for component in measurements:
                if(len(component['values'])>0):
                    new_df = pd.DataFrame()
                    current_index = 0
                   
                    for record in component['values']:
                        if(record['value'] is None):
                            record['value'] = -1   
                        # Convert UTC time to NY time
                        ny_time = self.utc2ny(datetime.strptime(record['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc))
                        ny_time_string = ny_time.strftime("%Y-%m-%d %H:%M:%S")
                         
                        # Fill the record before sunrise to -1    
                        if(ny_time > time_list[current_index]):
                            update_date = time_list[current_index]                     
                            while(update_date < ny_time):
                                if(index==0):
                                    tmp_df = pd.DataFrame({'time': update_date.strftime("%Y-%m-%d %H:%M:%S"),'value':[-1]})
                                else:
                                    tmp_df = pd.DataFrame({'value': [-1]})
                                new_df = pd.concat([new_df, tmp_df], axis=0)
                                current_index = current_index + 1
                                update_date = time_list[current_index]
                        
                        if(index==0):
                            tmp_df = pd.DataFrame({'time': ny_time_string,'value':[record['value']]})
                        else:
                            tmp_df = pd.DataFrame({'value':[record['value']]})
                        current_index = current_index + 1
                
                        new_df = pd.concat([new_df, tmp_df], axis=0)  
                
                  
                    while(current_index<time_list.shape[0]):
                        if(index == 0):
                             tmp_df = pd.DataFrame({'time': time_list[current_index].strftime("%Y-%m-%d %H:%M:%S"),'value':[-1]})
                        else:
                            tmp_df = pd.DataFrame({'value':[-1]})
                        new_df = pd.concat([new_df, tmp_df], axis=0)
                        current_index = current_index + 1
                    index = index + 1

                    df = pd.concat([df, new_df], axis=1, ignore_index=True)
                    
                else:
                    print(deviceID, component)
            
            if deviceID in self.inverterList: 
                deviceid = pd.concat([pd.DataFrame([self.inverterList[deviceID]])] * len(df))
                df = pd.concat([df, deviceid], axis=1, ignore_index=True)
                df.columns = ['time','ac_power','ac_power_l1','ac_power_l2','ac_power_l3', 'ac_reactive_power','ac_reactive_power_l1','ac_reactive_power_l2','ac_reactive_power_l3','ac_apparent_power','ac_apparent_power_l1','ac_apparent_power_l2','ac_apparent_power_l3','ac_voltage_l1','ac_voltage_l2','ac_voltage_l3','ac_current_l1','ac_current_l2','ac_current_l3','grid_frequency','dc_power_a','dc_power_b','dc_voltage_a','dc_voltage_b','dc_current_a','dc_current_b','iso', 'deviceID'] 
                
            elif deviceID in self.sensorList:
                if deviceID in self.solarIrradiance:
                    df.columns = ['time','ambient_temp1','ambient_temp2', 'ambient_rh', 'ir']
                else:
                    deviceid = pd.concat([pd.DataFrame([self.sensorList[deviceID]])] * len(df))
                    df = pd.concat([df, deviceid], axis=1, ignore_index=True)
                    df.columns = ['time','inv_temp1', 'inv_temp2', 'inv_rh', 'deviceID']
                    df = df[['inv_temp1', 'inv_temp2','inv_rh','deviceID']]
                    
            else:
                print("Wrong Device ID")
                return None


        return df


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

        begin_time = (self.ny2utc((currentDate-timedelta(days=1)).replace(hour=23, minute=55))).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_time = (self.ny2utc(currentDate.replace(hour=23, minute=55))).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        #end_time = (self.ny2utc(currentDate+timedelta(days=1))).strftime("%Y-%m-%dT%H:%M:%S.000Z")
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
        
        
        inverter_dfs = []
        sensor_dfs = []
        
        solar_info_payloads = self.composePayloads(self.solarIrradiance[0], begin_time, end_time)
        try:
            solar_measurements = json.loads(requests.post(self.info_url, headers=info_header, json=solar_info_payloads).text)
            solar_df = self.composeDataFrame(self.solarIrradiance[0], currentDate, solar_measurements)
        except Exception as e:
            solar_measurements = None
        
        for inverter, sensor in zip(self.inverterList, self.sensorList):
        
            inverter_info_payloads = self.composePayloads(inverter, begin_time, end_time)
            sensor_info_payloads = self.composePayloads(sensor, begin_time, end_time)
            
            try:
                inverter_measurements = json.loads(requests.post(self.info_url, headers=info_header, json=inverter_info_payloads).text)
            except Exception as e:
                inverter_measurements = None
           
            try:
                sensor_measurements = json.loads(requests.post(self.info_url, headers=info_header, json=sensor_info_payloads).text)
            except Exception as e:
                sensor_measurements = None
                
            inverter_df = self.composeDataFrame(inverter, currentDate, inverter_measurements)
            inverter_dfs.append(inverter_df)
            
            sensor_df = self.composeDataFrame(sensor, currentDate, sensor_measurements)     
            sensor_df = pd.concat([solar_df, sensor_df], axis=1)
        
            sensor_dfs.append(sensor_df)
            
        op_filename = "operating/sp_" + currentDate.strftime("%Y-%m-%d")
        env_filename = "environmental/sp_" + currentDate.strftime("%Y-%m-%d")
        
        try:
            final_inverter_df = pd.concat(inverter_dfs)
            final_inverter_df.to_csv(self.path + op_filename +".csv", index=False)
            print(op_filename," has been saved!")
        except Exception as e:
                print(op_filename,"save Failed: ",e)
    
        try:
            final_sensor_df = pd.concat(sensor_dfs)
            final_sensor_df.to_csv(self.path + env_filename +".csv", index=False)
            print(env_filename," has been saved!")
        except Exception as e:
                print(env_filename,"save Failed: ",e)
        

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
            if(os.path.isfile(self.path+"operating/"+filename+".csv") and os.path.isfile(self.path+"environmental/"+filename+".csv") ):
                print(filename, " existed!")
            else:
                try:
                    self.requestInfo(currentDate)
                except Exception as e:
                    print(currentDate.strftime("%Y-%m-%d"), " Request Information Error: ",e)
                    flag = 0
                    # Open the file in append mode
                    with open('exception/sp.txt', 'r') as f:
                        for line in f:
                        # Check if the string exists in the line
                            if filename in line.strip():
                                # Write the new line
                                flag = 1
                               
                     
                    if(flag == 0):
                        with open('exception/sp.txt', 'a') as f:
                            f.write(filename + "\n")
                    
                    currentDate = currentDate + timedelta(days=1)
                    continue
                

            currentDate = currentDate + timedelta(days=1)

        












