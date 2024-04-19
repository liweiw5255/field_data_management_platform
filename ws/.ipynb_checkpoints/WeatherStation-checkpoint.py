from bs4 import BeautifulSoup
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import os
import pytz

severe_weather = ['Haze', 'Thunder', 'Storm', 'Heavy', 'Drizzle', 'T-storm'] # 10 points
mild_weather = ['Cloudy', 'Rain', 'Fog', 'Smoke', 'Mist'] # 5 point

class WeatherStation:

    startDate = None
    endDate = None
    apiKey =  None
    path = None
    
    def __init__(self, path, apiKey):
        self.path = path
        self.apiKey = apiKey
    
    def setStartDate(self, startDate):
        self.startDate = startDate
        
    def setEndDate(self, endDate):
        self.endDate = endDate
        
    def getAPIKey(self):
        return self.apiKey

    def getStartDate(self):
        return self.startDate

    def getEndDate(self):
        return self.endDate
    
    def requestInfo(self, currentDate):
        
        current_string = currentDate.strftime('%Y%m%d')
        url = "https://api.weather.com/v1/location/KCAE:9:US/observations/historical.json?apiKey="+self.apiKey+"&units=m&startDate="+current_string
        response = requests.get(url)
        if(response.status_code==200):
            print(current_string,"Request Succeed!")
        else:
            print(current_string,"Request Failed!")
   
        # load data
        data_df = pd.DataFrame(columns=['time', 'ambient_temperature', 'relative_humidity', 'weather_condition'])
        data = json.loads(response.text)
        for v in data['observations']:
            # Create a datetime object from the Unix epoch time
            #gmt_time = datetime.fromtimestamp(v['expire_time_gmt'], pytz.timezone('GMT'))
            # Convert GMT to New York time
            #ny_time = gmt_time.astimezone(pytz.timezone('America/New_York'))
            
            new_row = pd.DataFrame({'time': [datetime.fromtimestamp(v['valid_time_gmt'])], 'ambient_temperature': [v['temp']], 'relative_humidity':[v['rh']], 'weather_condition':[v['wx_phrase']]})
            data_df = pd.concat([data_df, new_row], ignore_index=True)
        
        filename = "ws_"+currentDate.strftime('%Y-%m-%d')
        try:
            data_df.to_csv(self.path+filename+".csv",index=False)         
            print(filename,"has been saved!")
        except Exception as e:
            # Open the file in append mode
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
            print(filename," save Failed: ",e)

    def WeatherStation(self):
        
        if(self.startDate is None):
            self.startDate = datetime.now() - timedelta(days=1)
        
        currentDate = self.startDate

        if(self.endDate is None):
            self.endDate = datetime.now() - timedelta(days=1)

        while (currentDate <= self.endDate):
            filename= "ws_" + currentDate.strftime("%Y-%m-%d")
            if(os.path.isfile(self.path+filename+".csv")):
                print(filename, " existed!")
            else:
                try:
                    self.requestInfo(currentDate)
                except Exception as e:
                    print("Error: ",e)  
                    
            currentDate = currentDate + timedelta(days=1)