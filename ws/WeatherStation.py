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
    
    def __init__(self, path, apiKey, startDate, endDate):
        self.path = path
        self.apiKey = apiKey
        self.startDate = startDate
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
        data_df = pd.DataFrame(columns=['timestamp', 'temp', 'dewPt', 'rh', 'wx_phrase'])
        data = json.loads(response.text)
        for v in data['observations']:
            new_row = pd.DataFrame({'timestamp': [datetime.fromtimestamp(v['expire_time_gmt'])], 'temp': [v['temp']], 'dewPt':[v['dewPt']], 'rh':[v['rh']], 'wx_phrase':[v['wx_phrase']]})
            data_df = pd.concat([data_df, new_row], ignore_index=True)
        
        filename = "ws_"+currentDate.strftime('%Y-%m-%d')
        try:
            data_df.to_csv(self.path+filename+".csv",index=False)         
            print(filename,"has been saved!")
        except Exception as e:
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