# Gather Weather Information
import os
import datetime
import pandas as pd
import WeatherStation as ws
import numpy as np
import sys

sys.path.append('../')
from path import path

ws_path = path + "ws/ws_data/"
apiKey = "e1f10a1e78da46f5b10a1e78da96f525"
#path = "C:\\Users\\lwwan\\Desktop\\Scripts\\ws_data\\"

startDate = datetime.datetime(2022,1,1)
#endDate = startDate
endDate = datetime.datetime.now()
ws_object = ws.WeatherStation(ws_path, apiKey)
ws_object.setStartDate(startDate)
ws_object.setEndDate(endDate)
ws_object.WeatherStation()



dic = dict()
'''
Weather Condition Table
1. List all the dictionary keys here
2. Assign serial number for each key

Weather Condition Dictionary
1. If the key exists, append the timestamp
2. If the key does not exist, assign the first timestamp 


Weather Condition Classification:

    Severe Weather: Haze, Thunder, Storm, Heavy, Drizzle (10)
    Mild Weather: Cloudy, Rain, Fog, Smoke, Mist (1)
    Good Weather and other: Windy, Fair, Other condition, nan (0)


'''
severe_weather = ['Haze', 'Thunder', 'Storm', 'Heavy', 'Drizzlei'];
mild_weather = ['Cloudy', 'Rain', 'Fog', 'Smoke', 'Mist'];

'''
index = 1
for file_path in os.listdir(ws_path):
    # Read the CSV file
    df = pd.read_csv(ws_path+file_path)
    # Retreive the timestamp and dictionaries
    for record in df.itertuples():
        dt = datetime.datetime.strptime(record.timestamp,"%Y-%m-%d %H:%M:%S")
        curr_date = dt.strftime("%Y-%m-%d")
        curr_time = dt.strftime("%H:%M:%S")
        if(record.wx_phrase not in list(dic.keys())):
            dic[record.wx_phrase] = index
            index = index + 1
        if(dic[record.wx_phrase]==36):
            print(record)

#print(dic.keys())

for key in dic.keys():
    print(key,":")
    points = 0
    for k in str(key).split("/"):
        for kk in str(k).split():
            if(kk in severe_weather):
                points = points + 10
            elif(kk in mild_weather):
                points = points + 5

    print(points)

full_df = pd.DataFrame()

for file_path in os.listdir(ws_path):
    # Read the CSV file
    df = pd.read_csv(ws_path+file_path)
    df.wx_phrase = [dic[record.wx_phrase] for record in df.itertuples() ]
    full_df = pd.concat([full_df, df])

print(full_df.tail(24))
'''
