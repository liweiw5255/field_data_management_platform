import SunnyPortal as sp
from datetime import datetime, timedelta
import sys
import os

path = os.getcwd() + '/' #os.path.dirname(os.getcwd()) + '/'
print(path)

# Pat
driverPath = path + "chromedriver_palmetto/chromedriver"
chromePath = path + "chrome_palmetto/chrome"
sp_path = path + "sp/sp_data/"

sp_username = "liweiw@g.clemson.edu"
sp_password = "tracePV123..."
sp_object = sp.SunnyPortal(sp_path, chromePath, driverPath)
sp_object.setUserName(sp_username)
sp_object.setPassword(sp_password)
sp_object.setStartDate(datetime(2024,1,1))
sp_object.setEndDate(datetime(2025,12,31))
#sp_object.setEndDate(datetime.today() - timedelta(days=1))
sp_object.SunnyPortal()


# 5-10
# 5-24
# 6-08
# 7-01
