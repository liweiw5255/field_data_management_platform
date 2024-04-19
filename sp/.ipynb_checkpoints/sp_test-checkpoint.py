import SunnyPortal as sp
from datetime import datetime
import sys
sys.path.append('../')
from path import path

sp_username = "liweiw@g.clemson.edu"
sp_password = "tracePV123..."
sp_path = path + "sp/sp_data/"
sp_object = sp.SunnyPortal(sp_path)
sp_object.setUserName(sp_username)
sp_object.setPassword(sp_password)
sp_object.setStartDate(datetime(2023,11,5))
sp_object.setEndDate(datetime(2023,11,5))
sp_object.SunnyPortal()
