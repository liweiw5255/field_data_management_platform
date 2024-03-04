import SunnyPortal as sp
import sys
sys.path.append('../')
from path import path

sp_username = "liweiw@g.clemson.edu"
sp_password = "tracePV123..."
sp_path = path + "sp/sp_data/"
sp_object = sp.SunnyPortal(sp_path)
sp_object.setUserName(sp_username)
sp_object.setPassword(sp_password)
sp_object.setStartDate(2023,9,12)
sp_object.setEndDate(2024,3, 3)
sp_object.SunnyPortal()
