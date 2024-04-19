'''
    conda install selenium
    conda install pandas
    conda install bs4
    conda install requests
'''

import AlsoEnergy as ae
from datetime import datetime
import sys
sys.path.append('../')
from path import path


# Path
driverPath = path + "chromedriver"
chromePath = path + "chrome/chrome"

# Confidential Information
ae_username = 'tracepv2022@outlook.com'
ae_password = 'tracePV123!!!!!!'

ae_path = path + "ae/ae_data/"
ae_object = ae.AlsoEnergy(ae_path, driverPath, chromePath)
ae_object.setUserName(ae_username)
ae_object.setPassword(ae_password)
ae_object.setStartDate(datetime(2023,1,1))
ae_object.setEndDate(datetime(2023,1,2))
ae_object.AlsoEnergy()


