'''
    conda install selenium
    conda install pandas
    conda install bs4
    conda install requests
'''

import AlsoEnergy as ae
from datetime import datetime, timedelta
import os
#import sys
#sys.path.append('../')
#from path import path
path = os.getcwd() + '/' # os.path.dirname(os.getcwd()) + '/'

# Path
#driverPath = path + "chromedriver-linux64"
#chromePath = path + "chrome-linux64"
driverPath = path + "chromedriver_palmetto"
chromePath = path + "chrome_palmetto/chrome"


# Confidential Information
ae_username = 'tracepv2022@outlook.com'
ae_password = 'Expert2025!!!!!!'

ae_path = path + "ae/ae_data/"
print(path, ae_path, driverPath, chromePath)

ae_object = ae.AlsoEnergy(ae_path, driverPath, chromePath)
ae_object.setUserName(ae_username)
ae_object.setPassword(ae_password)
ae_object.setStartDate(datetime(2025,7,23))
ae_object.setEndDate(datetime(2025,12,31))
#ae_object.setEndDate(datetime.today() - timedelta(days=1))
ae_object.AlsoEnergy()

