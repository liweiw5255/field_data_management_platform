'''
    conda install selenium
    conda install pandas
    conda install bs4
    conda install requests
'''

import AlsoEnergy as ae

# Path
driverPath = "/home/liweiw/TRACE_PV/data_process/new_driver/chromedriver"
chromePath = "/home/liweiw/TRACE_PV/data_process/new_driver/chrome/chrome"

# Confidential Information
ae_username = 'tracepv2022@outlook.com'
ae_password = 'tracePV123!!!!!!'

# Url
ae_url = 'https://apps.alsoenergy.com/'

ae_path = "/home/liweiw/TRACE_PV/data_process/new_driver/ae/ae_data/"
ae_object = ae.AlsoEnergy(ae_path, driverPath, chromePath, ae_url)
ae_object.setUserName(ae_username)
ae_object.setPassword(ae_password)
ae_object.setStartDate(datetime(2023,1,1))
ae_object.setEndDate(datetime(2023,1,2))
ae_object.AlsoEnergy()


