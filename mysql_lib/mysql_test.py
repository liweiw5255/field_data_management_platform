# 1. create conda environment
#     conda create -n dataprocess
# 2. activate the environemnt
#     source activate dataprocess
# 3. install eseential packages
#     conda install -c anaconda mysql-connector-python
#     conda install pandas


import mysql.connector as mysql
import mySQLConnect as mysqlconnect

mysql_host = 'hpcese1.computing.clemson.edu'
mysql_username = 'tracepv'
mysql_password = 'tracePV123...'
mysql_database = 'tracepv'

mysql_connect_object = mysqlconnect.mySQLConnect(mysql_username, mysql_password, mysql_database, mysql_host)
mysql_connect_object.connect()
mysql_connect_object.query("SELECT SunnyPortal.deviceID FROM SunnyPortal WHERE SunnyPortal.time BETWEEN '2022-07-22 23:47:00' AND '2022-07-22 23:48:00' ")
dataframe = 1
#mysql_connect_object.insert(dataframe)



'''

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
from sqlalchemy import create_engine
import csv
from datetime import date

# mysql login information
host = 'hpcese1.computing.clemson.edu'
username = 'tracepv'
password = 'tracePV123...'
database = 'tracepv'

def mysql_query(date_start, date_end):
     
    cnx = mysql.connector.connect(user=username, password=password, database=database, host=host)
    cursor = cnx.cursor()
    query =  ("SELECT SunnyPortal.time, SunnyPortal.ac_power, SunnyPortal.ac_voltage_l1, SunnyPortal.ac_voltage_l2, SunnyPortal.ac_voltage_l3, AlsoEnergy.GHI, AlsoEnergy.POA, SunnyPortal.ambient_temp, ambient_rh FROM SunnyPortal, AlsoEnergy WHERE SunnyPortal.time = AlsoEnergy.Time AND SunnyPortal.deviceId=30 AND SunnyPortal.time BETWEEN %s AND %s")
    date_start = date_start.isoformat()
    date_end = date_end.isoformat()
    cursor.execute(query, (date_start,date_end))
    myresult=cursor.fetchall()
    with open("sql_result.csv", "w") as f:
        write = csv.writer(f)
        write.writerows(myresult)

    cursor.close()
    cnx.close()
    

def mysql_insert(df, table):

    try:
        
        # configure mysql connector
        # config = {
        #    'host':host,
        #    'user':username,
        #    'password':password,
        #    'database':database,
        #    'raise_on_warnings': True
        #}
        
        # cnx = mysql.connector.connect(**config)
        
        engine = create_engine("mysql+mysqldb://"+username+":"+password+"@"+host+"/"+database) 

    
        df.to_sql(table,con=engine, if_exists='append',index=False)

        return True

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        return False
'''                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                 
                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                                                                                                                                                                                                                                                                                                                                                                                                                                                      
