# conda install sqlalchemy
# conda install -c anaconda mysql-connector-python
# pip install mysqlclient
import mysql.connector as mysql
from sqlalchemy import create_engine
import logging

class mySQLConnect:

    def __init__(self, username, password, database, host):
        self.username = username  
        self.password = password
        self.database = database
        self.host = host
        self.connection = None
        
    def getDatabase(self):
        return self.database 

    def getHost(self):
        return self.host 

    def connect(self):
        
        try:
            connection = mysql.connect(
                user = self.username,
                password = self.password,
                database = self.database,
                host = self.host
            )
            self.connection = connection
            print("Connection succeed!")
        except  Exception as e:
            logging.error('Connection failed: ' + str(e))

    def insert(self, df, table):
    
        flag = 0
        print("Insert", table)
        
        try:
            engine = create_engine('mysql+mysqlconnector://'+self.username+':'+self.password+'@'+self.host+'/'+self.database)
            df.to_sql(table,con=engine, if_exists='append',index=False)
            flag = 1
            print("Insert Succeed!")

        except mysql.Error as err:
            print("Insert Error!")
            flag = -1
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        finally:
            return flag


    #def delete(self):
    #    pass

    def query(self,query):
        
        try:
            # TODO: verify data format
            cursor = self.connection.cursor()              
            if(query is not None):
                cursor.execute(query)
                myresult=cursor.fetchall()
                print(myresult)
                
        except  Exception as e:
            logging.error('Query failed: ' + str(e))