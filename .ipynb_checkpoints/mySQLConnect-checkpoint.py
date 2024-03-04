import mysql.connector as mysql
import logging

class mySQLConnect:

    def __init__(self, username, password, database, host):
        self.username = username  
        self.password = password
        self.database = database
        self.host = host
        self.connection = None
        
    #def getUserName(self):
    #    return self.username 

    #def getPassword(self):
    #    return self.password 

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

    def insert(self):
        pass

    def delete(self):
        pass

    def query(self,field_list, table_list, condition):
        
        try:
            # TODO: verify data format
            if(len(field_list)==0):
                print("Invalid field list")
            else:
                cursor = self.connection.cursor()
                query_str = "SELECT "
                
                # concate query fields
                for field in field_list:
                    query_str = query_str + field + " "
                # concate table 
                query_str = query_str + "FROM "
                for table in table_list:
                    query_str = query_str + table + ","
                query_str = query_str[:-1] + " "
                
                if(condition is not None):
                    # concate condition
                    query_str = query_str + "WHERE " + condition + "#;"
                    
                print(query_str)

                cursor.execute(query_str)
                myresult=cursor.fetchall()
                print(myresult)
                
        except  Exception as e:
            logging.error('Query failed: ' + str(e))