import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import date
from datetime import timedelta
import pandas as pd
import logging


class AlsoEnergy:

    # properties
    path = ''
    filename = 'chart-data.csv'
    url = "https://apps.alsoenergy.com/"
    driverPath = ''
    startDate = date(date.today().year, date.today().month, date.today().day) - timedelta(days=1)
    endDate = date(date.today().year, date.today().month, date.today().day) - timedelta(days=1)
    timeout = 20
    alsoNameList = ["Time","GHI","POA","ambient_temp","module_temp"]
    # private properties
    __username = ''
    __password = ''

    def __init__(self, path, driverPath, chromePath):
        self.path = path
        self.driverPath = driverPath
        self.chromePath = chromePath
    
    # credential information get method
    def setUserName(self, username):
        self.__username = username
    
    def setPassword(self, password):
        self.__password = password
    
    def setStartDate(self, start_date):
        self.startDate = start_date
        
    def setEndDate(self, end_date):
        self.endDate = end_date
        
    def enable_download_headless(self,driver):
        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd':'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': self.path}}
        driver.execute("send_command", params)

    def initChromeDriver(self):
        option = webdriver.ChromeOptions()
        option.binary_location = self.chromePath
        option.add_argument("--headless")
        option.add_argument('--no-sandbox')
        option.add_argument('--verbose')
        option.add_argument('--disable-gpu')
        option.add_argument('--disable-software-rasterizer')
        option.add_argument('--disable-logging')
        print(self.driverPath)
        driver = webdriver.Chrome(executable_path=self.driverPath, options=option)
        #driver = webdriver.Chrome(options=option)
        self.enable_download_headless(driver)

        return driver

    def loginWebsite(self, driver):
    
        driver.get(self.url)

        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.ID, "username")))
        element.clear()
        element.send_keys(self.__username)
    
        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/main/section/div/div/div/div/div/form/div[2]/button")))
        element.click()

        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.ID, "password")))
        element.clear()
        element.send_keys(self.__password)

        #element = WebDriverWait(driver, self.timeout).until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/main/section/div/div/div/form/div[3]/button")))
        # XPath for login buttion has been updated
        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/main/section/div/div/div/form/div[2]/button")))
                
        element.click()
        print("Also Energy login succeed!")

    def AlsoEnergy(self):
        # initialize web driver
        driver = self.initChromeDriver()
        # login to the website
        self.loginWebsite(driver)

        while self.startDate <= self.endDate:
            yesterday = self.startDate.strftime("%Y-%m-%d")
            dst_filename = self.path+'ae_'+yesterday+'.csv'
            self.startDate = self.startDate + timedelta(days=1)
            if(os.path.isfile(dst_filename)):
                print("ae_"+ yesterday + " file already existed!")
                continue
            try:
                #url = 'https://apps.alsoenergy.com/powertrack/S40225/analysis/chartbuilder?start='+yesterday+'&end='+yesterday+'&d=day&bin=1&k=%7B~measurements~%3A%5B4%2C8%5D%7D&m=k&a=0&h=5&c=259&s=1'
                url = 'https://apps.alsoenergy.com/powertrack/S40225/analysis/chartbuilder?start='+yesterday+'&end='+yesterday+'&d=day&bin=1&k=%7B~measurements~%3A%5B4%2C8%5D%7D&m=k&a=0&h=5&c=259&s=1&i=%7B~includeGHI~%3Atrue%7D'
                driver.get(url)
                
                element = WebDriverWait(driver, 60).until(
                    #EC.presence_of_element_located((By.ID, "chart-more-options-button")))
                    EC.presence_of_element_located((By.ID, "data-export-button")))
                element.click()
                
                element = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.ID, "chart-more-options-download-csv")))
                element.click()
                time.sleep(3)

                if(os.path.isfile(self.path+self.filename)):
                    os.rename(self.path+self.filename, dst_filename)      
                    if(os.path.isfile(dst_filename)):
                        self.cleanData(dst_filename)
                        print("ae_" + yesterday +" download succeed")
            except  Exception as e:
                print("ae_"+ yesterday + " download failed")
                #logging.error('Failed to do something: ' + str(e))

                flag = 0
                # Open the file in append mode
                with open('exception/sp.txt', 'r') as f:
                    for line in f:
                    # Check if the string exists in the line
                        if "ae_"+ yesterday in line.strip():
                            # Write the new line
                            flag = 1


                if(flag == 0):
                    with open('exception/sp.txt', 'a') as f:
                        f.write("ae_"+ yesterday + "\n")
                
    def cleanData(self, filename):
            
        df = pd.read_csv(filename, thousands=',')
    
        # rename header
        df.columns = self.alsoNameList   

        # update time format
        df['Time'] = pd.to_datetime(df['Time'], infer_datetime_format=True)

        # fill Nan with preceding values
        df = df.fillna(method='ffill', axis=0)
        df.to_csv(filename)

    '''
128     df = pd.read_csv(alsoenergyPath+filename,thousands=',')
129     df.columns = alsoNameList
130    
131     #update time format
132     df['Time'] = pd.to_datetime(df['Time'], infer_datetime_format=True)
133 
134     df = df.fillna(0)
135 
136     if(mysql_connect.mysql_insert(df=df, table="AlsoEnergy")):
137         print(currentDate + " Insert Succeed!")
138     else:
139         print(currentDate + " Insert Failed!") 
140 
141     alsoenergyDf = pd.concat([alsoenergyDf, df],axis=0)
    '''

