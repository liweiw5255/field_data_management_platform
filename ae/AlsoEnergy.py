import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.proxy import Proxy, ProxyType
from datetime import date
from datetime import timedelta
import pandas as pd
import logging
import random


class AlsoEnergy:

    # properties
    filename = 'chart-data.csv'
    url = "https://apps.alsoenergy.com/"
    driverPath = ''
    startDate = date(date.today().year, date.today().month, date.today().day) - timedelta(days=1)
    endDate = date(date.today().year, date.today().month, date.today().day) - timedelta(days=1)
    timeout = 60
    alsoNameList = ["Time","GHI","POA","ambient_temp","module_temp"]
    # private properties
    __username = ''
    __password = ''
 
    # private properties
    __username = ''
    __password = ''

    def __init__(self, path, driverPath, chromePath):
        self.path = path
        self.driverPath = driverPath
        self.chromePath = chromePath
    
    # credential information get methods
    def setUserName(self, username):
        self.__username = username
    
    def setPassword(self, password):
        self.__password = password
    
    def setStartDate(self, start_date):
        self.startDate = start_date
        
    def setEndDate(self, end_date):
        self.endDate = end_date

    def loginWithRetry(self, driver, retries=3):
        attempt = 0
        while attempt < retries:
            try:
                self.loginWebsite(driver)  # Try to login
                print("Login succeeded")
                break
            except Exception as e:
                print(f"Login attempt {attempt + 1} failed. Error: {str(e)}")
                attempt += 1
                time.sleep(1)  # Wait before retrying
                if attempt == retries:
                    print("Maximum retries reached. Login failed.")
                    raise

    def enable_download_headless(self, driver):
        driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')
        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': self.path}}
        driver.execute("send_command", params)

    def initChromeDriver(self):
        option = webdriver.ChromeOptions()
        #option.binary_location = self.chromePath
        option.add_argument("--headless")
        option.add_argument("--incognito")
        option.add_argument('--no-sandbox')
        option.add_argument('--verbose')
        option.add_argument('--disable-gpu')
        option.add_argument('--disable-software-rasterizer')
        option.add_argument('--disable-logging')
        option.add_argument('--ignore-certificate-errors')
        
        # Randomize User-Agent to simulate different browsers
        user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36"
        ]
        user_agent = random.choice(user_agents)
        option.add_argument(f'user-agent={user_agent}')
        
        driver = webdriver.Chrome(options=option)
        self.enable_download_headless(driver)

        return driver

    def download_click(self, driver):
        try:
            element = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.ID, "data-export-button")))
            element.click()
            
            element = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "chart-more-options-download-csv")))
            element.click()
        except Exception as e:
            print(f"Error during download: {str(e)}")
            raise

    def retryConnectSecondPage(self, yesterday, driver, retries=6, delay=5):
        attempt = 0
        second_page_url = f'https://apps.alsoenergy.com/powertrack/S40225/analysis/chartbuilder?start={yesterday}&end={yesterday}&d=day&bin=1&k=%7B~measurements~%3A%5B4%2C8%5D%7D&m=k&a=0&h=5*1&c=259&s=1&i=%7B~includeGHI~%3Atrue%7D'

        while attempt < retries:
            try:
                driver.get(second_page_url)  # Attempt to connect to the second page
                WebDriverWait(driver, self.timeout).until(
                    EC.presence_of_element_located((By.ID, "data-export-button")))  # Adjust with an actual element on the second page
                # print("Successfully connected to the second page.")
                return driver  # Return the driver after successful connection
            except Exception as e:
                attempt += 1
                print(f"Attempt {attempt} to connect to the second page failed. Error: {str(e)}")
                if attempt < retries:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)  # Retry after delay
                else:
                    print("Failed to connect to the second page after several retries.")
                    raise

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

        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/main/section/div/div/div/form/div[2]/button")))
        element.click()
        print("Also Energy login succeeded!")

    def AlsoEnergy(self):
        # initialize web driver
        driver = self.initChromeDriver()
        # login to the website
        self.loginWithRetry(driver, retries=3)

        while self.startDate <= self.endDate:
            yesterday = self.startDate.strftime("%Y-%m-%d")
            dst_filename = os.path.join(self.path, f'ae_{yesterday}.csv')
            self.startDate += timedelta(days=1)

            if os.path.isfile(dst_filename):
                print(f"ae_{yesterday} file already exists!")
                continue
            
            try:
                print(f"Processing data for {yesterday}")
                # Use retryConnectSecondPage and pass necessary arguments
                driver = self.retryConnectSecondPage(yesterday, driver, retries=3, delay=5)

                # Retry until file is available or fail after a few attempts
                attempts = 0
                while not os.path.isfile(self.path + self.filename) and attempts < 5:
                    self.download_click(driver)
                    time.sleep(1)
                    print("Download in progress...")
                    attempts += 1
                if os.path.isfile(self.path + self.filename):
                    os.rename(self.path + self.filename, dst_filename)
                    if os.path.isfile(dst_filename):
                        self.cleanData(dst_filename)
                        print(f"ae_{yesterday} download succeeded")
                    else:
                        print("Download failed!")
                else:
                    print("Failed to download file after multiple attempts.")
            
            except Exception as e:
                print(f"ae_{yesterday} download failed: {str(e)}")
                self.logError(yesterday)

    def cleanData(self, filename):
        df = pd.read_csv(filename, thousands=',')
        df.columns = self.alsoNameList  # rename header
        df['Time'] = pd.to_datetime(df['Time'], infer_datetime_format=True)  # update time format
        df = df.fillna(method='bfill', axis=0)  # fill NaN with preceding values
        df.to_csv(filename)

    def logError(self, yesterday):
        flag = False
        exception_file = 'exception/ae.txt'

        with open(exception_file, 'r') as f:
            for line in f:
                if f"ae_{yesterday}" in line.strip():
                    flag = True
                    break

        if not flag:
            with open(exception_file, 'a') as f:
                f.write(f"ae_{yesterday}\n")

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

