import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.service import Service
from datetime import date, timedelta, datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import pytz

class SunnyPortal:
 
    startDate = None
    endDate = None
    timeout = 60
    login_url = 'https://ennexos.sunnyportal.com/login'
    info_url = "https://uiapi.sunnyportal.com/api/v1/measurements/search"
    
    inverterList = {
        '10249473': '1',
        '10249449': '2',
        '10249460': '3',
        '10249447': '4',
        '10249448': '5',
        '10249468': '6',
        '10249454': '7',
        '10249463': '8',
        '10249450': '9',
        '10249466': '10',
        '16142275': '11',
        '10249455': '12',
        '10249464': '13',
        '10424760': '14',
        '10249471': '15',
        '10249457': '16',
        '10249459': '17',
        '10249452': '18',
        '10249451': '19',
        '10249465': '20',
        '10249453': '21',
        '10249467': '22',
        '10249456': '23',
        '10249458': '24',
        '10249461': '25',
        '10249470': '26',
        '10249499': '27',
        '10249489': '28',
        '10249492': '29',
        '10249486': '30',
        '10249504': '31',
        '10249498': '32',
        '13807225': '33',
        '15860505': '34',
        '10249476': '35',
        '10249496': '36',
        '10249497': '37',
        '10249494': '38',
        '10249483': '39',
        '10249481': '40',
        '10249474': '41',
        '11233877': '42',
        '15117262': '43',
        '10249475': '44',
        '10249501': '45',
        '10249484': '46',
        '10249480': '47',
        '10249493': '48',
        '10249479': '49',
        '10249485': '50',
        '10249478': '51',
        '10249477': '52',
        '10249490': '53',
        '10249488': '54',
    }
    sensorList = {'10764334':'29','10764335':'30','10764336':'31','10764341':'ir'}
    sensorChannelList = ['Measurement.InOut.Tmp[0]', 'Measurement.InOut.Tmp[1]','Measurement.InOut.ValNom']
    inverterChannelList = ['Measurement.GridMs.TotW.Pv', 'Measurement.GridMs.W.phsA','Measurement.GridMs.W.phsB','Measurement.GridMs.W.phsC',
        'Measurement.GridMs.TotVAr','Measurement.GridMs.VAr.phsA','Measurement.GridMs.VAr.phsB','Measurement.GridMs.VAr.phsC',
        'Measurement.GridMs.TotVA', 'Measurement.GridMs.VA.phsA','Measurement.GridMs.VA.phsB','Measurement.GridMs.VA.phsC',
        'Measurement.GridMs.PhV.phsA','Measurement.GridMs.PhV.phsB','Measurement.GridMs.PhV.phsC',
        'Measurement.GridMs.A.phsA','Measurement.GridMs.A.phsB','Measurement.GridMs.A.phsC','Measurement.GridMs.Hz',
        'Measurement.DcMs.Watt[0]','Measurement.DcMs.Watt[1]','Measurement.DcMs.Vol[0]','Measurement.DcMs.Vol[1]',
        'Measurement.DcMs.Amp[0]','Measurement.DcMs.Amp[1]','Measurement.Isolation.LeakRis']
    
    AVGmultiAggregateList = ['Measurement.GridMs.PhV.phsA','Measurement.GridMs.PhV.phsB','Measurement.GridMs.PhV.phsC','Measurement.GridMs.Hz', 'Measurement.DcMs.Vol[0]', 'Measurement.DcMs.Vol[1]','Measurement.InOut.Tmp[0]','Measurement.InOut.Tmp[1]','Measurement.InOut.TotInsol']
    MINmultiAggregateList = ['Measurement.Isolation.LeakRis']
    
    solarIrradiance = ['10764341']
    solarIrradianceChannel = 'Measurement.InOut.TotInsol'
    environmentalDeviceIds = {'29', '30', '31'}

    access_token = ''

    # private properties
    __username = ''
    __password = ''
    chrome_path = ''
    driver_path = ''
    path = ''
    keep_browser_open = False

    def __init__(self, path, chrome_path, driver_path):
        self.path = path
        self.chrome_path = chrome_path
        self.driver_path = driver_path
        self._session = requests.Session()

    def _ensure_dir(self, *parts):
        directory = os.path.join(self.path, *parts)
        os.makedirs(directory, exist_ok=True)
        return directory

    def _fetch_measurements(
        self,
        payload: Dict[str, Any],
        headers: Dict[str, str],
        currentDate: datetime,
        label: str,
        use_session: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        if payload is None:
            print(f"Warning: No payload provided for {label}; skipping request")
            return None
        for attempt in range(2):
            try:
                if use_session:
                    response = self._session.post(self.info_url, headers=headers, json=payload)
                else:
                    response = requests.post(self.info_url, headers=headers, json=payload)

                if response.status_code in (401, 403) and attempt == 0:
                    print(f"Warning: token expired for {label}, refreshing")
                    if self._refresh_token():
                        headers["Authorization"] = "Bearer " + self.access_token
                        continue
                if response.status_code == 429 and attempt == 0:
                    print(f"Warning: rate limited for {label}, retrying")
                    time.sleep(2)
                    continue

                response.raise_for_status()
                measurements = json.loads(response.text)
                if isinstance(measurements, dict) and ("error" in measurements or "message" in measurements):
                    print(
                        f"Warning: API returned error for {label} on {currentDate.strftime('%Y-%m-%d')}: {measurements}"
                    )
                    return None
                return measurements
            except requests.exceptions.RequestException as e:
                print(f"Request error fetching {label} for {currentDate.strftime('%Y-%m-%d')}: {e}")
            except json.JSONDecodeError as e:
                print(f"JSON decode error for {label} on {currentDate.strftime('%Y-%m-%d')}: {e}")
            except Exception as e:
                print(f"Error fetching {label} for {currentDate.strftime('%Y-%m-%d')}: {e}")
        return None

    def _refresh_token(self) -> bool:
        driver = None
        try:
            driver = self.chromedriver_init()
            self.access_token = self.login(driver, self.login_url)
            return bool(self.access_token)
        except Exception as e:
            print(f"Token refresh error: {e}")
            return False
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    def _record_exception(self, filename: str) -> None:
        self._ensure_dir("exception")
        exception_path = os.path.join(self.path, "exception", "sp.txt")
        existing = set()
        if os.path.exists(exception_path):
            try:
                with open(exception_path, "r") as f:
                    existing = {line.strip() for line in f if line.strip()}
            except Exception as e:
                print(f"Warning: failed to read exception file: {e}")
        if filename not in existing:
            try:
                with open(exception_path, "a") as f:
                    f.write(filename + "\n")
            except Exception as e:
                print(f"Warning: failed to write exception file: {e}")

    def _record_no_inverter_values(self, inverter_id: str, currentDate: datetime) -> None:
        self._ensure_dir("exception")
        inverter_no = self.inverterList.get(inverter_id, inverter_id)
        record = f"{currentDate.strftime('%Y-%m-%d')}, inverter {inverter_no}"
        exception_path = os.path.join(self.path, "exception", "sp_no_inverter_values.txt")
        existing = set()
        if os.path.exists(exception_path):
            try:
                with open(exception_path, "r") as f:
                    existing = {line.strip() for line in f if line.strip()}
            except Exception as e:
                print(f"Warning: failed to read no-inverter-values file: {e}")
        if record not in existing:
            try:
                with open(exception_path, "a") as f:
                    f.write(record + "\n")
            except Exception as e:
                print(f"Warning: failed to write no-inverter-values file: {e}")

    def _all_values_empty(self, measurements: Optional[List[Dict[str, Any]]]) -> bool:
        if not measurements:
            return True
        for component in measurements:
            values = component.get("values")
            if values:
                return False
        return True

    def setUserName(self, username):
        self.__username = username

    def setPassword(self, password):
        self.__password = password

    def setStartDate(self, startDate):
        self.startDate = startDate
    
    def setEndDate(self, endDate):
        self.endDate = endDate

    def setKeepBrowserOpen(self, keep_open: bool):
        self.keep_browser_open = keep_open

    def ny2utc(self, ny_time):
        return ny_time.astimezone(pytz.utc)

    def utc2ny(self, utc_time):
        return utc_time.astimezone(pytz.timezone('America/New_York'))
  
    def composePayloads(self, deviceID, begin_time, end_time):
        
        json_structure = {'queryItems': [],
                   'dateTimeBegin': begin_time,
                   'dateTimeEnd': end_time}
        
        if deviceID in self.inverterList:            
         
            for channel in self.inverterChannelList:
                if channel in self.AVGmultiAggregateList:
                    multi_aggregate = 'Avg'
                elif channel in self.MINmultiAggregateList:
                    multi_aggregate = 'Min'
                else:
                    multi_aggregate = 'Sum'
                    
                json_structure['queryItems'].append(
                    {
                        'componentId': deviceID,
                        'channelId': channel,
                        'timezone': 'America/New_York',
                        'aggregate': 'Avg',
                        'multiAggregate': multi_aggregate
                    },)
        
        elif deviceID in self.sensorList:              
            for channel in self.sensorChannelList:
                if channel in self.AVGmultiAggregateList:
                    multi_aggregate = 'Avg'
                elif channel in self.MINmultiAggregateList:
                    multi_aggregate = 'Min'
                else:
                    multi_aggregate = 'Sum'
                    
                json_structure['queryItems'].append(
                    {
                        'componentId': deviceID,
                        'channelId': channel,
                        'timezone': 'America/New_York',
                        'aggregate': 'Avg',
                        'multiAggregate': multi_aggregate
                    },)
        
            if deviceID in self.solarIrradiance:
                 json_structure['queryItems'].append(
                    {
                        'componentId': deviceID,
                        'channelId': self.solarIrradianceChannel,
                        'timezone': 'America/New_York',
                        'aggregate': 'Avg',
                        'multiAggregate': 'Sum'
                    },)
                
        
        else:
            print("Wrong device ID")
            return None
        
        return json_structure #json.dumps(json_structure, indent=4) 
            
    def composeDataFrame(self, deviceID, currentDate, measurements):
        df = pd.DataFrame()
        time_list = pd.date_range(start=currentDate.replace(hour=0, minute=0),end=currentDate.replace(hour=23, minute=55), freq="5min", tz='US/Eastern')   
        index = 0
        if measurements is None:
            #print(currentDate.strftime('%Y-%m-%d'), " data not available")
            return
        else:
            for component in measurements:
                if(len(component['values'])>0):
                    new_df = pd.DataFrame()
                    current_index = 0
                   
                    for record in component['values']:
                        if(record['value'] is None):
                            record['value'] = -1   
                        # Convert UTC time to NY time
                        ny_time = self.utc2ny(datetime.strptime(record['time'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc))
                        ny_time_string = ny_time.strftime("%Y-%m-%d %H:%M:%S")
                         
                        # Fill the record before sunrise to -1    
                        if current_index < len(time_list) and (ny_time > time_list[current_index]):
                            update_date = time_list[current_index]                     
                            while current_index < len(time_list) and (update_date < ny_time):
                                if(index==0):
                                    tmp_df = pd.DataFrame({'time': update_date.strftime("%Y-%m-%d %H:%M:%S"),'value':[-1]})
                                else:
                                    tmp_df = pd.DataFrame({'value': [-1]})
                                new_df = pd.concat([new_df, tmp_df], axis=0)
                                current_index = current_index + 1
                                if current_index < len(time_list):
                                    update_date = time_list[current_index]
                        if current_index >= len(time_list):
                            break
                        
                        if(index==0):
                            tmp_df = pd.DataFrame({'time': ny_time_string,'value':[record['value']]})
                        else:
                            tmp_df = pd.DataFrame({'value':[record['value']]})
                        current_index = current_index + 1
                
                        new_df = pd.concat([new_df, tmp_df], axis=0)  
                
                  
                    while(current_index<time_list.shape[0]):
                        if(index == 0):
                             tmp_df = pd.DataFrame({'time': time_list[current_index].strftime("%Y-%m-%d %H:%M:%S"),'value':[-1]})
                        else:
                            tmp_df = pd.DataFrame({'value':[-1]})
                        new_df = pd.concat([new_df, tmp_df], axis=0)
                        current_index = current_index + 1
                    index = index + 1

                    df = pd.concat([df, new_df], axis=1, ignore_index=True)
                    
                else:
                    # Component has empty values - create column filled with -1
                    new_df = pd.DataFrame()
                    for current_index in range(len(time_list)):
                        if(index == 0):
                            tmp_df = pd.DataFrame({'time': time_list[current_index].strftime("%Y-%m-%d %H:%M:%S"),'value':[-1]})
                        else:
                            tmp_df = pd.DataFrame({'value':[-1]})
                        new_df = pd.concat([new_df, tmp_df], axis=0)
                    index = index + 1
                    df = pd.concat([df, new_df], axis=1, ignore_index=True)
            
            # Check if df is empty (no data was added)
            if df.empty:
                return None
            
            if deviceID in self.inverterList: 
                deviceid = pd.concat([pd.DataFrame([self.inverterList[deviceID]])] * len(df))
                df = pd.concat([df, deviceid], axis=1, ignore_index=True)
                df.columns = ['time','ac_power','ac_power_l1','ac_power_l2','ac_power_l3', 'ac_reactive_power','ac_reactive_power_l1','ac_reactive_power_l2','ac_reactive_power_l3','ac_apparent_power','ac_apparent_power_l1','ac_apparent_power_l2','ac_apparent_power_l3','ac_voltage_l1','ac_voltage_l2','ac_voltage_l3','ac_current_l1','ac_current_l2','ac_current_l3','grid_frequency','dc_power_a','dc_power_b','dc_voltage_a','dc_voltage_b','dc_current_a','dc_current_b','iso', 'deviceID'] 
                
            elif deviceID in self.sensorList:
                if deviceID in self.solarIrradiance:
                    df.columns = ['time','ambient_temp1','ambient_temp2', 'ambient_rh', 'ir']
                else:
                    deviceid = pd.concat([pd.DataFrame([self.sensorList[deviceID]])] * len(df))
                    df = pd.concat([df, deviceid], axis=1, ignore_index=True)
                    df.columns = ['time','inv_temp1', 'inv_temp2', 'inv_rh', 'deviceID']
                    df = df[['time', 'inv_temp1', 'inv_temp2', 'inv_rh', 'deviceID']]
                    
            else:
                print("Wrong Device ID")
                return None


        return df

    def requestInfo(self, currentDate):

        if not self.access_token:
            print("Warning: access token is empty; skipping request")
            return

        begin_time = (self.ny2utc((currentDate-timedelta(days=1)).replace(hour=23, minute=55))).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_time = (self.ny2utc(currentDate.replace(hour=23, minute=55))).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        #end_time = (self.ny2utc(currentDate+timedelta(days=1))).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        #begin_time = currentDate.strftime("%Y-%m-%dT00:00:00.000Z")
        #end_time = (currentDate+timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000Z")

        info_header = {
            'Accept' : 'application/json, text/plain, */*',
            'Accept-Encoding' : 'gzip, deflate, br',
            'Accept-Language' : 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection' : 'keep-alive',
            'Content-Type' : 'application/json',
            'Authorization' : 'Bearer '+ self.access_token,
            'Host' : 'uiapi.sunnyportal.com',
            'Origin' : 'https://ennexos.sunnyportal.com',
            'Referer' : 'https://ennexos.sunnyportal.com/',
            'Sec-Fetch-Dest' : 'empty',
            'Sec-Fetch-Mode' : 'cors',
            'Sec-Fetch-Site' : 'same-site',
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'sec-ch-ua-platform' : 'Windows'
        }
        
        
        inverter_dfs = []
        sensor_dfs = []
        
        # Environmental data should include solar irradiance/ambient for 29-31
        solar_df = None
        if self.solarIrradiance:
            solar_info_payloads = self.composePayloads(self.solarIrradiance[0], begin_time, end_time)
            solar_measurements = self._fetch_measurements(
                solar_info_payloads,
                info_header,
                currentDate,
                "solar data",
            )
            if solar_measurements:
                solar_df = self.composeDataFrame(self.solarIrradiance[0], currentDate, solar_measurements)
                if solar_df is None or solar_df.empty:
                    print(f"Warning: Solar data for {currentDate.strftime('%Y-%m-%d')} is empty or None")
                    solar_df = None
        else:
            print("Warning: Solar irradiance list is empty; skipping solar data fetch")
        
        def _fetch_inverter(device_id: str) -> Tuple[str, Optional[List[Dict[str, Any]]]]:
            payload = self.composePayloads(device_id, begin_time, end_time)
            measurements = self._fetch_measurements(
                payload,
                info_header,
                currentDate,
                f"inverter {device_id} data",
                use_session=False,
            )
            return device_id, measurements

        inverter_ids = list(self.inverterList.keys())
        max_workers = min(8, max(1, len(inverter_ids)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_fetch_inverter, inverter) for inverter in inverter_ids]
            for future in as_completed(futures):
                inverter, inverter_measurements = future.result()
                if self._all_values_empty(inverter_measurements):
                    print(f"Warning: No inverter values for {inverter} on {currentDate.strftime('%Y-%m-%d')}")
                    self._record_no_inverter_values(inverter, currentDate)
                inverter_df = self.composeDataFrame(inverter, currentDate, inverter_measurements)
                if inverter_df is not None and not inverter_df.empty:
                    inverter_dfs.append(inverter_df)

        def _fetch_sensor(sensor_id: str) -> Tuple[str, Optional[List[Dict[str, Any]]]]:
            payload = self.composePayloads(sensor_id, begin_time, end_time)
            measurements = self._fetch_measurements(
                payload,
                info_header,
                currentDate,
                f"sensor {sensor_id} data",
                use_session=False,
            )
            return sensor_id, measurements

        sensor_ids = [sid for sid in self.sensorList if self.sensorList[sid] in self.environmentalDeviceIds]
        if sensor_ids:
            max_workers = min(4, max(1, len(sensor_ids)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(_fetch_sensor, sensor) for sensor in sensor_ids]
                for future in as_completed(futures):
                    sensor, sensor_measurements = future.result()
                    if self._all_values_empty(sensor_measurements):
                        print(f"Warning: No sensor values for {sensor} on {currentDate.strftime('%Y-%m-%d')}")
                    sensor_df = self.composeDataFrame(sensor, currentDate, sensor_measurements)
                    if sensor_df is not None and not sensor_df.empty:
                        if solar_df is not None and not solar_df.empty:
                            solar_aligned = solar_df.reset_index(drop=True)
                            sensor_aligned = sensor_df.reset_index(drop=True)
                            sensor_aligned = sensor_aligned.drop(columns=['time'], errors='ignore')
                            sensor_df = pd.concat([solar_aligned, sensor_aligned], axis=1)
                        sensor_dfs.append(sensor_df)
                    else:
                        print(f"Warning: Sensor data for {currentDate.strftime('%Y-%m-%d')} is empty or None")
            
        op_filename = "operating/sp_" + currentDate.strftime("%Y-%m-%d")
        env_filename = "environmental/sp_" + currentDate.strftime("%Y-%m-%d")
        
        try:
            if len(inverter_dfs) > 0:
                self._ensure_dir("operating")
                final_inverter_df = pd.concat(inverter_dfs, ignore_index=True)
                op_path = os.path.join(self.path, op_filename + ".csv")
                final_inverter_df.to_csv(op_path, index=False)
                print(op_filename," has been saved!")
            else:
                print(f"Warning: No inverter data available for {currentDate.strftime('%Y-%m-%d')}, skipping save")
        except Exception as e:
                print(op_filename,"save Failed: ",e)
    
        try:
            if len(sensor_dfs) > 0:
                self._ensure_dir("environmental")
                final_sensor_df = pd.concat(sensor_dfs, ignore_index=True)
                env_path = os.path.join(self.path, env_filename + ".csv")
                final_sensor_df.to_csv(env_path, index=False)
                print(env_filename," has been saved!")
            else:
                print(f"Warning: No sensor data available for {currentDate.strftime('%Y-%m-%d')}, skipping save")
        except Exception as e:
                print(env_filename,"save Failed: ",e)

    def chromedriver_init(self):
    
        caps = DesiredCapabilities.CHROME
        caps['goog:loggingPrefs'] = {'performance': 'ALL'}

        option = webdriver.ChromeOptions()
        # Set Chrome binary location if provided
        if self.chrome_path and os.path.exists(self.chrome_path):
            # Make sure Chrome binary is executable
            os.chmod(self.chrome_path, 0o755)
            option.binary_location = self.chrome_path
            print(f"Using Chrome binary: {self.chrome_path}")
        
        # Essential arguments for server/headless environments
        option.add_argument('--no-sandbox')
        option.add_argument('--disable-dev-shm-usage')
        option.add_argument('--disable-gpu')
        option.add_argument('--disable-software-rasterizer')
        option.add_argument('--disable-extensions')
        option.add_argument('--disable-background-networking')
        option.add_argument('--disable-background-timer-throttling')
        option.add_argument('--disable-renderer-backgrounding')
        option.add_argument('--disable-backgrounding-occluded-windows')
        option.add_argument('--disable-breakpad')
        option.add_argument('--disable-client-side-phishing-detection')
        option.add_argument('--disable-default-apps')
        option.add_argument('--disable-hang-monitor')
        option.add_argument('--disable-popup-blocking')
        option.add_argument('--disable-prompt-on-repost')
        option.add_argument('--disable-sync')
        option.add_argument('--disable-translate')
        option.add_argument('--metrics-recording-only')
        option.add_argument('--no-first-run')
        option.add_argument('--safebrowsing-disable-auto-update')
        option.add_argument('--enable-automation')
        option.add_argument('--password-store=basic')
        option.add_argument('--use-mock-keychain')
        option.add_argument('--incognito')
        option.add_argument("--enable-logging")
        option.add_argument('--ignore-certificate-errors')
        option.add_argument('--ignore-certificate-errors-spki-list')
        option.add_argument('--ignore-ssl-errors')
        option.add_argument('--window-size=1920,1080')
        option.add_argument("--headless")
        option.add_argument('--verbose')
        option.add_argument('--log-level=3')

        option.add_experimental_option('useAutomationExtension', False)
        option.add_experimental_option('excludeSwitches', ['enable-automation', 'enable-logging'])
        # Enable logging and specify logging preferences for network and performance logs
        option.add_experimental_option("perfLoggingPrefs", {"enableNetwork": True, "enablePage": True, "traceCategories": "devtools.timeline,devtools.network,devtools.page"})

        # Set ChromeDriver service with driver path if provided
        try:
            if self.driver_path and os.path.exists(self.driver_path):
                print(f"Using ChromeDriver: {self.driver_path}")
                # Make sure the driver is executable
                os.chmod(self.driver_path, 0o755)
                service = Service(self.driver_path)
                driver = webdriver.Chrome(service=service, options=option)
            else:
                print("Using system ChromeDriver")
                driver = webdriver.Chrome(options=option)
        except Exception as e:
            print(f"Error initializing ChromeDriver: {e}")
            # Try with minimal options as fallback
            minimal_option = webdriver.ChromeOptions()
            if self.chrome_path and os.path.exists(self.chrome_path):
                minimal_option.binary_location = self.chrome_path
            minimal_option.add_argument('--no-sandbox')
            minimal_option.add_argument('--disable-dev-shm-usage')
            if self.driver_path and os.path.exists(self.driver_path):
                service = Service(self.driver_path)
                driver = webdriver.Chrome(service=service, options=minimal_option)
            else:
                driver = webdriver.Chrome(options=minimal_option)

        return driver

    def login(self, driver, url):
        driver.get(url)

        try:
            time.sleep(2)
            # Try to dismiss cookie/consent overlays that block clicks (headless often hits this)
            try:
                consent_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
                )
                consent_button.click()
            except Exception:
                pass
            try:
                driver.execute_script(
                    "const el=document.getElementById('cmpwrapper'); if(el){el.style.display='none';}"
                )
            except Exception:
                pass

            element = WebDriverWait(driver, self.timeout).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/sma-ennexos/div/mat-sidenav-container/mat-sidenav-content/div/main/sma-smaid-startpage/div/div[1]/div/div[1]/div/ennexos-button")))
            try:
                element.click()
            except Exception:
                driver.execute_script("arguments[0].click();", element)
        except (TimeoutException, WebDriverException) as e:
            self._ensure_dir("exception")
            screenshot_path = os.path.join(self.path, "exception", "login_error.png")
            try:
                driver.save_screenshot(screenshot_path)
                print(f"Login debug screenshot saved: {screenshot_path}")
            except Exception:
                pass
            print(f"Login interaction error: {e}")
            print(f"Login page url: {driver.current_url}")
            raise
        '''
        try:
            # Wait for the cookie consent pop-up to appear
            element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.ID, "onetrust-accept-btn-handler")))
            element.click()
            print("1")
            
            element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/sma-ennexos/div/mat-sidenav-container/mat-sidenav-content/div/main/sma-smaid-startpage/div/div[1]/div/div[1]/div/ennexos-button")))
            element.click()
            print("2")

            time.sleep(5)

        except Exception as e:

            print(f"Error: {e}")

       '''
        time.sleep(3)
        print("Login Page")
    
        print(driver.current_url)

        # username
        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div/main/div[2]/form/div[2]/input")))
        element.clear()
        element.send_keys(self.__username)

        # password
        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div/main/div[2]/form/div[2]/div[1]/input")))
        element.clear()
        element.send_keys(self.__password)

        # submit
        element = WebDriverWait(driver, self.timeout).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/div/main/div[2]/form/div[2]/div[3]/button")))
        element.click()

        time.sleep(3)

        access_token = ''

        logs = driver.get_log("performance")
        for entry in logs:
            if "Bearer" in str(entry["message"]):
                access_token = (entry["message"].split()[3]).split('"')[0]
                if(len(access_token) > 128):
                    break

        return access_token  

    def SunnyPortal(self):

        driver = None
        try:
            driver = self.chromedriver_init()
            self.access_token = self.login(driver, self.login_url)
            print("Crediential verified")
        except Exception as e:
            print("Login Error: ", e)
        finally:
            if driver is not None and not self.keep_browser_open:
                try:
                    driver.quit()
                except Exception as e:
                    print(f"Warning: failed to close browser: {e}")

        if not self.access_token:
            print("Warning: access token is empty; aborting requests")
            return


        if(self.startDate is None):
            self.startDate = datetime.now() - timedelta(days=1)
        
        currentDate = self.startDate

        if(self.endDate is None):
            self.endDate = datetime.now() - timedelta(days=1)

        while (currentDate <= self.endDate):
            filename= "sp_" + currentDate.strftime("%Y-%m-%d")
            op_path = os.path.join(self.path, "operating", filename + ".csv")
            env_path = os.path.join(self.path, "environmental", filename + ".csv")
            if(os.path.isfile(op_path) and os.path.isfile(env_path)):
                print(filename, " existed!")
            else:
                try:
                    self.requestInfo(currentDate)
                except Exception as e:
                    print(currentDate.strftime("%Y-%m-%d"), " Request Information Error: ",e)
                    self._record_exception(filename)
                    
                    currentDate = currentDate + timedelta(days=1)
                    continue
                

            currentDate = currentDate + timedelta(days=1)
