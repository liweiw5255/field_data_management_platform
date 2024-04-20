from selenium import webdriver

option = webdriver.ChromeOptions()
option.binary_location = "/home/liweiw/TRACE_PV/data_process/data_management_platform/chrome/chrome"
option.add_argument("--headless")
option.add_argument('--no-sandbox')
option.add_argument('--verbose')
option.add_argument('--disable-gpu')
option.add_argument('--disable-software-rasterizer')
option.add_argument('--disable-logging')

driver = webdriver.Chrome(executable_path="/home/liweiw/TRACE_PV/data_process/data_management_platform/chromedriver", options=option)