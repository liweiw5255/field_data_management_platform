"""
Example configuration file for data management platform.
Copy this file to config.py and update with your actual credentials.
Alternatively, set environment variables instead of modifying config.py directly.
"""
import os

# AlsoEnergy Credentials
AE_USERNAME = os.getenv('AE_USERNAME', 'your_ae_username_here')
AE_PASSWORD = os.getenv('AE_PASSWORD', 'your_ae_password_here')

# SunnyPortal Credentials
SP_USERNAME = os.getenv('SP_USERNAME', 'your_sp_username_here')
SP_PASSWORD = os.getenv('SP_PASSWORD', 'your_sp_password_here')

# Weather Station API Key
WS_API_KEY = os.getenv('WS_API_KEY', 'your_ws_api_key_here')

# MySQL Database Credentials
MYSQL_HOST = os.getenv('MYSQL_HOST', 'your_mysql_host_here')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME', 'your_mysql_username_here')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'your_mysql_password_here')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'your_mysql_database_here')

# SFTP Configuration
SFTP_HOST = os.getenv('SFTP_HOST', 'your_sftp_host_here')
SFTP_USERNAME = os.getenv('SFTP_USERNAME', 'your_sftp_username_here')
SFTP_PRIVATE_KEY_PATH = os.getenv('SFTP_PRIVATE_KEY_PATH', 'de/sftp/clemson_privatekey.pem')

