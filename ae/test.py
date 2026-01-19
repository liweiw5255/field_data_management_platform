import requests
from bs4 import BeautifulSoup

# Replace with your actual username and password
username = 'tracepv2022@outlook.com'
password = 'tracePV123........'

# Start a session to manage cookies
session = requests.Session()

state = 'hKFo2SBCYndpZWZhSDhQS2QxU1ZLRS15MFlCQWo0dlNIdFd2aKFur3VuaXZlcnNhbC1sb2dpbqN0aWTZIDVIZUVhMUkzQUQwcDVoRmlHeGt2OWwzWmxqY0tRYmpEo2NpZNkgc3dhdzJrcGs2RWxzcXJxWVpoVkwyeU9OdFdJeU9rQ3o'

# The login URL with the 'state' parameter included
login_url = 'https://login.stem.com/u/login/password?state=' + state


# Step 1: Get the login page to retrieve cookies and hidden form fields
response = session.get(login_url)

# Check if the page was retrieved successfully
if response.status_code != 200:
    print("Failed to load the login page.")
    exit()

# Parse the login page to extract hidden form fields (e.g., CSRF tokens)
soup = BeautifulSoup(response.text, 'html.parser')

# Prepare the payload with hidden inputs
payload = {}

# Find all input tags
input_tags = soup.find_all('input')

for input_tag in input_tags:
    name = input_tag.get('name')
    value = input_tag.get('value', '')
    if name:
        payload[name] = value
        


# Update the payload with your username and password
payload.update({
    'username': username,
    'password': password,
    'action': 'default',
    'state': state,
})

print(payload)

# Headers to mimic a real browser
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/129.0.0.0 Safari/537.36',
    'Referer': login_url,
    'Origin': 'https://login.stem.com',
}

# Step 2: Submit the login form
login_response = session.post(login_url, data=payload, headers=headers)

# Check if login was successful by examining the response URL or content
if 'error' in login_response.url or 'Login failed' in login_response.text:
    print("Login failed.")
else:
    print("Login successful!")

    # You can now make authenticated requests using the session
    # For example, access the dashboard or profile page
    dashboard_url = 'https://apps.alsoenergy.com/powertrack/C17774/overview/sites'
    dashboard_response = session.get(dashboard_url)

    if dashboard_response.status_code == 200:
        print(dashboard_response.content)
        # print("Accessed the dashboard successfully!")
    else:
        print("Failed to access the dashboard.")