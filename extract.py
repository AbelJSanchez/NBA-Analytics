import http.client
import os
from dotenv import load_dotenv # pip install python-dotenv
import json
import pandas as pd # pip install pandas

# Retrieve API and Database credentials from local .env file
load_dotenv()
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
# DB_HOST = os.getenv("DB_HOST")
# DB_USER = os.getenv("DB_USER")
# DB_PW = os.getenv("DB_PASS")
# DB_NAME = os.getenv("DB_NAME")

if not API_URL:
    raise ValueError("API_URL not found in .env file. Please check your configuration.")

if not API_KEY:
    raise ValueError("API_KEY not found in .env file. Please check your configuration.")

# Database credentials commented out until created
# if not DB_HOST:
    # raise ValueError("DB_HOST not found in .env file. Please check your configuration.")

# if not DB_USER:
    # raise ValueError("DB_USER not found in .env file. Please check your configuration.")

# if not DB_PW:
    # raise ValueError("DB_PW not found in .env file. Please check your configuration.")

# if not DB_NAME:
    # raise ValueError("DB_NAME not found in .env file. Please check your configuration.")

conn = http.client.HTTPSConnection(API_URL)

headers = {
    'x-rapidapi-host': API_URL,
    'x-rapidapi-key': API_KEY,
    }

conn.request("GET", "/teams", headers=headers)

res = conn.getresponse()
data = res.read()

json_data = json.loads(data)
print(json.dumps(json_data, indent=2))