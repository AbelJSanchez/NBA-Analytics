import http.client
import os
from dotenv import load_dotenv # pip install python-dotenv
import json
import pandas as pd # pip install pandas

conn = http.client.HTTPSConnection("v2.nba.api-sports.io")

headers = {
    'x-rapidapi-host': "v2.nba.api-sports.io",
    'x-rapidapi-key': "e095cd24c68a45af7e2ade71eeaba611"
    }

conn.request("GET", "/players/statistics?season=2024&id=265", headers=headers)

res = conn.getresponse()
data = res.read()
json_data = json.loads(data)


print(json.dumps(json_data, indent=2))