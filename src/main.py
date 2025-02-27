from dotenv import load_dotenv
import os
import requests
import json

load_dotenv()

api_key = os.getenv("API_KEY")
city = 'paris'
URL = f'https://api.openweathermap.org/data/3.0/onecall?lat=48.8534&lon=2.3488&exclude=minutely,hourly&appid={api_key}'

r = requests.get(URL)
data = r.json()

# Write data into a file
with open("../Data/raw_data.json", "w") as f :
    json.dump(data, f )

print(data)
