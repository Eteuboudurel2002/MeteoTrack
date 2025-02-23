from dotenv import load_dotenv
import os
import requests

load_dotenv()

api_key = os.getenv("API_KEY")
city = 'paris'
URL = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'

r = requests.get(URL)
data = r.json()

print(data)