from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
import psycopg2

default_args = {
    'owner': 'meteotrack8',
    'start_date': datetime(2025, 2, 25, 00, 00)
}

class Destination:

    def __init__(self, name, lat, long) -> None:
        self.name = name
        self.lat = lat
        self.long = long


def get_destination():

    HOST = Variable.get("HOST")
    PORT = Variable.get("PORT")  
    DATABASE = Variable.get("DATABASE")
    USER = Variable.get("USER")
    PASSWORD = Variable.get("PASSWORD")

    destinations = []
    
    try:
        connection = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )

        cursor = connection.cursor()

        # Query to fetch data from the Destination table
        cursor.execute("SELECT * FROM Destination;")

        # Fetch all rows from the executed query
        rows = cursor.fetchall()

        # Display the fetched data
        for row in rows:
            dest = Destination(row[0], row[1], row[2])
            destinations.append(dest)

        return destinations    

    except Exception as error:
        print(f"Error: {error}")

    finally:
        # Close the cursor and connection to free up resources
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def request_api():

    destinations = get_destination()

    destinations = [
        Destination("Arc de Triomphe", 48.87824713555056, 2.2951888736057775),
        Destination("Musée du Louvre", 48.86077341478219, 2.337665454169912),
    ]

    api_key =  Variable.get("API_KEY")

    dest_weather_data =[]

    for dest in destinations:

        URL = f'https://api.openweathermap.org/data/3.0/onecall?lat={dest.lat}4&lon={dest.long}&exclude=current,minutely,hourly&appid={api_key}'

        r = requests.get(URL)

        data = r.json()


        for entry in data.get("daily", []):
            dt = entry["dt"]
            day = datetime.utcfromtimestamp(dt).strftime('%Y-%m-%d').split('-')[2]
            
            humidity = entry.get("humidity", None)
            pressure = entry.get("pressure", None)
            wind_speed = entry.get("wind_speed", None)
            main_weather = entry.get("weather", [{}])[0].get("main", None)
            rain = entry.get("rain", None) 

            dest_weather_data.append({
                "day": day,
                "humidity": humidity,
                "pressure": pressure,
                "main": main_weather,
                "wind_speed": wind_speed,
                "rain": rain,
                "name_destination": dest.name
            })

    
    print(dest_weather_data)

        
#request_api()

with DAG('meteotrack',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='request_api',
        python_callable=request_api
    )