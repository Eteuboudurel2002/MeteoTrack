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
        print(rows)

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

def update_weather(weather_data):
    
    HOST = Variable.get("HOST")
    PORT = Variable.get("PORT")  
    DATABASE = Variable.get("DATABASE")
    USER = Variable.get("USER")
    PASSWORD = Variable.get("PASSWORD")
    
    try:
        connection = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
        cursor = connection.cursor()
        
        # Query to upsert weather data into the database 
        upsert_query = f"""INSERT INTO weather (weather_date, temp, feel_temp, humidity, pressure,main, wind_speed,rain, summary,  name_destination) 
                        VALUES 
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (weather_date ,name_destination) DO UPDATE
                        SET (temp, feel_temp, humidity, pressure,main, wind_speed,rain, summary) = (EXCLUDED.temp, EXCLUDED.feel_temp ,EXCLUDED.humidity, EXCLUDED.pressure, EXCLUDED.main, EXCLUDED.wind_speed,EXCLUDED.rain, EXCLUDED.summary)"""
        #
        for dest_weather in weather_data :
            print(dest_weather)
            cursor.execute( upsert_query,
                           (dest_weather["weather_date"], dest_weather["temp"], dest_weather["feel_temp"], dest_weather["humidity"], dest_weather["pressure"], dest_weather["main"], dest_weather["wind_speed"], dest_weather["rain"], dest_weather["summary"], dest_weather["name_destination"]))
        
        connection.commit()
    except Exception as error:
        print(f"Error: {error}")
    
    finally :
        # Close the cursor and connection to free up resources
        if cursor :
            cursor.close()
        if connection :
            connection.close()
        
    
    
def request_api():

    destinations = get_destination()

    api_key =  Variable.get("API_KEY")

    dest_weather_data =[]

    for dest in destinations:

        URL = f'https://api.openweathermap.org/data/3.0/onecall?lat={dest.lat}4&lon={dest.long}&exclude=current,minutely,hourly&appid={api_key}&units=metric'

        r = requests.get(URL)

        data = r.json()


        for entry in data.get("daily", []):
            dt = entry["dt"]
            weather_date = datetime.utcfromtimestamp(dt).strftime('%Y-%m-%d')
            temp = entry.get("temp")["day"]
            feel_temp = entry.get("feels_like")["day"]
            humidity = entry.get("humidity", None)
            pressure = entry.get("pressure", None)
            wind_speed = entry.get("wind_speed", None)
            main_weather = entry.get("weather", [{}])[0].get("main", None)
            rain = entry.get("rain", None) 
            summary = entry.get("summary", None)

            dest_weather_data.append({
                "weather_date": weather_date,
                "temp": temp,
                "feel_temp": feel_temp,
                "humidity": humidity,
                "pressure": pressure,
                "main": main_weather,
                "wind_speed": wind_speed,
                "rain": rain,
                "summary":summary,
                "name_destination": dest.name
            })
            
    # update weather data into the database
    update_weather(dest_weather_data)
    
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