from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from airflow.models import Variable
import psycopg2
import pandas as pd

default_args = {
    'owner': 'meteotrack8',
    'start_date': datetime(2025, 2, 28, 00, 00)
}

class Destination:

    def __init__(self, name, lat, long) -> None:
        self.name = name
        self.lat = lat
        self.long = long

# Define the function to classify CId and CIa based on Effective Temperature (ET) scale
def classify_temperature(temp):
    """
    Converts temperature to a rating from -3 to 5 based on Mieczkowski's comfort scale.
    """
    if 20 <= temp <= 27:
        return 5
    elif 18 <= temp < 20 or 27 < temp <= 29:
        return 4
    elif 16 <= temp < 18 or 29 < temp <= 31:
        return 3
    elif 14 <= temp < 16 or 31 < temp <= 33:
        return 2
    elif 10 <= temp < 14 or 33 < temp <= 35:
        return 1
    elif 5 <= temp < 10 or 35 < temp <= 38:
        return 0
    else:
        return -3  # Very poor conditions

# Function to normalize other indices (P, S, W) between 0-5
def classify_precipitation(rain):
    """Converts precipitation into a rating from 0 to 5."""
    if rain == 0:
        return 5
    elif 0 < rain <= 1:
        return 4
    elif 1 < rain <= 5:
        return 3
    elif 5 < rain <= 10:
        return 2
    elif 10 < rain <= 20:
        return 1
    else:
        return 0

def classify_wind_speed(wind):
    """Converts wind speed into a rating from 0 to 5."""
    if wind < 2:
        return 5
    elif 2 <= wind < 4:
        return 4
    elif 4 <= wind < 6:
        return 3
    elif 6 <= wind < 8:
        return 2
    elif 8 <= wind < 10:
        return 1
    else:
        return 0

# Function to calculate ICT
def calculate_ICT(df_):
    results = []
    df = pd.DataFrame(df_)

    # Reset the index to remove the default integer index
    df.reset_index(drop=True, inplace=True)

    for index, row in df.iterrows():
        CId = classify_temperature(row['temp'])   # Daytime comfort index
        CIa = classify_temperature(row['feel_temp'])  # Daily comfort index (feels like temp)
        P = classify_precipitation(row['rain'] if pd.notna(row['rain']) else 0)  # Rainfall
        S = 4  # Assuming average sunshine rating (should be replaced with actual sunshine data)
        W = classify_wind_speed(row['wind_speed'])  # Wind speed

        # Calculate ICT using the formula
        ICT = 2 * ((4 * CId) + CIa + (2 * P) + (2 * S) + W)
        
        results.append({
            'weather_date': row['weather_date'],
            'destination': row['name_destination'],
            'ICT': ICT
        })

    return pd.DataFrame(results)


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

def update_ict(destination):

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
        upsert_query = f"""INSERT INTO weather (destination) 
                        VALUES 
                            (%s)
                        ON CONFLICT (destination) DO UPDATE
                        SET (ict_6j) = (EXCLUDED.ict_6j)"""
        for dest_ict in destination :
            cursor.execute( upsert_query, (dest_ict[1]["ICT"]))
        
        connection.commit()
    except Exception as error:
        print(f"Error: {error}")
    
    finally :
        # Close the cursor and connection to free up resources
        if cursor :
            cursor.close()
        if connection :
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

    # Calculate ICT scores
    ict_results = calculate_ICT(dest_weather_data)

    # Compute the average ICT for each destination
    average_ict = ict_results.groupby('destination')['ICT'].mean().reset_index()

    update_ict(average_ict)



# delete old weather data (for previous days)
def delete_weather_data() :
    
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
        today = datetime.today().strftime('%Y-%m-%d')
        cursor.execute("DELETE FROM weather WHERE weather_date < %s", (today,))
        
        connection.commit()
        
    except Exception as error:
        print(f"Error : {error}")
    
    finally :
        if cursor :
            cursor.close()
        if connection :
            connection.close() 
#request_api()

with DAG('meteotrack',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='request_api',
        python_callable=request_api
    )
    
    delete_task = PythonOperator(
        task_id="delete_old_weather_data",
        python_callable=delete_weather_data
    )
    
    streaming_task >> delete_task