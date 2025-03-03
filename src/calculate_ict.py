import pandas as pd

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
def calculate_ICT(df):
    results = []
    
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