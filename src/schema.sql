CREATE TABLE Destination (
    name VARCHAR(255) PRIMARY KEY,
    lat FLOAT NOT NULL,  -- Latitude of the destination
    lon FLOAT NOT NULL   -- Longitude of the destination
);

CREATE TABLE weather (
    weather_date DATE NOT NULL,
    temp FLOAT NOT NULL,
    feel_temp FLOAT,
    humidity INT NOT NULL,
    pressure INT NOT NULL,
    main VARCHAR(255) NOT NULL,
    wind_speed FLOAT NOT NULL,
    rain FLOAT,
    summary TEXT,
    name_destination VARCHAR(255) NOT NULL,
    PRIMARY KEY (weather_date, name_destination),
    FOREIGN KEY (name_destination) REFERENCES Destination(name)
);
