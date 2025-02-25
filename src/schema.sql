CREATE TABLE Destination (
    name VARCHAR(255) PRIMARY KEY,
    lat FLOAT NOT NULL,  -- Latitude of the destination
    lon FLOAT NOT NULL   -- Longitude of the destination
);

CREATE TABLE weather (
    day INT NOT NULL,
    humidity INT NOT NULL,
    pressure INT NOT NULL,
    main VARCHAR(255) NOT NULL,
    wind_speed FLOAT NOT NULL,
    rain FLOAT NOT NULL,
    name_destination VARCHAR(255) NOT NULL,
    PRIMARY KEY (day, name_destination),
    FOREIGN KEY (name_destination) REFERENCES Destination(name)
);
