DROP TABLE IF EXISTS temp_stadiums;

-- CREATE TEMP TABLE TO COPY VALUE TO OTHER TABLES
CREATE TEMP TABLE temp_stadiums (
    stadium_id INT PRIMARY KEY,
    stadium VARCHAR(255),
    capacity INT,
    continent VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255),
    home_teams TEXT,
    country_id VARCHAR(5),
    continent_id VARCHAR(5)
);

-- COPY VALUE FROM READY DATASET TO TEMP TABLE
COPY temp_stadiums
FROM 'F:\\Download\\ready_football_stadiums.csv'
DELIMITER ','
CSV HEADER;



-- INSET VALUE FROM temp_stadiums TO OTHER TABLES
INSERT INTO Dim_Continent (continent_id, continent_name)
SELECT DISTINCT continent_id, continent
FROM temp_stadiums
ON CONFLICT (continent_id) DO NOTHING;

INSERT INTO Dim_Country (country_id, country_name, continent_id)
SELECT DISTINCT country_id, country, continent_id
FROM temp_stadiums
ON CONFLICT (country_id) DO NOTHING;

INSERT INTO Dim_Stadiums (stadium_id, stadium_name, home_teams, capacity, city)
SELECT DISTINCT stadium_id, stadium, home_teams, capacity, city
FROM temp_stadiums
ON CONFLICT (stadium_id) DO NOTHING;

INSERT INTO Fact_Stadiums (stadium_id, country_id, continent_id)
SELECT DISTINCT stadium_id, country_id, continent_id
FROM temp_stadiums
ON CONFLICT (stadium_id) DO NOTHING;

-- DROP TEMP TABLE
DROP TABLE IF EXISTS temp_stadiums;


