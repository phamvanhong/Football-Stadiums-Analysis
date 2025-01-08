DROP TABLE IF EXISTS fact_footballstadiums;
DROP TABLE IF EXISTS dim_stadiums;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_continent;


CREATE TABLE dim_continent (
    continent_id VARCHAR(10) PRIMARY KEY,
    continent_name VARCHAR(255)
);
CREATE TABLE dim_country (
    country_id VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(255),
    continent_id VARCHAR(10),
    FOREIGN KEY (continent_id) REFERENCES Dim_Continent(continent_id)
);
CREATE TABLE dim_stadiums (
    stadium_id INT PRIMARY KEY,
    stadium_name VARCHAR(255),
	home_teams TEXT,
    capacity INT,
    city VARCHAR(255)
);
CREATE TABLE fact_footballstadiums (
    stadium_id INT PRIMARY KEY,
    country_id VARCHAR(10),
    continent_id VARCHAR(10),
    FOREIGN KEY (country_id) REFERENCES Dim_Country(country_id),
    FOREIGN KEY (continent_id) REFERENCES Dim_Continent(continent_id)
);
