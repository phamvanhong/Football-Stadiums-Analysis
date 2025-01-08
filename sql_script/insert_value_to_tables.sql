-- INSET VALUE FROM temp_stadiums TO OTHER TABLES
INSERT INTO dim_continent (continent_id, continent_name)
SELECT DISTINCT continent_id, continent
FROM temp_stadiums
ON CONFLICT (continent_id) DO NOTHING;

INSERT INTO dim_country (country_id, country_name, continent_id)
SELECT DISTINCT country_id, country, continent_id
FROM temp_stadiums
ON CONFLICT (country_id) DO NOTHING;

INSERT INTO dim_stadiums (stadium_id, stadium_name, home_teams, capacity, city)
SELECT DISTINCT stadium_id, stadium, home_teams, capacity, city
FROM temp_stadiums
ON CONFLICT (stadium_id) DO NOTHING;

INSERT INTO fact_footballstadiums (stadium_id, country_id, continent_id)
SELECT DISTINCT stadium_id, country_id, continent_id
FROM temp_stadiums
ON CONFLICT (stadium_id) DO NOTHING;

-- DROP TEMP TABLE AFTER INSERT VALUE INTO FACT AND DIMENSIONS
DROP TABLE IF EXISTS temp_stadiums;


