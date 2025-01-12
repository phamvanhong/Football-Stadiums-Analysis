-- COPY DATA FROM ready_footballstadiums INTO temp_stadiums TO INSERT VALUE TO OTHER TABLES
-- NOTE: CHANGE YOUR PATH TO DATA IN LOCAL
INSERT INTO temp_stadiums (stadium_id, stadium, capacity, city, home_teams, country, country_id, continent, continent_id)
SELECT stadium_id, stadium, capacity, city, home_teams, country, country_id, continent, continent_id
FROM json_populate_recordset(
  NULL::temp_stadiums, 
  pg_read_file('E:/DataEngineerProject/Football-Stadiums-Analysis/data/GOLD/ready/ready_footballstadiums.json')::json
);

-- INSET VALUE FROM temp_stadiums TO OTHER TABLES
INSERT INTO dim_continent (continent_id, continent_name)
SELECT DISTINCT continent_id, continent
FROM temp_stadiums
ON CONFLICT (continent_id) DO NOTHING;

INSERT INTO dim_country (country_id, country_name, continent_id)
SELECT DISTINCT country_id, country, continent_id
FROM temp_stadiums
ON CONFLICT (country_id) DO NOTHING;

INSERT INTO dim_stadiums (stadium_id, stadium_name, capacity, city)
SELECT DISTINCT stadium_id, stadium, capacity, city
FROM temp_stadiums
ON CONFLICT (stadium_id) DO NOTHING;

INSERT INTO fact_footballstadiums (stadium_id, country_id, continent_id, home_teams)
SELECT DISTINCT stadium_id, country_id, continent_id, home_teams
FROM temp_stadiums
ON CONFLICT (stadium_id) DO NOTHING;

-- DROP TEMP TABLE AFTER INSERT VALUE INTO FACT AND DIMENSIONS
DROP TABLE IF EXISTS temp_stadiums;
