-- CREATE MATERIALIZED VIEW TO EASY FOR QUERING
CREATE MATERIALIZED VIEW football_stadiums AS
SELECT 
	F.stadium_id, 
	DS.stadium, 
	DS.capacity, 
	F.home_teams, 
	DS.city, 
	DC.country, 
	DC.country_id, 
	DCT.continent, 
	DCT.continent_id
FROM fact_footballstadiums F
LEFT JOIN dim_stadiums DS
ON F.stadium_id = DS.stadium_id
LEFT JOIN dim_country DC
ON F.country_id = DC.country_id
LEFT JOIN dim_continent DCT
ON F.continent_id = DCT.continent_id

-- Top 10 stadiums by capacity
SELECT stadium_id, stadium, capacity
FROM football_stadiums
ORDER BY capacity DESC
LIMIT 10;

-- Top 5 countries have the largest number of stadiums
SELECT country, country_id, COUNT(stadium) AS number_of_stadiums
FROM football_stadiums
GROUP BY country, country_id
ORDER BY number_of_stadiums DESC
LIMIT 5;

-- The number of stadiums in each continent
SELECT continent, continent_id, COUNT(stadium) AS number_of_stadiums
FROM football_stadiums
GROUP BY continent, continent_id
ORDER BY number_of_stadiums DESC;

-- The number of stadiums in each country
SELECT country, country_id, COUNT(stadium) AS number_of_stadiums
FROM football_stadiums
GROUP BY country, country_id
ORDER BY number_of_stadiums DESC;

-- The stadiums have capacity above average
SELECT stadium_id, stadium, capacity
FROM football_stadiums
WHERE capacity > (SELECT avg(capacity) FROM football_stadiums)
ORDER BY capacity DESC;

--  Top 5 stadiums in each continent by capacity
WITH RankedStadiums AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY continent ORDER BY capacity DESC) AS rank
    FROM football_stadiums
)
SELECT continent, stadium_id, stadium, capacity, city, country
FROM RankedStadiums
WHERE rank <= 5
ORDER BY continent, rank;

-- The largest stadiums of each country
WITH RankedStadiums AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY country ORDER BY capacity DESC) AS rank
    FROM football_stadiums
)
SELECT country, continent, stadium_id, stadium, capacity, city
FROM RankedStadiums
WHERE rank = 1
ORDER BY country;

