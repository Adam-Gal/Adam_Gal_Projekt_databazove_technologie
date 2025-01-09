-- Vytvorenie databázy
CREATE OR REPLACE DATABASE SPIDER_MovieLens;
USE SPIDER_MovieLens;

-- Vytvorenie schémy pre staging tabuľky
CREATE OR REPLACE SCHEMA SPIDER_MovieLens.staging;

USE SCHEMA SPIDER_MovieLens.staging;

CREATE OR REPLACE STAGE spider_stage;

-- Staging tabuľky pre načítanie surových dát
CREATE OR REPLACE TABLE age_group_staging (
    id INT PRIMARY KEY,
    name VARCHAR(45)
);

CREATE OR REPLACE TABLE occupations_staging (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE OR REPLACE TABLE users_staging (
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(25),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);

CREATE OR REPLACE TABLE movies_staging (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

CREATE OR REPLACE TABLE genres_staging (
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE OR REPLACE TABLE genres_movies_staging (
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(id)
);

CREATE OR REPLACE TABLE tags_staging (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at DATETIME
);

CREATE OR REPLACE TABLE ratings_staging (
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

-- Načítanie údajov z CSV súborov
COPY INTO age_group_staging
FROM @spider_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO occupations_staging
FROM @spider_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO users_staging
FROM @spider_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO movies_staging
FROM @spider_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genres_staging
FROM @spider_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO genres_movies_staging
FROM @spider_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO tags_staging
FROM @spider_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @spider_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- Transformácia dát (ETL)
-- dim_users
CREATE OR REPLACE TABLE dim_users AS
SELECT
    users_staging.id AS dim_user_id,
    users_staging.gender AS gender,
    users_staging.zip_code AS zip_code,
    CASE
        WHEN users_staging.age < 18 THEN 'Under 18'
        WHEN users_staging.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN users_staging.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN users_staging.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN users_staging.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN users_staging.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group,
    occupations_staging.name AS occupation
FROM users_staging
JOIN occupations_staging ON users_staging.occupation_id = occupations_staging.id;

-- dim_movies
CREATE OR REPLACE TABLE dim_movies AS
SELECT
    movies_staging.id AS dim_movie_id,
    movies_staging.title AS title,
    movies_staging.release_year AS release_year,
    AVG(ratings_staging.rating) AS avg_rating
FROM movies_staging
LEFT JOIN ratings_staging ON movies_staging.id = ratings_staging.movie_id
GROUP BY movies_staging.id, movies_staging.title, movies_staging.release_year;

-- dim_tags
CREATE OR REPLACE TABLE dim_tags AS
SELECT
    ROW_NUMBER() OVER (ORDER BY tags_staging.tags) AS dim_tags_id,
    tags_staging.tags AS tag
FROM (
    SELECT DISTINCT tags
    FROM tags_staging
) tags_staging;

-- dim_time
CREATE OR REPLACE TABLE dim_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS TIME)) AS dim_time_id,
    CAST(rated_at AS TIME) AS timestamp,
    DATE_PART(hour, rated_at) AS hour,
    CASE
        WHEN DATE_PART(hour, rated_at) < 12 THEN 'AM'
        ELSE 'PM'
    END AS am_pm
FROM (SELECT DISTINCT CAST(rated_at AS TIME) AS rated_at FROM ratings_staging);

-- dim_date
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY unique_date) AS dim_date_id,
    unique_date AS timestamp,
    DATE_PART(year, unique_date) AS year,
    DATE_PART(month, unique_date) AS month,
    DATE_PART(day, unique_date) AS day,
    DATE_PART(dow, unique_date) + 1 AS day_of_week,
    CASE DATE_PART(dow, unique_date) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_string,
    CASE DATE_PART(month, unique_date)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_string
FROM (
    SELECT DISTINCT CAST(rated_at AS DATE) AS unique_date
    FROM ratings_staging
) subquery;

-- dim_genres
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    ROW_NUMBER() OVER (ORDER BY genres_staging.name) AS dim_genres_id,
    genres_staging.name AS genre
FROM (
    SELECT DISTINCT name
    FROM genres_staging
) genres_staging;

-- fact_ratings
CREATE OR REPLACE TABLE fact_ratings AS
SELECT
    ratings_staging.id AS fact_rating_id,
    ratings_staging.rating AS rating,
    dim_users.dim_user_id AS dim_user_id,
    ratings_staging.movie_id AS dim_movie_id,
    dim_time.dim_time_id AS dim_time_id,
    dim_date.dim_date_id AS dim_date_id,
    dim_genres.dim_genres_id AS dim_genres_id,
    dim_tags.dim_tags_id AS dim_tags_id
FROM ratings_staging
JOIN dim_users ON ratings_staging.user_id = dim_users.dim_user_id
JOIN dim_date ON CAST(ratings_staging.rated_at AS DATE) = dim_date.timestamp
JOIN dim_time ON CAST(ratings_staging.rated_at AS TIME) = dim_time.timestamp
JOIN genres_movies_staging ON ratings_staging.movie_id = genres_movies_staging.movie_id
JOIN dim_genres ON genres_movies_staging.genre_id = dim_genres.dim_genres_id
JOIN tags_staging ON ratings_staging.id = tags_staging.movie_id
JOIN dim_tags ON tags_staging.id = dim_tags.dim_tags_id;


-- Odstránenie staging tabuliek
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS ratings_staging;




