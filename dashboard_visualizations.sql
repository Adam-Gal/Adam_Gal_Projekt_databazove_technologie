-- Graf 1: Hodnotenia podľa žánru a pohlavia
SELECT
    gender,
    genre,
    COUNT(fact_rating_id) AS rating_count
FROM fact_ratings
JOIN dim_users ON fact_ratings.dim_user_id = dim_users.dim_user_id
JOIN dim_genres ON fact_ratings.dim_genres_id = dim_genres.dim_genres_id
GROUP BY gender, genre
ORDER BY gender, rating_count DESC;

-- Graf 2: Priemerné hodnotenia filmov podľa roku vydania
SELECT release_year, AVG(avg_rating) AS avg_rating
FROM dim_movies
GROUP BY release_year
ORDER BY release_year;

-- Graf 3: Aktivita používateľov počas hodín dňa
SELECT
    hour,
    COUNT(fact_rating_id) AS rating_count
FROM fact_ratings
JOIN dim_time ON fact_ratings.dim_time_id = dim_time.dim_time_id
GROUP BY hour
ORDER BY hour;

-- Graf 4: Priemerné hodnotenie podľa mesiacov
SELECT
    month_string,
    AVG(rating) AS avg_rating
FROM fact_ratings
JOIN dim_date ON fact_ratings.dim_date_id = dim_date.dim_date_id
GROUP BY month_string, month
ORDER BY month;

-- Graf 5: Priemerné hodnotenia podľa žánru a vekových skupín
SELECT
    dim_genres.genre,
    dim_users.age_group,
    AVG(fact_ratings.rating) AS avg_rating
FROM fact_ratings
JOIN dim_users ON fact_ratings.dim_user_id = dim_users.dim_user_id
JOIN dim_genres ON fact_ratings.dim_genres_id = dim_genres.dim_genres_id
GROUP BY dim_genres.genre, dim_users.age_group
ORDER BY dim_genres.genre, dim_users.age_group;