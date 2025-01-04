# Adam Gal Projekt MovieLens

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre spracovanie MovieLens datasetu. Projekt je zameraný na analýzu správania používateľov a ich preferencií pri výbere filmov, pričom vychádza z hodnotení a demografických údajov používateľov. Výsledný dátový model umožňuje vizualizáciu kľúčových metrik a multidimenzionálnu analýzu.

#
## 1. Úvod a popis zdrojových dát
Zamerianie semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy vo filmových preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z MovieLens datasetu dostupného tu. Dataset obsahuje štyri hlavné tabulky:

- `age_group`
- `genres`
- `genres_movies`
- `movies`
- `occupations`
- `tags`
- `ratings`
- `users`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.
#

### 1.1 Dátová architektúra
ERD diagram
Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na entitno-relačnom diagrame (ERD):
![MovieLens_ERD](https://github.com/user-attachments/assets/b95ddf0a-c2b3-45ce-9997-c982164a0890)
<em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
#
## 2. Dimenzionálny model
Bol navrhnutý hviezdicový model, ktorý umožňuje efektívnu analýzu dát. Centrálnym bodom modelu je faktová tabuľka fact_ratings, ktorá je prepojená s nasledujúcimi dimenziami:

- `dim_movies:` Obsahuje podrobné informácie o filmoch, ako sú názov, rok vydania, dĺžka filmu, žánre a značky (tags).
- `dim_users:` Obsahuje demografické údaje používateľov, ako sú vek, pohlavie, PSČ, povolanie a vekové kategórie.
- `dim_date:` Zahŕňa informácie o dátumoch hodnotení, vrátane dňa, mesiaca, roka, dňa v týždni a názvov mesiacov.
- `dim_time:` Poskytuje detailné časové údaje, ako sú hodiny, minúty a formát AM/PM.
- `dim_genres:` Obsahuje jedinečné žánre pre kategorizáciu filmov.
- `dim_tags:` Obsahuje značky (tags), ktoré môžu byť použité na analýzu sentimentu alebo dodatočnú kategorizáciu filmov.

Štruktúra hviezdicového modelu je znázornená na priloženom diagrame. Diagram ukazuje vzťahy medzi faktovou tabuľkou fact_ratings a jednotlivými dimenziami, čím sa uľahčuje analýza a vizualizácia dát. Tento návrh zabezpečuje prehľadnú štruktúru dát, čo umožňuje rýchlu a flexibilnú analýzu hodnotení filmov.

![MovieLens_ETL](https://github.com/user-attachments/assets/11195878-934e-4aa2-ae0d-91c250691153)
#
## 3. ETL proces v Snowflake
### 3.1 Extract (Extrahovanie dát)

Najskôr nahráme dáta do Snowflake prostredníctvom interného stage úložiska my_stage. Dáta sa importujú do staging tabuliek.

#### Kód pre vytvorenie stage:
```sql
CREATE OR REPLACE STAGE spider_stage;
```
#### Príklad vytvárania stage tabuliek:

```sql
CREATE OR REPLACE TABLE users_staging (
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(25),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);
```

#### Nahratie a kopírovanie dát do staging tabuliek:

Pre každú tabuľku použijeme príkaz COPY INTO. Príklad pre tabuľku users_staging:

```sql
COPY INTO users_staging
FROM @spider_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```

### 3.2 Transform (Transformácia dát)

V tejto fáze vytvoríme dimenzie a faktovú tabuľku. Dimenzie poskytujú kontext, zatiaľ čo faktová tabuľka obsahuje kľúčové metriky.

#### Vytvorenie dimenzií:
##### Dimenzia: `dim_users` obsahuje údaje o používateľoch vrátane vekovej skupiny, pohlavia a zamestnania:

```sql
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
```

##### Dimenzia: `dim_movies` obsahuje údaje o filmoch vrátane názvu, roku vydania a priemerného hodnotenia:

```sql
CREATE OR REPLACE TABLE dim_movies AS
SELECT
    movies_staging.id AS dim_movie_id,
    movies_staging.title AS title,
    movies_staging.release_year AS release_year,
    AVG(ratings_staging.rating) AS avg_rating
FROM movies_staging
LEFT JOIN ratings_staging ON movies_staging.id = ratings_staging.movie_id
GROUP BY movies_staging.id, movies_staging.title, movies_staging.release_year;
```

##### Dimenzia: `dim_tags` obsahuje údaje o značkách (tags), ktoré sú používané na označovanie filmov.

```sql
CREATE OR REPLACE TABLE dim_tags AS
SELECT
    tags_staging.id AS dim_tags_id,
    tags_staging.tags AS tag
FROM tags_staging;
```

##### Dimenzia: `dim_time` obsahuje údaje o čase hodnotení filmov vrátane hodín, minút a dennej časti (AM/PM).

```sql
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
```

##### Dimenzia: `dim_date` obsahuje údaje o dátumoch:

```sql
CREATE OR REPLACE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_date_id,
    CAST(rated_at AS DATE) AS timestamp,
    DATE_PART(year, rated_at) AS year,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(day, rated_at) AS day,
    DATE_PART(dow, rated_at) + 1 AS day_of_week,
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_string,
    CASE DATE_PART(month, rated_at)
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
FROM ratings_staging;
```
##### Dimenzia: `dim_genres` obsahuje údaje o žánroch:

```sql
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    genres_staging.id AS dim_genres_id,
    genres_staging.name AS genre
FROM genres_staging;
```

##### Vytvorenie faktovej tabuľky: `fact_ratings`:

```sql
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
```

### 3.3 Load (Načítanie dát)
#### Po úspešnom vytvorení dimenzií a faktovej tabuľky môžeme staging tabuľky odstrániť:

```sql
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS ratings_staging;
```
