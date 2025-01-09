# Adam Gal Projekt MovieLens

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake pre spracovanie MovieLens datasetu. Projekt je zameraný na analýzu správania používateľov a ich preferencií pri výbere filmov, pričom vychádza z hodnotení a demografických údajov používateľov. Výsledný dátový model umožňuje vizualizáciu kľúčových metrik a multidimenzionálnu analýzu.

---
## 1. Úvod a popis zdrojových dát
Zamerianie semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy vo filmových preferenciách, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z datasetu dostupného [tu](https://grouplens.org/datasets/movielens/). Dataset obsahuje osem hlavných tabuliek:

- `age_group`
- `genres`
- `genres_movies`
- `movies`
- `occupations`
- `tags`
- `ratings`
- `users`

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

---
### 1.1 Dátová architektúra
### ERD diagram

Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na entitno-relačnom diagrame (ERD):
<p align="center">
  <img src="https://github.com/Adam-Gal/Adam_Gal_Projekt_databazove_technologie/blob/main/MovieLens_ERD.png">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---
## 2. Dimenzionálny model
Bol navrhnutý hviezdicový model, ktorý umožňuje efektívnu analýzu dát. Centrálnym bodom modelu je faktová tabuľka fact_ratings, ktorá je prepojená s nasledujúcimi dimenziami:

- `dim_movies:` Obsahuje podrobné informácie o filmoch, ako sú názov, rok vydania, dĺžka filmu, žánre a značky (tags).
- `dim_users:` Obsahuje demografické údaje používateľov, ako sú vek, pohlavie, PSČ, povolanie a vekové kategórie.
- `dim_date:` Zahŕňa informácie o dátumoch hodnotení, vrátane dňa, mesiaca, roka, dňa v týždni a názvov mesiacov.
- `dim_time:` Poskytuje detailné časové údaje, ako sú hodiny, minúty a formát AM/PM.
- `dim_genres:` Obsahuje jedinečné žánre pre kategorizáciu filmov.
- `dim_tags:` Obsahuje značky (tags), ktoré môžu byť použité na analýzu sentimentu alebo dodatočnú kategorizáciu filmov.

Štruktúra hviezdicového modelu je znázornená na priloženom diagrame. Diagram ukazuje vzťahy medzi faktovou tabuľkou fact_ratings a jednotlivými dimenziami, čím sa uľahčuje analýza a vizualizácia dát. Tento návrh zabezpečuje prehľadnú štruktúru dát, čo umožňuje rýchlu a flexibilnú analýzu hodnotení filmov.

<p align="center">
  <img src="https://github.com/Adam-Gal/Adam_Gal_Projekt_databazove_technologie/blob/main/MovieLens_ETL.png">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre AmazonBooks</em>
</p>

---
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
---
### 3.2 Transform (Transformácia dát)

V tejto fáze vytvoríme dimenzie a faktovú tabuľku. Dimenzie poskytujú kontext, zatiaľ čo faktová tabuľka obsahuje kľúčové metriky.

#### Vytvorenie dimenzií:
Dimenzia `dim_users` - Táto dimenzia obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia, PSČ a zamestnania. Transformácia zahŕňala rozdelenie veku používateľov do kategórií, ako napríklad „18-24“, čo umožňuje detailnejšie demografické analýzy. Taktiež boli pridané popisy zamestnaní, ktoré umožňujú segmentáciu používateľov podľa profesií.

Táto dimenzia je typu SCD Typ 2, čo umožňuje sledovanie historických zmien, napríklad v zamestnaní alebo vekovej kategórii používateľov.

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

Dimenzia `dim_movies` - Obsahuje údaje o filmoch, ako sú názov, rok vydania a priemerné hodnotenie. Rok vydania bol extrahovaný z dostupných údajov a priemerné hodnotenie bolo vypočítané ako agregát hodnotení jednotlivých filmov. Táto dimenzia je typu SCD Typ 0, keďže údaje o filmoch, ako názov a autor, sú považované za nemenné.

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

Dimenzia `dim_tags` - Sú v nej jedinečné značky (tags), ktoré sú používané na klasifikáciu filmov a slov použivaných používateľmi. Tieto údaje umožňujú analýzu a filtrovanie filmov na základe ich špecifických vlastností. Dimenzia je typu SCD Typ 0, keďže značky sú statické.


Dimenzia `dim_time` - Uchováva údaje o čase hodnotenia filmov, ako sú hodiny, minúty a denné obdobie (AM/PM). Tento formát umožňuje analýzu časových trendov a preferencií divákov podľa dennej doby.


Dimenzia: `dim_date` - Dimenzia dim_date je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení filmov. Obsahuje odvodené údaje, ako sú deň, mesiac, rok, mesiac v textovom formáte a deň v týždni v textovom. Táto dimenzia je štruktúrovaná ako SCD Typ 0 a umožňuje sezónne analýzy trendov hodnotení.

Dimenzia `dim_genres` - Táto dimenzia obsahuje údaje o žánroch filmov. Žánre boli extrahované zo staging tabuliek a klasifikované ako jedinečné záznamy. Táto dimenzia umožňuje analýzu preferencií divákov podľa kategórií filmov.

```sql
CREATE OR REPLACE TABLE dim_genres AS
SELECT
    ROW_NUMBER() OVER (ORDER BY genres_staging.name) AS dim_genres_id,
    genres_staging.name AS genre
FROM (
    SELECT DISTINCT name
    FROM genres_staging
) genres_staging;
```

Vytvorenie faktovej tabuľky `fact_ratings` - Faktová tabuľka obsahuje záznamy o hodnoteniach filmov a prepojenia na všetky dimenzie. Zahŕňa údaje, ako je hodnota hodnotenia, čas hodnotenia a väzby na používateľov, filmy a časové atribúty. Táto tabuľka je kľúčová pre analýzu výkonu filmov a preferencií divákov.

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
---
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
---
## 4 Vizualizácia dát

<p align="center">
  <img src="https://github.com/Adam-Gal/Adam_Gal_Projekt_databazove_technologie/blob/main/Dashboard%20MovieLens.png">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>

---
### Graf 1: Hodnotenia podľa žánru a pohlavia
Táto vizualizácia zobrazuje počet hodnotení rozdelených podľa žánru a pohlavia používateľov. Umožňuje identifikovať, ktoré žánre sú populárnejšie medzi mužmi alebo ženami. Podľa grafu vidíme že muži najviac hodnotili žánre ako `Drama`, `Action`, `Comedy` a ženy najviac hodnotili `Comedy`, `Drama`. Tento prehľad môže byť užitočný pri tvorbe cielenej marketingovej stratégie pre jednotlivé žánre.

```sql
SELECT
    gender,
    genre,
    COUNT(fact_rating_id) AS rating_count
FROM fact_ratings
JOIN dim_users ON fact_ratings.dim_user_id = dim_users.dim_user_id
JOIN dim_genres ON fact_ratings.dim_genres_id = dim_genres.dim_genres_id
GROUP BY gender, genre
ORDER BY gender, rating_count DESC;
```
---
### Graf 2: Priemerné hodnotenia filmov podľa roku vydania
Graf ukazuje, ako sa priemerné hodnotenie filmov mení podľa roku ich vydania. Vizualizácia umožňuje odhaliť trendy vo filmovej kvalite alebo preferenciách používateľov v priebehu rokov. Môžeme si všimnúť, že filmy z posledných rokov majú nižšie hodnotenia, čo môže naznačovať zhoršenie produkčnej kvality alebo zmenu hodnotiacich kritérií.

```sql
SELECT release_year, AVG(avg_rating) AS avg_rating
FROM dim_movies
GROUP BY release_year
ORDER BY release_year;
```
---
### Graf 3: Aktivita používateľov počas hodín dňa
Graf znázorňuje, ako sa mení počet hodnotení počas dňa. Ukazuje, kedy sú používatelia najviac aktívni pri hodnotení obsahu. Z údajov môže byť zrejmé, že najvyššia aktivita je zaznamenaná ráno o 4, 7 a 8 hodine alebo večer o 23 hodine. Tieto informácie môžu pomôcť lepšie načasovať marketingové kampane alebo zverejňovanie obsahu.

```sql
SELECT
    hour,
    COUNT(fact_rating_id) AS rating_count
FROM fact_ratings
JOIN dim_time ON fact_ratings.dim_time_id = dim_time.dim_time_id
GROUP BY hour
ORDER BY hour;
```
---
### Graf 4: Priemerné hodnotenie podľa mesiacov
Táto vizualizácia zobrazuje priemerné hodnotenia rozdelené podľa mesiacov v roku. Graf umožňuje identifikovať, či existujú sezónne trendy v hodnoteniach, napríklad vyššie hodnotenia zimných mesiacoch, keď sú ľudia viacej zavretý doma, aj keď najvyššie hodnotenia boli zaznamenané v Máji. Tento prehľad môže byť užitočný pri plánovaní premiér alebo promo kampaní.

```sql
SELECT
    month_string,
    AVG(rating) AS avg_rating
FROM fact_ratings
JOIN dim_date ON fact_ratings.dim_date_id = dim_date.dim_date_id
GROUP BY month_string, month
ORDER BY month;
```
---
### Graf 5: Priemerné hodnotenia podľa žánru a vekových skupín
Graf poskytuje prehľad o tom, ako jednotlivé vekové skupiny hodnotia rôzne žánre. Vizualizácia môže odhaliť, že mladšie vekové skupiny preferujú napríklad `Romace`, `Animation`, `Drama`, `History`, `Musical`, zatiaľ čo starší používatelia môžu uprednostňovať `Sci-fy`, `Drama`, `War`, `Action`, `Western`. Tieto údaje môžu byť využité na personalizáciu odporúčaní a lepšie pochopenie preferencií rôznych vekových kategórií.

```sql
SELECT
    dim_genres.genre,
    dim_users.age_group,
    AVG(fact_ratings.rating) AS avg_rating
FROM fact_ratings
JOIN dim_users ON fact_ratings.dim_user_id = dim_users.dim_user_id
JOIN dim_genres ON fact_ratings.dim_genres_id = dim_genres.dim_genres_id
GROUP BY dim_genres.genre, dim_users.age_group
ORDER BY dim_genres.genre, dim_users.age_group;
```

---
### Vytvoril Adam Gál
