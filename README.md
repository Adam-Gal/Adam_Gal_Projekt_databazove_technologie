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
