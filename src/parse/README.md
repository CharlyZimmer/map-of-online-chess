# Parsing Lichess games
## Preparation
1. Get the opening data from [lichess' opening repo](https://github.com/lichess-org/chess-openings/) 
2. After cloning, run `make` in the repository to get the expanded `.tsv` files.
3. Copy the `.tsv` files from the `dist` to [data/openings](/data/openings)

## Jobs
From the top-level directory (`map-of-online-chess`), run the following commands in order. The examples are based on `test.pgn` that can be replaced for any lichess database file.

### 1. rewrite_pgn
Clean the pgn file for spark processing:
```
.src/parse/scripts/rewrite_pgn.sh ./data/parse/pgn/test.pgn ./data/parse/pgn/test_cleaned.pgn
```

### 2. games.py 
Extract opening data per game:
```
python3 -m src.parse.jobs.games --pgn_file test_cleaned.pgn
```

### 3. player_openings 
Count number of openings played per player:
```
python3 -m src.parse.jobs.player_openings --parquet_file test_cleaned.parquet.gzip
```
### 4. player_profiles 
Get profile dictionary for all players in output of player_openings:
```
python3 -m src.parse.jobs.player_profiles --token YOUR_LICHESS_API_TOKEN --parquet_file test_cleaned.parquet.gzip
```

### 5. player_probabilities 
Get the probability of playing an opening for each player and add the country information from player_profiles:
```
python3 -m src.parse.jobs.player_probabilities --parquet_file test_cleaned.parquet.gzip
```

### 6. country_openings 
Get the probabilities (and standardized prob) of an opening on country level plus the number of players per country:
(Don't forget the '_prob'-suffix of the file name)
```
python3 -m src.parse.jobs.country_openings --parquet_file test_cleaned_prob.parquet.gzip
```

## Notes
- Games: https://database.lichess.org/
- List of openings: https://github.com/lichess-org/chess-openings/tree/64b26a2ca37659cdc3e87e181a8844db64aee7b9
  - Clone and run `make` to get openings in UCI notation
  - Cop
- Use the [berserk](https://github.com/lichess-org/berserk) client by lichess to request user data