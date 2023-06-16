# Parsing Lichess games
## Preparation
1. Get the opening data from [lichess' opening repo](https://github.com/lichess-org/chess-openings/) 
2. After cloning, run `make` in the repository to get the expanded `.tsv` files.
3. Copy the `.tsv` files from the `dist` to [data/parse](/data/parse)

## Jobs
From the top-level directory (`map-of-online-chess`), run the following commands in order. The examples are based on `test.pgn` that can be replaced for any lichess output.

1. Clean the pgn file for spark processing. E.g.:
```
.src/parse/scripts/rewrite_pgn.sh ./data/parse/pgn/test.pgn ./data/parse/pgn/test_cleaned.pgn
```

2. games.py - To extract opening data per game. E.g.:
```
python3 -m src.parse.jobs.games --pgn_file test_cleaned.pgn
```

3. player_openings - To count number of openings played per player. E.g.:
```
python3 -m src.parse.jobs.player_openings --parquet_file test_cleaned.parquet.gzip
```
4. player_profiles - Get profile dictionary for all players in output of player_openings. E.g.:
```
python3 -m src.parse.jobs.player_profiles --token YOUR_LICHESS_API_TOKEN --parquet_file test_cleaned.parquet.gzip
```

## Notes
- Games: https://database.lichess.org/
- List of openings: https://github.com/lichess-org/chess-openings/tree/64b26a2ca37659cdc3e87e181a8844db64aee7b9
  - Clone and run `make` to get UCI notation
- Use the [berserk](https://github.com/lichess-org/berserk) client by lichess to request user data