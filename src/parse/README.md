# Parsing Lichess games
## Jobs

1. games.py - To extract opening data per game
2. player_openings - To count number of openings played per player
3. player_profiles - Get profile dictionary for all players in output of player_openings

## Notes
- Games: https://database.lichess.org/
- List of openings: https://github.com/lichess-org/chess-openings/tree/64b26a2ca37659cdc3e87e181a8844db64aee7b9
  - Clone and run `make` to get UCI notation
- Use the [berserk](https://github.com/lichess-org/berserk) client by lichess to request user data