import chess.pgn
import requests
from tqdm import tqdm

# pgn_path = "data/test.pgn"
pgn_path = "data/lichess_db_standard_rated_2013-02.pgn"
pgn = open(pgn_path)
num_games = len(pgn.read().split("[Event ")) - 1
pgn.close()
pgn = open(pgn_path)
base_url = "https://lichess.org/api/users"
output_file = open("data/output_batch.pgn", "a")
pbar = tqdm(total=num_games)

players_batch = []
games_batch = []


def augment():
    global players_batch
    global games_batch
    data_users = requests.post(url=base_url, data=",".join(players_batch)).json()

    for elem in games_batch:
        has_location_white = False
        has_location_black = False
        data_white = next(
            (x for x in data_users if x["username"] == elem.headers["White"]), {}
        )
        data_black = next(
            (x for x in data_users if x["username"] == elem.headers["Black"]), {}
        )
        if "profile" in data_white and "country" in data_white["profile"]:
            has_location_white = True
            elem.headers["WhiteCountry"] = data_white["profile"]["country"]
        if "profile" in data_black and "country" in data_black["profile"]:
            has_location_black = True
            elem.headers["BlackCountry"] = data_black["profile"]["country"]

        if has_location_white or has_location_black:
            output_file.write(str(elem) + "\n\n")

    players_batch = []
    games_batch = []


game = chess.pgn.read_game(pgn)
while True:
    # last batch will have less than 300 players (probably)
    if game is None and len(games_batch) > 0:
        augment()
        break
    games_batch.append(game)
    if game.headers["White"] != "?":
        players_batch.append(game.headers["White"])
    if game.headers["Black"] != "?":
        players_batch.append(game.headers["Black"])

    # lichess api allows up to 300 ids per request
    if len(players_batch) >= 300:
        augment()

    game = chess.pgn.read_game(pgn)
    pbar.update(1)

pbar.close()
