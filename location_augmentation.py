import chess.pgn
import requests
from tqdm import tqdm

pgn = open("data/lichess_db_standard_rated_2013-02.pgn")
# pgn = open("data/test.pgn")
num_games = len(pgn.read().split("[Event ")) - 1
pgn.close()
pgn = open("data/lichess_db_standard_rated_2013-02.pgn")
base_url = "https://lichess.org/api/user/"
output_file = open("data/output.pgn", "a")
pbar = tqdm(total=num_games)

game = chess.pgn.read_game(pgn)
while game is not None:
    data_white = requests.get(url=base_url + game.headers["White"]).json()
    data_black = requests.get(url=base_url + game.headers["Black"]).json()

    has_location_white = False
    has_location_black = False
    if "profile" in data_white and "country" in data_white["profile"]:
        has_location_white = True
        game.headers["WhiteCountry"] = data_white["profile"]["country"]
    if "profile" in data_black and "country" in data_black["profile"]:
        has_location_black = True
        game.headers["BlackCountry"] = data_black["profile"]["country"]

    if has_location_white or has_location_black:
        output_file.write(str(game) + "\n\n")

    game = chess.pgn.read_game(pgn)
    pbar.update(1)

pbar.close()
