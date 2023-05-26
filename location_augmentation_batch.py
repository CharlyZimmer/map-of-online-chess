import chess.pgn
import requests
from tqdm import tqdm
import time
import json

pgn_path = "data/lichess_db_standard_rated_2013-02.pgn"
pgn = open(pgn_path)
num_games = len(pgn.read().split("[Event ")) - 1
pgn.close()
pgn = open(pgn_path)
base_url = "https://lichess.org/api/users"
output_file = open("data/output_batch_test.pgn", "a")
pbar = tqdm(total=num_games)

players_batch = []
games_batch = []
player_data_buffer = []

with open("player_data_lookup.json") as f:
    player_data_lookup = json.load(f)


def augment():
    global players_batch
    global games_batch
    global player_data_buffer

    response = requests.post(url=base_url, data=",".join(players_batch))

    # api throttle
    while response.status_code == 429:
        for i in range(11 * 60, 0, -1):
            print(f"{i}", end="\r", flush=True)
            time.sleep(1)
        response = requests.post(url=base_url, data=",".join(players_batch))

    data_users = response.json()
    data_users = data_users + player_data_buffer

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
            if (
                next(
                    (
                        x
                        for x in player_data_lookup
                        if x["username"] == elem.headers["White"]
                    ),
                    None,
                )
                is None
            ):
                player_data_lookup.append(
                    {
                        "username": elem.headers["White"],
                        "profile": {"country": data_white["profile"]["country"]},
                    }
                )
        else:
            if (
                next(
                    (
                        x
                        for x in player_data_lookup
                        if x["username"] == elem.headers["White"]
                    ),
                    None,
                )
                is None
            ):
                player_data_lookup.append({"username": elem.headers["White"]})
        if "profile" in data_black and "country" in data_black["profile"]:
            has_location_black = True
            elem.headers["BlackCountry"] = data_black["profile"]["country"]
            if (
                next(
                    (
                        x
                        for x in player_data_lookup
                        if x["username"] == elem.headers["Black"]
                    ),
                    None,
                )
                is None
            ):
                player_data_lookup.append(
                    {
                        "username": elem.headers["Black"],
                        "profile": {"country": data_black["profile"]["country"]},
                    }
                )
        else:
            if (
                next(
                    (
                        x
                        for x in player_data_lookup
                        if x["username"] == elem.headers["Black"]
                    ),
                    None,
                )
                is None
            ):
                player_data_lookup.append({"username": elem.headers["Black"]})

        if has_location_white or has_location_black:
            output_file.write(str(elem) + "\n\n")

    players_batch = []
    games_batch = []
    player_data_buffer = []

    with open("player_data_lookup.json", "w") as f:
        json.dump(player_data_lookup, f)


game = chess.pgn.read_game(pgn)
while True:
    # last batch will have less than 300 players (probably)
    if game is None and len(games_batch) > 0:
        augment()
        break
    games_batch.append(game)
    # only add if the account still exists
    if game.headers["White"] != "?":
        if not game.headers["White"] in players_batch:
            player_data = next(
                (
                    x
                    for x in player_data_lookup
                    if x["username"] == game.headers["White"]
                ),
                None,
            )
            if player_data is None:
                players_batch.append(game.headers["White"])
            else:
                player_data_buffer.append(player_data)

    if game.headers["Black"] != "?":
        if not game.headers["Black"] in players_batch:
            player_data = next(
                (
                    x
                    for x in player_data_lookup
                    if x["username"] == game.headers["Black"]
                ),
                None,
            )
            if player_data is None:
                players_batch.append(game.headers["Black"])
            else:
                player_data_buffer.append(player_data)

    # lichess api allows up to 300 ids per request
    if len(players_batch) >= 300:
        augment()

    game = chess.pgn.read_game(pgn)
    pbar.update(1)

pbar.close()
