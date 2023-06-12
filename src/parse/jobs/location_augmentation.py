import chess.pgn
import requests
from tqdm import tqdm
import time
import json
import os

dirname = os.path.dirname(__file__)
pgn_path = os.path.join(
    dirname, "../../data/lichess/lichess_db_standard_rated_2013-02.pgn"
)
pgn = open(pgn_path)
num_games = len(pgn.read().split("[Event ")) - 1
pgn.close()
pgn = open(pgn_path)
base_url = "https://lichess.org/api/users"
output_file = open(
    os.path.join(dirname, "../data/augmented/output_batch_test.pgn"), "a"
)
pbar = tqdm(total=num_games)

players_batch = []
games_batch = []
player_data_buffer = []

with open(os.path.join(dirname, "../../../data/players/player_data_lookup.json")) as f:
    # player_data_lookup = json.load(f)
    player_data_lookup = []


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
            if not "buffer" in data_white:
                player_data_lookup.append(
                    {
                        "username": elem.headers["White"],
                        "profile": {"country": data_white["profile"]["country"]},
                    }
                )
        else:
            if not "buffer" in data_white:
                player_data_lookup.append({"username": elem.headers["White"]})

        if "profile" in data_black and "country" in data_black["profile"]:
            has_location_black = True
            elem.headers["BlackCountry"] = data_black["profile"]["country"]
            if not "buffer" in data_black:
                player_data_lookup.append(
                    {
                        "username": elem.headers["Black"],
                        "profile": {"country": data_black["profile"]["country"]},
                    }
                )
        else:
            if not "buffer" in data_black:
                player_data_lookup.append({"username": elem.headers["Black"]})

        if has_location_white or has_location_black:
            output_file.write(str(elem) + "\n\n")

    players_batch = []
    games_batch = []
    player_data_buffer = []

    with open(
        os.path.join(dirname, "../../../data/players/player_data_lookup.json", "w")
    ) as f:
        json.dump(player_data_lookup, f)


def processPlayer(username: str):
    global players_batch
    global games_batch
    global player_data_buffer

    # ? means the account doesnt exist anymore
    if username != "?":
        if not username in players_batch:
            player_data = next(
                (x for x in player_data_lookup if x["username"] == username),
                None,
            )
            if player_data is None:
                players_batch.append(username)
            else:
                if (
                    next(
                        (x for x in player_data_buffer if x["username"] == username),
                        None,
                    ),
                ) is None:
                    # flag for augmentation step to avoid another search in lookup
                    player_data["buffer"] = True
                    player_data_buffer.append(player_data)


game = chess.pgn.read_game(pgn)
while True:
    # last batch will have less than 300 players (probably)
    if game is None and len(games_batch) > 0:
        augment()
        break
    games_batch.append(game)

    processPlayer(game.headers["White"])
    processPlayer(game.headers["Black"])

    # lichess api allows up to 300 ids per request
    if len(players_batch) >= 300:
        augment()

    game = chess.pgn.read_game(pgn)
    pbar.update(1)

pbar.close()
