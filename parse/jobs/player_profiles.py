import berserk
from pandas import concat, read_parquet, Series
from ratelimiter import RateLimiter
from tqdm import tqdm
from typing import Dict

from parse import PARSING_DIRECTORY

# See: https://lichess.org/api#section/Introduction/Authentication
API_TOKEN = 'Enter Your Lichess API Token here'

class PlayerAPI:
    def __init__(self, token: str = API_TOKEN,
                 known_players_parquet: str = "known_players.parquet.gzip"):
        # Preparation: Try to load known players and set client
        self.parquet_path = PARSING_DIRECTORY / f'data/output/players/{known_players_parquet}'
        try:
            known_df = read_parquet(self.parquet_path)
            self.known_players = known_df.set_index("player").to_dict(orient="index")
        except:
            self.known_players = {}

        session = berserk.TokenSession(token)
        self.client = berserk.Client(session=session)


    def update_known_players(self, file_name: str = 'test.parquet.gzip'):
        # Load file with new player data and create ratelimiter
        parquet_path = PARSING_DIRECTORY / f'data/output/players/{file_name}'
        df = read_parquet(parquet_path)
        player_df = df.groupby('player')['matched_name'].count().reset_index().drop('matched_name', axis=1)

        lichess_rate_limiter = RateLimiter(max_calls=10, period=1)

        # Try to get the player profiles from the lichess API (Subject to the rate limit)
        tqdm.pandas(desc="Requesting player profiles")
        new_player_df = player_df.progress_apply(lambda player_row:
                                                 self._get_public_profile(player_row=player_row,
                                                                          client=self.client,
                                                                          known_players=self.known_players,
                                                                          rate_limiter=lichess_rate_limiter),
                                                 axis=1, result_type='expand')

        # Save the output
        concat([player_df, new_player_df], axis=1).to_parquet(self.parquet_path, compression='gzip')



    @staticmethod
    def _get_public_profile(
        player_row: Series,
        client: berserk.clients.Client,
        known_players: Dict[str, Dict],
        rate_limiter: RateLimiter,
    ) -> Dict:
        """
        Return the profile dictionary of a lichess player from their name.
        The information about known players is reused.
        Requests to the lichess API are subject to the limits set in rate_limiter

        :param player_row:         Row of a DataFrame corresponding to a player
        :param client:              Instance of berserk.clients.Client (lichess' API client)
        :param known_players:       Dictionary of PLAYER_NAME: profile_dictionary
        :param rate_limiter:        Instance of ratelimiter.RateLimiter for API requests
        :return:                    The profile dict of a player (if available)
                                    Potential keys:
                                    - 'location'
                                    - 'country',
                                    - 'firstName'
                                    - 'lastName'
                                    - 'fideRating'
                                    - 'bio'
                                    - 'links'
        """
        player_name = player_row['player']
        if player_name is None or player_name == '?':
            return {}
        if player_name not in known_players:
            with rate_limiter:
                known_players[player_name] = client.users.get_public_data(player_name).get('profile', {})
        return known_players[player_name]


def run(file_path: str):
    print("\n" + "-" * 50)
    print(f"Updating known players based on {file_path}")
    print("-" * 50)
    api = PlayerAPI()
    api.update_known_players(file_name=file_path)

