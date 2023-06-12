import berserk
import os
from pandas import concat, DataFrame, read_parquet, Series
from ratelimiter import RateLimiter
import time
from tqdm import tqdm
from typing import Dict

from src import DATA_DIRECTORY

class PlayerAPI:
    def __init__(self, token: str,
                 known_players_parquet: str = "known_players.parquet.gzip"):
        '''

        :param token:                   Token for the lichess API (https://lichess.org/api#section/Introduction/Authentication)
        :param known_players_parquet:   Name of the file to load and store known players
        '''
        # Preparation: Try to load known players and set client
        self.parquet_path = DATA_DIRECTORY / f'parse/output/players/{known_players_parquet}'
        try:
            self.known_df = read_parquet(self.parquet_path)
            self.known_players = self.known_df.set_index("player").to_dict(orient="index")
        except:
            self.known_players = {}
            self.known_df = DataFrame()

        session = berserk.TokenSession(token)
        self.client = berserk.Client(session=session)


    def update_known_players(self, new_players_parquet: str = 'test.parquet.gzip'):
        # Load file with new player data and create ratelimiter
        parquet_path = DATA_DIRECTORY / f'parse/output/players/{new_players_parquet}'
        df = read_parquet(parquet_path)
        player_df = df.groupby('player')['matched_name'].count().reset_index().drop('matched_name', axis=1)
        lichess_rate_limiter = RateLimiter(max_calls=10, period=1)

        # Only loop through unknown players
        player_df = player_df.loc[~player_df['player'].isin(self.known_players.keys())]

        # Try to get the player profiles from the lichess API
        temp_path = str(self.parquet_path).replace('.parquet.gzip', '_temp.parquet.gzip')
        for _, player_row in tqdm(iterable=player_df.iterrows(), total=player_df.shape[0],
                                  desc="Updating player profiles"):
            profile_dict = self._get_public_profile(player_row=player_row,
                                                    client=self.client,
                                                    known_players=self.known_players,
                                                    rate_limiter=lichess_rate_limiter)

            # Save the known player parquet after each iteration
            self.known_df = concat([self.known_df, DataFrame.from_records([profile_dict])], axis=0)
            self.known_df.to_parquet(temp_path, compression='gzip')
            time.sleep(0.1)

        # Update the known players and remove the temp file
        self.known_df.to_parquet(self.parquet_path, compression='gzip')
        if os.path.isfile(temp_path):
            os.remove(temp_path)



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
                try:
                    response = client.users.get_public_data(player_name)
                except Exception as e:
                    time_str = time.strftime('%H:%M:%S', time.localtime(time.time()))
                    print(f'\n{time_str}: {e}')
                    time.sleep(60)
                    # Wait for 60s once and return an empty dict if another error occurs
                    try:
                        response = client.users.get_public_data(player_name)
                    except:
                        response = {}
                known_players[player_name] = response.get('profile', {})
        return known_players[player_name]


def run(token: str, file_name: str = 'test_cleaned.parquet.gzip'):
    api = PlayerAPI(token)
    api.update_known_players(new_players_parquet=file_name)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--token', type=str, required=True)
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    token = args.token
    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f"Updating known players based on {file_name}")
    print('-' * 50)
    run(token=token, file_name=file_name)
