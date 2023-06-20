import berserk
import os
from pandas import concat, DataFrame, read_parquet
from ratelimiter import RateLimiter
from tqdm import tqdm

from src import DATA_DIRECTORY

class PlayerAPI:
    def __init__(self, token: str,
                 known_players_parquet: str = "known_players.parquet.gzip"):
        '''

        :param token:                   Token for the lichess API (https://lichess.org/api#section/Introduction/Authentication)
        :param known_players_parquet:   Name of the file to load and store known players
        '''
        # Preparation: Try to load known players and set client
        self.parquet_path = DATA_DIRECTORY / f'output/players/{known_players_parquet}'
        if os.path.isfile(self.parquet_path):
            self.known_df = read_parquet(self.parquet_path).drop_duplicates(subset=['id'])
            self.known_players = self.known_df.set_index('id').to_dict(orient='index')
        else:
            self.known_df = DataFrame()
            self.known_players = {}

        session = berserk.TokenSession(token)
        self.client = berserk.Client(session=session)

    def update_known_players(self, new_players_parquet: str = 'test.parquet.gzip',
                             request_size: int = 300,
                             ratelimit_players: int = 8_000,
                             ratelimit_sec: int = 10*60,
                             max_players: int = 120_000):
        '''
        Read in a parquet file of new players and request their public information from the lichess API
        :param new_players_parquet:     Filename of the parquet file (in data/parse/output/players)
        :param request_size:            How many players to request at once
        :param ratelimit_players:       How many players to request per iteration; default 8.000
        :param ratelimit_sec:           How many seconds should pass between requests; default 600
        :param max_players:             How many players to request in one job (daily limit); default 120_000
        '''
        parquet_path = DATA_DIRECTORY / f'output/players/{new_players_parquet}'
        temp_path = str(self.parquet_path).replace('.parquet.gzip', '_temp.parquet.gzip')

        # Load file with new player data (drop known players) and create ratelimiter
        df = read_parquet(parquet_path)
        player_df = df.groupby('id')['matched_name'].count().reset_index().drop('matched_name', axis=1)
        player_df = player_df.loc[~player_df['id'].str.lower().isin(self.known_players.keys())]
        player_df = player_df.loc[(player_df['id'] != '?') & (player_df['id'].notna())].iloc[:max_players]

        lichess_rate_limiter = RateLimiter(max_calls=ratelimit_players // request_size, period=ratelimit_sec)

        # Create dataframe chunks to be requested at once and request subject to the rate limit
        request_chunks = [player_df[i:i+request_size] for i in range(0, player_df.shape[0], request_size)]
        for chunk_df in tqdm(request_chunks, desc='Requesting player profiles'):
            with lichess_rate_limiter:
                request_str = ','.join([name[0] for name in chunk_df.values])
                result = self.client.users.get_by_id(request_str)

                # Post-process the result dictionary and get only relevant keys (id and profile if it exists)
                # Save the intermediate result
                player_records = [{'id': x['id'].lower(), **x.get('profile', {})} for x in result]
                self.known_df = concat([self.known_df, DataFrame.from_records(player_records)], axis=0)
                self.known_df.to_parquet(temp_path, compression='gzip')

        # Update the known players and remove the temp file
        self.known_df.to_parquet(self.parquet_path, compression='gzip')
        if os.path.isfile(temp_path):
            os.remove(temp_path)


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
