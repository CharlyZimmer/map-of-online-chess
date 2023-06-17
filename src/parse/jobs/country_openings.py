import os
from pandas import DataFrame, read_parquet
from typing import Tuple

from src import DATA_DIRECTORY

def load_and_filter_data(player_openings_parquet: str = "test_cleaned.parquet.gzip",
                         known_players_parquet: str = "known_players.parquet.gzip",
                         min_games: int = 10,) -> DataFrame:
    # 1. Load the player openings parquet
    openings_path = DATA_DIRECTORY / f'output/players/{player_openings_parquet}'
    openings_df = read_parquet(openings_path)

    # 2. Load the DataFrame of known players and keep only those with country information
    known_path = DATA_DIRECTORY / f'output/players/{known_players_parquet}'
    known_df = read_parquet(known_path).drop_duplicates(subset=['id'])
    known_df = known_df.loc[known_df.country.notna()][['id', 'country']]

    # 3. Keep only players for which the country is known
    openings_df = openings_df.loc[openings_df.id.isin(known_df.id)]

    # 4. Keep only players that have a minimum number of games
    num_games = openings_df.groupby('id')['count'].sum()
    considered_players = num_games.loc[num_games >= min_games].index.to_list()
    openings_df = openings_df.loc[openings_df.id.isin(considered_players)]

    return openings_df.merge(known_df, on='id')

def get_country_dfs(openings_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    # 1. Calculate the share per opening for each player
    openings_df['share'] = openings_df['count'].div(openings_df.groupby('id')['count'].transform('sum'))

    # 2. Calculate the mean share for an opening across players from the same country; Count players per country
    country_opening_df = openings_df.groupby(['country', 'matched_name'])['share'].mean().reset_index()
    country_player_count_df = openings_df.groupby('country')['id'].count().reset_index()


    return country_opening_df, country_player_count_df

def run(file_name: str = "test_cleaned.parquet.gzip"):
    out_dir = DATA_DIRECTORY / f'output/countries'
    out_path = out_dir / file_name
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    openings_df = load_and_filter_data(player_openings_parquet=file_name)
    country_opening_df, country_player_count_df = get_country_dfs(openings_df=openings_df)

    country_opening_df.to_parquet(str(out_path).replace('.parquet', '_openings.parquet'))
    country_player_count_df.to_parquet(str(out_path).replace('.parquet', '_player_count.parquet'))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f"Calculating country openings based on {file_name}")
    print('-' * 50)
    run(file_name=file_name)

