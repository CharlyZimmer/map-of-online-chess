import os
from pandas import concat, DataFrame, merge, read_parquet
from typing import Tuple

from src import DATA_DIRECTORY

def load_and_filter_data(player_openings_parquet: str = "test_cleaned.parquet.gzip",
                         known_players_parquet: str = "known_players.parquet.gzip",
                         min_games: int = 10,) -> DataFrame:
    '''
    Creates a DataFrame with player / opening combinations and how often they played / won / lost with it.
    The columns are:
        - id:                   ID of a player
        - matched_id: 	        ID of a known opening
        - count_w / count_b:    Number of times that player played an opening as white / black
        - won_w / lost_w:       Number of times that player won / lost with an opening as white
        - won_b / lost_b:       Number of times that player won / lost with an opening as black
        - country:              Country of the player as obtained by the lichess API

    :param player_openings_parquet:     File name of the parquet file containing player/opening combinations
    :param known_players_parquet:       File name of the parquet file containing the country information for each player
    :param min_games:                   Minimum number of games for a player to be considered in country probabilities

    :return:                            DataFrame with the aforementioned columns
    '''

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
    num_games = openings_df.groupby('id')['count_w'].sum() + openings_df.groupby('id')['count_b'].sum()
    considered_players = num_games.loc[num_games >= min_games].index.to_list()
    openings_df = openings_df.loc[openings_df.id.isin(considered_players)]

    return openings_df.merge(known_df, on='id')

def get_global_mean_std(openings_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    '''
    Creates a DataFrame of openings and their global values for mean and standard deviation.
    The columns are:
        - mean_p_w / mean_p_b:              Global mean probability for an opening played as white/black
        - mean_p_won_w / mean_p_lost_w:     Global mean for winning / losing with an opening as white
        - mean_p_won_b / mean_p_lost_b:     Global mean for winning / losing with an opening as black
        - std_p_w, std_p_b,... :            Standard deviation for the same events as described for the mean values

    :param openings_df:                     DataFrame as produced by load_and_filter_data;
                                            Contains how often each player played / won with / lost with an opening

    :return:                                DataFrame with the aforementioned columns;
                                            Updated openings_df with probabilities on player level
    '''
    global_mean_std = DataFrame()
    for color in ['w', 'b']:
        # 1a. Calculate the probability per opening for each player
        openings_df[f'p_{color}'] = openings_df[f'count_{color}'] \
            .div(openings_df.groupby('id')[f'count_{color}'].transform('sum'))

        # 1b. Calculate probability of winning and losing for each player-opening-combination
        openings_df[f'p_won_{color}'] = openings_df[f'won_{color}'] / openings_df[f'count_{color}']
        openings_df[f'p_lost_{color}'] = openings_df[f'lost_{color}'] / openings_df[f'count_{color}']

        # 2. Calculate global mean and standard deviation for opening and win/loose probabilities
        global_mean_std[[f'mean_p_{color}', f'mean_p_won_{color}', f'mean_p_lost_{color}']] = (
            openings_df.groupby('matched_id')[[f'p_{color}', f'p_won_{color}', f'p_lost_{color}']].mean())

        global_mean_std[[f'std_p_{color}', f'std_p_won_{color}', f'std_p_lost_{color}']] = (
            openings_df.groupby('matched_id')[[f'p_{color}', f'p_won_{color}', f'p_lost_{color}']].std())

    return global_mean_std, openings_df


def get_country_probabilities(openings_df: DataFrame, global_mean_std: DataFrame) -> DataFrame:
    # Join all observed combinations of country and opening with mean and standard deviation values
    df = openings_df.groupby(['country', 'matched_id']).count().reset_index()[['country', 'matched_id']]
    df = merge(global_mean_std, df, on='matched_id', how='right')

    for color in ['w', 'b']:
        # 1. Calculate the probabilities per opening for each country (played, won, and lost)
        tmp_df = (openings_df.groupby(['country', 'matched_id'])[[f'p_{color}', f'p_won_{color}', f'p_lost_{color}']]
                  .mean().reset_index())
        df = merge(df, tmp_df, on=['matched_id', 'country'], how='right')

        # 2. Standardize the country_opening DataFrames using z-score (mean and std)
        # 2a. Standardize probabilities for an opening being played
        df[f'stand_p_{color}'] = ((df[f'p_{color}'] - df[f'mean_p_{color}'])
                                   / (1.e-17 + df[f'std_p_{color}']))

        # 2b. Probabilities for winning with an opening
        df[f'stand_p_won_{color}'] = ((df[f'p_won_{color}'] - df[f'mean_p_won_{color}'])
                                       / (1.e-17 + df[f'std_p_won_{color}']))

        # 2c. Probabilities for losing with an opening
        df[f'stand_p_lost_{color}'] = ((df[f'p_lost_{color}'] - df[f'mean_p_lost_{color}'])
                                        / (1.e-17 + df[f'std_p_lost_{color}']))

    # 3. Drop all columns used for standardization (mean_p_* and std_p_*); Get country column in front
    df = df[df.columns.drop(list(df.filter(regex='mean_p_*')))]
    df = df[df.columns.drop(list(df.filter(regex='std_p_*')))]

    return df



def get_country_dfs(openings_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    # Calculate mean and standard deviation for opening probabilities and win/loss per color on global level
    global_mean_std, openings_df = get_global_mean_std(openings_df)

    # Calculate values on country level and standardize them
    country_opening_df = get_country_probabilities(openings_df, global_mean_std)

    # Finalize DataFrames and save global means and standard deviations
    global_mean_std.to_parquet(
        str(DATA_DIRECTORY / 'openings/opening_mean_std.parquet.gzip'),
        compression='gzip'
    )
    country_player_count_df = openings_df.groupby('country')['id'].count() \
        .reset_index().rename(columns={'id': 'num_players'})

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

