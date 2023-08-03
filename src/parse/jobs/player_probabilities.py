import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

from src import DATA_DIRECTORY

def run(file_name: str = 'test_cleaned.parquet',
        known_players_parquet: str = 'known_players.parquet'):
    '''
    Calculates probabilities for all player-openings combinations and attaches country information.
    The output file has the following columns:
        - id: 	                ID of a player
        - matched_id:           ID of a known opening
        - total_count_w:        How many games that player played as white
        - total_count_b 	    How many games that player played as black
        - p_w / p_b:            Probability of playing a certain opening as white / black
        - p_won_w / p_won_b:    Probability of winning a game as white / black if a certain opening was played
        - country:              Country of the player as recorded in their lichess profile

    :param file_name:               File name of the parquet file containing player/opening combinations
    :param known_players_parquet:   File name of the parquet file containing the country information for each player
    '''

    # 1. Prepare folders
    in_path = DATA_DIRECTORY / f'output/players/{file_name}'
    out_dir = DATA_DIRECTORY / f'output/players'
    out_path = out_dir / file_name.replace('.p', '_prob.p')
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # 2. Prepare spark and load the dataframe
    spark = SparkSession.builder \
        .config('spark.driver.memory', '4g') \
        .appName('Player Probabilities') \
        .getOrCreate()
    spark.sparkContext.setCheckpointDir('./checkpoints')
    df = spark.read.parquet(str(in_path))

    # 3. Calculate probabilities for each player opening combination and winning / loosing
    df_total_w = df.groupBy('id').agg(sum('real_game_w').alias('total_count_w'))
    df_total_b = df.groupBy('id').agg(sum('real_game_b').alias('total_count_b'))

    df_joined = df.join(df_total_w, 'id').join(df_total_b, 'id')
    df_prob = (df_joined
               .withColumn('p_w', col('count_w') / col('total_count_w'))
               .withColumn('p_b', col('count_b') / col('total_count_b'))
               .withColumn('p_won_w', col('won_w') / col('count_w'))
               .withColumn('p_won_b', col('won_b') / col('count_b'))
               .drop('won_w', 'lost_w', 'won_b', 'lost_b', 'count_w', 'count_b', 'real_game_w', 'real_game_b'))

    # 4. Join the country information
    known_path = DATA_DIRECTORY / f'output/players/{known_players_parquet}'
    known_df = (spark.read.parquet(str(known_path))
                .select(['id', 'country'])
                .filter(col('country').isNotNull()))
    result = df_prob.join(known_df, on='id', how='left')

    # 4. Save the result and stop spark session
    result.write.parquet(str(out_path), mode='overwrite')
    spark.stop()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f'Calculating players-opening probabilities based on {file_name}')
    print('-' * 50)
    run(file_name=file_name)
