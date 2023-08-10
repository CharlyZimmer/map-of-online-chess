import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev

from src import DATA_DIRECTORY

def run(file_name: str = "test_cleaned_prob.parquet", min_games: int = 10):
    '''
    Creates a DataFrame with country / opening combinations and probabilities for playing / winning on country level.
    These probabilities are also standardized with global mean and standard deviation for openings
    The output file has the following columns:
        - country:                          Country name as recorded in the player profiles
        - matched_id: 	                    ID of a known opening
        - p_w / p_b                         Probability for playing an opening as white/black
        - p_won_w / p_won_b                 Probability for winning as white/black when an opening was played
        - stand_p_w / stand_p_b             z-Score standardized playing probabilities
        - stand_p_won_w / stand_p_won_b     z-Score standardized winning probabilities

    :param file_name:     File name of the parquet file containing probability values for player/opening combinations
    :param min_games:     Minimum number of games for a player to be considered in country probabilities
    '''

    # 1. Prepare folders
    in_path = DATA_DIRECTORY / f'output/players/{file_name}'
    out_dir = DATA_DIRECTORY / f'output/countries'
    out_path = out_dir / file_name.replace('_prob.p', '.p')
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # 2. Prepare spark
    spark = SparkSession.builder \
        .config('spark.driver.memory', '4g') \
        .appName('Player Probabilities') \
        .getOrCreate()
    spark.sparkContext.setCheckpointDir('./checkpoints')

    # 3. Load the probabilities parquet and only keep players with country information and the minimum number of games
    player_df = spark.read.parquet(str(in_path)) \
        .filter(col('country').isNotNull()) \
        .filter(col('total_count_w') + col('total_count_b') > min_games)

    # 4. Count players per country and save the result
    country_player_count_df = player_df.groupBy('country').agg(count('id').alias('num_players'))
    country_player_count_df.write.parquet(str(out_path).replace('.parquet', '_player_count.parquet'),
                                          mode='overwrite')

    # 5. Group by country and calculate the mean values
    country_df = player_df.groupBy('country', 'matched_id').agg(mean('p_w').alias('p_w'),
                                                                mean('p_b').alias('p_b'),
                                                                mean('p_won_w').alias('p_won_w'),
                                                                mean('p_won_b').alias('p_won_b')
                                                                )

    # 6. Find the global mean and stddev per opening; Save the result
    global_mean_std_df = player_df.groupBy('matched_id').agg(mean('p_w').alias('mean_p_w'),
                                                             stddev('p_w').alias('std_p_w'),
                                                             mean('p_b').alias('mean_p_b'),
                                                             stddev('p_b').alias('std_p_b'),
                                                             mean('p_won_w').alias('mean_p_won_w'),
                                                             stddev('p_won_w').alias('std_p_won_w'),
                                                             mean('p_won_b').alias('mean_p_won_b'),
                                                             stddev('p_won_b').alias('std_p_won_b')
                                                             ).fillna(0)

    mean_std_file = file_name.replace('_prob.p', '_mean_std.p')
    global_mean_std_df.write.parquet(str(DATA_DIRECTORY / f'openings/{mean_std_file}'),
                                     mode='overwrite')

    # 7. Use mean and stddev to standardize the country probabilities
    result = country_df.join(global_mean_std_df, on='matched_id') \
        .withColumn('stand_p_w', (col('p_w') - col('mean_p_w')) / (col('std_p_w') + 1.e-17)) \
        .withColumn('stand_p_b', (col('p_b') - col('mean_p_b')) / (col('std_p_b') + 1.e-17)) \
        .withColumn('stand_p_won_w', (col('p_won_w') - col('mean_p_won_w')) / (col('std_p_won_w') + 1.e-17)) \
        .withColumn('stand_p_won_b', (col('p_won_b') - col('mean_p_won_b')) / (col('std_p_won_b') + 1.e-17)) \
        .select(['country', 'matched_id', 'p_w', 'p_b', 'p_won_w', 'p_won_b',
                 'stand_p_w', 'stand_p_b', 'stand_p_won_w', 'stand_p_won_b'])

    result.write.parquet(str(out_path), mode='overwrite')
    spark.stop()


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

