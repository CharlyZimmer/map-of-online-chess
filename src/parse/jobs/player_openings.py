import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower

from src import DATA_DIRECTORY

def run(file_name: str = 'test_cleaned.parquet.gzip'):
    '''
    Read a parquet file of games and the played openings and turn it into a parquet of number of openings
    played by each player. The output file has the following columns:
        - id:           ID of a player (str)
        - matched_id    ID of a known opening (str)
        - count_w       Number of times that player played an opening as white (int)
        - won_w         Number of times that player won with an opening as white (int)
        - lost_w        Number of times that player lost with an opening as white (int)
        - count_b       Number of times that player played an opening as black (int)
        - won_b         Number of times that player won with an opening as black (int)
        - lost_b        Number of times that player lost with an opening as black (int)

    :param file_name:       Path to the parquet file containing games with openings
    '''

    # 0. Paths and directories
    in_path = DATA_DIRECTORY / f'output/games/{file_name}'
    out_dir = DATA_DIRECTORY / f'output/players'
    out_path = out_dir / file_name
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # 1. Spark preparation; Load the parquet file and keep only valid openings and classical games
    spark = SparkSession.builder \
        .config('spark.driver.memory', '4g') \
        .appName('Player Openings') \
        .getOrCreate()
    df = spark.read.parquet(str(in_path))
    df_filtered = df.filter(col('matched_id').isNotNull())

    # 2. Group players (across black and white) by their name and count the occurrence of openings
    df_grouped_a = df_filtered.groupBy('white', 'matched_id')\
        .agg(count('*').alias('count_w')).withColumnRenamed('white', 'username')
    df_grouped_b = df_filtered.groupBy('black', 'matched_id')\
        .agg(count('*').alias('count_b')).withColumnRenamed('black', 'username')

    # Outer join with 0 fill to keep all rows and get correct sum; Add an id column for API calls
    df_counts = df_grouped_a.join(df_grouped_b, ['username', 'matched_id'], 'outer')\
        .fillna(0)

    # 3. Count number of wins with opening per player and colour
    df_won_white = df_filtered.filter(col('result') == '1-0').groupBy('white', 'matched_id')\
        .agg(count('*').alias('won_w')).withColumnRenamed('white', 'username')
    df_lost_white = df_filtered.filter(col('result') == '0-1').groupBy('white', 'matched_id')\
        .agg(count('*').alias('lost_w')).withColumnRenamed('white', 'username')
    df_won_black = df_filtered.filter(col('result') == '0-1').groupBy('black', 'matched_id')\
        .agg(count('*').alias('won_b')).withColumnRenamed('black', 'username')
    df_lost_black = df_filtered.filter(col('result') == '1-0').groupBy('black', 'matched_id') \
        .agg(count('*').alias('lost_b')).withColumnRenamed('black', 'username')

    # 3. Save the result and stop spark session
    df_tmp_a = df_counts.join(df_won_white, ['username', 'matched_id'], 'outer').fillna(0)
    df_tmp_b = df_tmp_a.join(df_lost_white, ['username', 'matched_id'], 'outer').fillna(0)
    df_tmp_c = df_tmp_b.join(df_won_black, ['username', 'matched_id'], 'outer').fillna(0)
    df_final = df_tmp_c.join(df_lost_black, ['username', 'matched_id'], 'outer').fillna(0)\
        .withColumn('id', lower(col('username')))\
        .drop(col('username'))\
        .select('id', 'matched_id', 'count_w', 'won_w', 'lost_w', 'count_b', 'won_b', 'lost_b')

    df_final.write.parquet(str(out_path), mode='overwrite')
    spark.stop()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f'Grouping players and counting their openings based on {file_name}')
    print('-' * 50)
    run(file_name=file_name)