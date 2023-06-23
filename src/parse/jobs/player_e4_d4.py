from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from src import DATA_DIRECTORY
from src.parse.utils.openings import OpeningLoader



def run(file_name: str = 'test_cleaned.parquet.gzip'):
    '''
    Read a parquet file of openings per player and count the number of games that this player started with E4 or D4.
    Therefore, use all known openings that start with E4 (or D4) and count them when a player played as white.

    :param file_name:       Path to the parquet file containing opening counts for each player
    '''

    # 0. Paths and directories
    player_dir = DATA_DIRECTORY / f'output/players'
    in_path = player_dir / file_name
    out_path = player_dir / file_name.replace('.parquet', '_e4d4.parquet')

    # 1. Identify openings that start with e4 or d4
    loader = OpeningLoader()
    e4_openings = loader.df.loc[loader.df["pgn"].str.contains("1. e4"), "name"].to_list()
    d4_openings = loader.df.loc[loader.df["pgn"].str.contains("1. d4"), "name"].to_list()

    # 2. Spark preparation; Load the parquet file; Prepare the udf
    spark = SparkSession.builder \
        .config('spark.driver.memory', '4g') \
        .appName('Player E4 D4') \
        .getOrCreate()
    df = spark.read.parquet(str(in_path))

    def e4d4_map(opening_name: str):
        if opening_name in e4_openings:
            return 'e4'
        elif opening_name in d4_openings:
            return 'd4'
        return 'other'
    e4d4_udf = udf(e4d4_map, StringType())

    # 3. Replace opening names with e4, d4 or other; Group by id and first_move; Sum count_w
    df = df.withColumn("first_move", e4d4_udf(df['matched_name']))\
        .drop('matched_name') \
        .drop('count_b') \
        .drop('count') \
        .drop('username') \
        .groupBy('id', 'first_move') \
        .sum('count_w') \
        .withColumnRenamed("sum(count)", "count")

    # 3. Save the result and stop spark session
    df.write.parquet(str(out_path))
    spark.stop()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f'Counting first moves (e4, d4, other) based on {file_name}')
    print('-' * 50)
    run(file_name=file_name)