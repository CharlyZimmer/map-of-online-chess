import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lower

from src import DATA_DIRECTORY

#TODO: Create unique ID per opening in the CSV; Use it instead of matched_name in downstream steps

def run(file_name: str = 'test_cleaned.parquet.gzip'):
    '''
    Read a parquet file of games and the played openings and turn it into a parquet of number of openings
    played by each player. The output file has the following columns:

    :param file_name:       Path to the parquet file containing games with openings
    '''

    # 0. Paths and directories
    in_path = DATA_DIRECTORY / f'output/games/{file_name}'
    out_dir = DATA_DIRECTORY / f'output/players'
    out_path = out_dir / file_name
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # 1. Spark preparation; Load the parquet file and keep only valid openings
    spark = SparkSession.builder \
        .config('spark.driver.memory', '4g') \
        .appName('Player Openings') \
        .getOrCreate()
    df = spark.read.parquet(str(in_path))
    df_filtered = df.filter(col('matched_name').isNotNull())

    # 2. Group players (across black and white) by their name and count the occurrence of openings
    df_grouped_a = df_filtered.groupBy('white', 'matched_name')\
        .agg(count('*').alias('opening_count'))\
        .withColumnRenamed('white', 'username').withColumnRenamed('opening_count', 'count_w')
    df_grouped_b = df_filtered.groupBy('black', 'matched_name')\
        .agg(count('*').alias('opening_count'))\
        .withColumnRenamed('black', 'username').withColumnRenamed('opening_count', 'count_b')

    # Outer join with 0 fill to keep all rows and get correct sum; Add an id column for API calls
    df_final = df_grouped_a.join(df_grouped_b, ['username', 'matched_name'], 'outer')\
        .fillna(0)\
        .withColumn('count', col('count_w') + col('count_b'))\
        .withColumn('id', lower(col('username')))

    # 3. Save the result and stop spark session
    df_final.write.parquet(str(out_path))
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