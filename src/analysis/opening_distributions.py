import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

from src import DATA_DIRECTORY
def run(file_name: str = 'test_cleaned_prob.parquet', min_games=50):
    # 1. Prepare folders
    in_path = DATA_DIRECTORY / f'output/players/{file_name}'
    out_dir = DATA_DIRECTORY / f'analysis/distributions'
    out_path = out_dir / file_name.replace('_prob.p', '.p')
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # 2. Prepare spark and load the dataframe (When running into issues with Java Heap space, increase driver.memory)
    spark = SparkSession.builder \
        .config('spark.driver.memory', '12g') \
        .appName('Opening Distributions') \
        .getOrCreate()
    spark.sparkContext.setCheckpointDir('./checkpoints')

    # Filter for all players with at least the required number of games
    df = spark.read.parquet(str(in_path)).filter(col('total_count_w') + col('total_count_b') >= min_games)

    # 3. Get the global and country distributions for each opening
    global_df = df.groupBy('matched_id')\
        .agg(
        collect_list('p_w').alias('p_w_dist_global'),
        collect_list('p_b').alias('p_b_dist_global'),
        collect_list('p_won_w').alias('p_won_w_dist_global'),
        collect_list('p_won_b').alias('p_won_b_dist_global')
    )

    country_df = df.filter(col('country').isNotNull()).groupBy('matched_id', 'country') \
        .agg(
        collect_list('p_w').alias('p_w_dist'),
        collect_list('p_b').alias('p_b_dist'),
        collect_list('p_won_w').alias('p_won_w_dist'),
        collect_list('p_won_b').alias('p_won_b_dist')
    )

    # 4. Save the results and stop the spark context
    global_df.write.parquet(str(out_path).replace('.p', '_global.p'), mode='overwrite')
    country_df.write.parquet(str(out_path).replace('.p', '_country.p'), mode='overwrite')
    spark.stop()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f'Gather opening distributions based on {file_name}')
    print('-' * 50)
    run(file_name=file_name)