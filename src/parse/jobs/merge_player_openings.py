from datetime import datetime
from dateutil.relativedelta import relativedelta
import functools
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

from src import DATA_DIRECTORY

def create_range(start_month: str, end_month: str = None):
    '''
    Merge counts for each player-opening combination in a range of months into a single file
    Month input needs to be in format 'YYYY-MM'.
    If no end_month is provided, the current month will be used
    :param start_month:      First month to be added to the merged file
    :param end_month:        Last month to be added to the merged file
    :return:                 List of file names for all months in range
    '''
    # Turn the month string into datetime instances
    if end_month is None:
        end_month = datetime.today()
    else:
        end_month = datetime.strptime(end_month, '%Y-%m')
    start_month = datetime.strptime(start_month, '%Y-%m')

    # Get all months in the range and turn them into file names
    delta = (end_month.year - start_month.year) * 12 + (end_month.month - start_month.month)
    months = [start_month + relativedelta(months=i) for i in range(delta + 1)]

    file_names = [(DATA_DIRECTORY / f'output/players/{month.strftime("%Y-%m")}.parquet') for month in months]
    return file_names

def unionAll(dfs):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)

def run(start_month: str, end_month: str = None):
    # 0. Get the filenames
    input_files = create_range(start_month, end_month)

    # 1. Spark preparation; Load and concat the parquet files
    spark = SparkSession.builder \
        .config('spark.driver.memory', '8g') \
        .appName('Merge Monthly Data') \
        .getOrCreate()
    spark.sparkContext.setCheckpointDir('./checkpoints')
    concat_df = unionAll([spark.read.parquet(str(path)) for path in input_files])

    # 2. Aggregate all columns by grouping on user id and opening id
    df_final = concat_df.groupBy('id', 'matched_id').agg(
        sum('count_w').alias('count_w'),
        sum('won_w').alias('won_w'),
        sum('lost_w').alias('lost_w'),
        sum('count_b').alias('count_b'),
        sum('won_b').alias('won_b'),
        sum('lost_b').alias('lost_b'),
        sum('real_game_w').alias('real_game_w'),
        sum('real_game_b').alias('real_game_b')
    )

    # 3. Write the result
    out_path = DATA_DIRECTORY / f'output/players/{start_month}_{end_month}.parquet'
    df_final.write.parquet(str(out_path), mode='overwrite')
    spark.stop()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--start_month', type=str, required=True)
    parser.add_argument('--end_month', type=str, required=True)
    args = parser.parse_args()

    start_month = args.start_month
    end_month = args.end_month

    print('\n' + '-' * 50)
    print(f'Aggregating player opening counts from {start_month} to {end_month}')
    print('-' * 50)
    run(start_month=start_month, end_month=end_month)