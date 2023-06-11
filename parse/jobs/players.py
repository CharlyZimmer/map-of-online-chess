import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from parse import PARSING_DIRECTORY

def run(file_name: str = 'test.parquet.gzip'):
    '''
    Read a parquet file of games and the played openings and turn it into a parquet of number of openings
    played by each player. The output file has the following columns:

    :param file_name:       Path to the parquet file containing games with openings
    '''

    # 0. Paths and directories
    parquet_path = PARSING_DIRECTORY / f'data/output/openings/{file_name}'
    out_dir = PARSING_DIRECTORY / f'data/output/players'
    out_path = out_dir / file_name
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # 1. Spark preparation; Load the parquet file and keep only valid openings
    spark = SparkSession.builder \
        .appName('Player Openings') \
        .getOrCreate()
    df = spark.read.parquet(str(parquet_path))
    df_filtered = df.filter(col('matched_name').isNotNull())

    # 2. Group players (across black and white) by their name and count the occurrence of openings
    df_grouped_a = df_filtered.groupBy('white', 'matched_name')\
        .agg(count('*').alias('opening_count'))\
        .withColumnRenamed('white', 'player').withColumnRenamed('opening_count', 'count_w')
    df_grouped_b = df_filtered.groupBy('black', 'matched_name')\
        .agg(count('*').alias('opening_count'))\
        .withColumnRenamed('black', 'player').withColumnRenamed('opening_count', 'count_b')

    # Outer join with 0 fill to keep all rows and get correct sum
    df_final = df_grouped_a.join(df_grouped_b, ["player", "matched_name"], "outer")\
        .fillna(0)\
        .withColumn("count", col("count_w") + col("count_b"))

    # 3. Save the result and stop spark session
    df_final.write.parquet(str(out_path))
    spark.stop()