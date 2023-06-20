from pyspark.sql import SparkSession
import chess.pgn
from io import StringIO
import os
from pandas import DataFrame
from typing import Dict

from src import DATA_DIRECTORY
from src.parse.utils.openings import OpeningLoader, match_opening


def parse_pgn(pgn_text: str) -> chess.pgn.Game:
    '''
    Turn a text snippet of a PGN game into a chess.pgn.Game
    :param pgn_text:    String of the game
    :return:            Instance of chess.pgn.Game parsed from the string
    '''
    pgn_io = StringIO(pgn_text)
    game = chess.pgn.read_game(pgn_io)
    return game

def process_game(game: chess.pgn.Game, opening_df: DataFrame, num_moves: int = 10) \
        -> Dict[str, str]:
    '''
    Parse a chess game into players, opening names and moves.
    Name and moves will be taken both from the game data as well as a collection of known moves
    :param game:        Instance of chess.pgn.Game
    :param opening_df:  DataFrame of known openings
    :param num_moves:   Number of moves to store per game
    :return:            Dictionary with the following keys:
                        - 'white':              Name of the white player
                        - 'black':              Name of the black player
                        - 'matched_name':       Name of a known opening (Or None)
                        - 'matched_moves':      Moves of the known opening (Or None)
                        - 'original_name':      Name of the opening according to the game record
                        - 'original_moves':     First num_moves of the game
    '''

    # Get player names
    w = game.headers['White']
    b = game.headers['Black']

    # Get opening name and first moves in uci notation to check with known openings
    try:
        opening_name = game.headers['Opening']
    except:
        opening_name = 'Unknown'
    uci_str = ' '.join([move.uci() for move in game.mainline_moves()][:num_moves])
    matched_name, matched_moves = match_opening(df=opening_df,
                                               name=opening_name,
                                               uci_str=uci_str)

    return {'white': w, 'black': b, 'matched_name': matched_name, 'matched_moves': matched_moves,
            'original_name': opening_name, 'original_moves': uci_str}


def run(file_name: str = 'test_cleaned.pgn', partitions=10):
    # 0. Preparation
    # Paths and directories
    in_path = DATA_DIRECTORY / f'pgn/{file_name}'
    out_dir = DATA_DIRECTORY / f'output/games'
    out_path = out_dir / file_name.replace('.pgn', '.parquet.gzip')
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # Get the openings df
    loader = OpeningLoader()
    opening_df = loader.df

    # 1. Spark preparation
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # 2. Extract the game data
    # The pgn file needs to be produced by parse/scripts/rewrite_pgn.sh to get lines of game strings
    # Create RDD of games by parsing the pgn text
    data = sc.textFile(str(in_path), minPartitions=partitions).map(lambda x: x.replace('###', '\n'))
    games = data.map(lambda game_str: parse_pgn(game_str))

    # 3. Extract the relevant information per game and save the results as a parquet file
    parsed_df = games.map(lambda game: process_game(game=game, opening_df=opening_df)).toDF()
    parsed_df.write.parquet(str(out_path))

    sc.stop()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--pgn_file', type=str, required=True)
    parser.add_argument('--num_partitions', type=int, required=False)
    args = parser.parse_args()

    file_name = args.pgn_file

    print('\n' + '-' * 50)
    print(f'Extracting openings from {file_name}')
    print('-' * 50)
    if args.num_partitions is not None:
        run(file_name=file_name, partitions=args.num_partitions)
    else:
        run(file_name=file_name)