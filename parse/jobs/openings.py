from pyspark.sql import SparkSession
import chess.pgn
from io import StringIO
import os
from pandas import DataFrame
from typing import Dict

from parse import PARSING_DIRECTORY
from parse.utils.openings import OpeningLoader, match_opening

class ChessRecordReader():
    '''
    Class to ensure that game lines are read together
    '''
    def __init__(self):
        self.buff = ''
        self.in_moves = False

    def read_records(self, file_data):
        records = []
        line_iter = iter(file_data.decode('utf-8').splitlines())
        for line in line_iter:
            if line.startswith('[Event '):
                if self.buff != '':
                    records.append(self.buff)
                    self.buff = ''
                    self.in_moves = False
            if not line.startswith('[') and not self.in_moves:
                self.in_moves = True
                self.buff = self.buff.rstrip() + '\n'
            self.buff += line + '\n'

        if self.buff != '':
            records.append(self.buff)

        return records


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

def group_game_lines(record):
    chess_record_reader = ChessRecordReader()
    return chess_record_reader.read_records(record[1])

def run(file_name: str = 'test.pgn', partitions=10):
    # 0. Preparation
    # Paths and directories
    pgn_path = PARSING_DIRECTORY / f'data/pgn/{file_name}'
    out_dir = PARSING_DIRECTORY / f'data/output/openings'
    out_path = out_dir / file_name.replace('.pgn', '.parquet.gzip')
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # Get the openings df
    loader = OpeningLoader()
    opening_df = loader.df

    # 1. Spark preparation
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # 2. Extract the game data
    # Load the data and ensure that lines of the same game are grouped together; 
    # Create RDD of games by parsing the pgn text
    data = sc.binaryFiles(str(pgn_path))\
        .flatMap(group_game_lines)\
        .repartition(numPartitions=partitions)
    games = data.map(parse_pgn).filter(lambda game: game is not None)

    # 3. Extract the relevant information per game and save the results as a parquet file
    parsed_df = games.map(lambda game: process_game(game=game, opening_df=opening_df)).toDF()
    parsed_df.write.parquet(str(out_path))

    sc.stop()