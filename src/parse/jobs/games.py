from pyspark.sql import SparkSession
import chess.pgn
from io import StringIO
import os
from typing import Dict, Sequence

from src import DATA_DIRECTORY
from src.parse.utils.openings import OpeningLoader, OpeningNode


def parse_pgn(pgn_text: str) -> chess.pgn.Game:
    '''
    Turn a text snippet of a PGN game into a chess.pgn.Game
    :param pgn_text:    String of the game
    :return:            Instance of chess.pgn.Game parsed from the string
    '''
    pgn_io = StringIO(pgn_text)
    game = chess.pgn.read_game(pgn_io)
    return game

def process_game(game: chess.pgn.Game, root_node: OpeningNode, num_moves: int = 10) -> Sequence[Dict]:
    '''
    Parse a chess game into players, opening names and moves.
    Moves will be taken both from the game data as well as a collection of known moves;
    One game will be split into 1 or more lines to reflect all matched openings. The longest matching move prefix
    will be noted with 'real_game': 1. This means that counting all rows with real_game == 1 yields number of games
    :param game:        Instance of chess.pgn.Game
    :param root_node:   Root node of the opening tree. Used to traverse tree; Has no valid opening associated with it
    :param num_moves:   Number of moves to store per game
    :return:            Dictionary with the following keys:
                        - 'white':              Name of the white player
                        - 'black':              Name of the black player
                        - 'matched_id':         ID of a known opening (Or None)
                        - 'matched_moves':      Moves of the known opening (Or None)
                        - 'original_name':      Name of the opening according to the game record
                        - 'original_moves':     First num_moves of the game
    '''
    # Get header information
    header = game.headers
    w = header['White']
    b = header['Black']
    result = header['Result']
    w_elo = header['WhiteElo']
    b_elo = header['BlackElo']
    event = header['Event']

    # Get opening name and first moves in uci notation to check with known openings
    try:
        opening_name = game.headers['Opening']
    except:
        opening_name = 'Unknown'
    uci_str = ' '.join([move.uci() for move in game.mainline_moves()][:num_moves])

    # Construct the dictionary of meta information
    meta_dict = {'white': w, 'black': b, 'original_name': opening_name, 'original_moves': uci_str, 'result': result,
                 'w_elo': w_elo, 'b_elo': b_elo, 'event': event}

    # Identify all openings with matching move prefix by traversing the tree of openings
    # The first element is dropped as it is the base opening (if that fails, no match was found)
    try:
        result_list = _opening_tree_traversal(uci_str, root_node)[1:]
    except:
        result_list = {'matched_id': None, 'matched_moves': None, 'real_game': 1}

    # Create the final list by pairing the meta_dict with each element in th result_list
    return [{**meta_dict, **move_dict} for move_dict in result_list]

def _opening_tree_traversal(uci_str: str, node: OpeningNode):
    result = []
    if uci_str.startswith(node.uci):
        result_dict = {'matched_id': node.opening_id, 'matched_moves': node.uci, 'real_game': 0}

        subtree_result = []
        for child in node.children:
            if uci_str.startswith(child.uci):
                subtree_result = _opening_tree_traversal(uci_str, child)

        if len(subtree_result) == 0:
            result_dict['real_game'] = 1

        result = [result_dict, *subtree_result]

    return result


def run(file_name: str = 'test_cleaned.pgn', partitions=10):
    # 0. Preparation
    # Paths and directories
    in_path = DATA_DIRECTORY / f'pgn/{file_name}'
    out_dir = DATA_DIRECTORY / f'output/games'
    out_path = out_dir / file_name.replace('.pgn', '.parquet')
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # Get the root_node of the opening tree
    loader = OpeningLoader()
    root_node = loader.get_root_node()

    # 1. Spark preparation
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # 2. Extract the game data
    # The pgn file needs to be produced by parse/scripts/rewrite_pgn.sh to get lines of game strings
    # Create RDD of games by parsing the pgn text
    data = sc.textFile(str(in_path), minPartitions=partitions).map(lambda x: x.replace('###', '\n'))
    games = data.map(lambda game_str: parse_pgn(game_str))

    # 3. Extract the relevant information per game and save the results as a parquet file
    parsed_df = games.flatMap(lambda game: process_game(game=game, root_node=root_node)).toDF()
    parsed_df.write.parquet(str(out_path), mode='overwrite')

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