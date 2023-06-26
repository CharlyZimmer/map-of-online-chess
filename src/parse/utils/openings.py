import hashlib
from os.path import isfile
from pandas import DataFrame, concat, read_csv, Series
from pathlib import Path
from typing import Tuple

from src import DATA_DIRECTORY

class OpeningLoader:
    def __init__(self, path: Path = DATA_DIRECTORY / 'openings'):
        self.path = path
        self.df_path = path / 'openings.csv'

        self._load_df()

    def _load_df(self):
        if isfile(f'{self.df_path}'):
            self.df = read_csv(self.df_path)
        else:
            self.df = self._merge_tsv_files()
            self.df = self.df.loc[:, ~self.df.columns.str.contains('^Unnamed')]
            self.df = self.df.apply(self._create_id, axis=1)
            self.df.to_csv(self.df_path)


    def _merge_tsv_files(self) -> DataFrame:
        '''
        Combine .tsv files of openings into a single DataFrame
        :return     DataFrame of all openings
        '''
        if not isfile(f'{self.path}/a.tsv'):
            print(f'.tsv files not found in {self.path}. '
                  f'Please clone the repository under https://github.com/lichess-org/chess-openings/ and run make to'
                  f'get the correct files. Then copy them to {self.path}')

        files = [x for x in self.path.glob('*.tsv')]
        df = DataFrame()
        for file in files:
            df = concat((df, read_csv(file, sep='\t')))
        return df

    @staticmethod
    def _create_id(row: Series) -> Series:
        '''
        Create a hash id for each opening row by using the name and moves in uci notation.
        Each id starts with its ECO string
        :param row:     Row of the opening dataframe
        :return:        Row with ID of form [ECO]-[HASHED-NAME]-[HASHED-UCI]
        '''
        part1 = str(int(hashlib.md5(row['name'].encode()).hexdigest(), 16))[0:4]
        part2 = str(int(hashlib.md5(row['uci'].encode()).hexdigest(), 16))[0:4]
        row['id'] = f'{row["eco"]}-{part1}-{part2}'
        return row


def match_opening(df: DataFrame, name: str, uci_str: str) -> Tuple[str, str]:
    '''
    Identify a known opening with the longest shared prefix of UCI moves
    :param df:          DataFrame of known openings
    :param name:        Name of the opening according to a game record
    :param uci_str:     Str of moves in UCI notation to identify the opening by
    :return:            ID of the matched opening and the moves of that opening in UCI notation.
                        If no match was found, return None, None
    '''
    matched_moves = None
    matched_id = None

    # Get the base opening (by splitting on ":") to identify all openings with that prefix
    base_opening = name.split(':')[0]
    relevant_df = df.loc[df['name'].str.contains(base_opening, regex=False)]

    # Loop over all moves to check if the uci_str fits
    for opening_id, moves in zip(relevant_df.id, relevant_df.uci):
        # As the moves are ordered by length, the longest shared prefix will be returned
        if uci_str.startswith(moves):
            matched_moves = moves
            matched_id = opening_id

    return matched_id, matched_moves



