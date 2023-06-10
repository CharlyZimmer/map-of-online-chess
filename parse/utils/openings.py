from pandas import DataFrame, concat, read_csv
from pathlib import Path
from typing import Tuple

from parse import PARSING_DIRECTORY

class OpeningLoader:
    def __init__(self, path: Path = PARSING_DIRECTORY / 'data'):
        self.path = path
        self.df_path = path / 'openings.csv'

        self._load_df()

    def _load_df(self):
        try:
            self.df = read_csv(self.df_path)
        except:
            self.df = self._merge_tsv_files()
            self.df.to_csv(self.df_path)

    def _merge_tsv_files(self) -> DataFrame:
        '''
        Combine .tsv files of openings into a single DataFrame
        :return     DataFrame of all openings
        '''
        files = [x for x in self.path.glob('*.tsv')]
        df = DataFrame()
        for file in files:
            df = concat((df, read_csv(file, sep='\t')))
        return df


def match_opening(df: DataFrame, name: str, uci_str: str) -> Tuple[str, str]:
    '''
    Identify a known opening with the longest shared prefix of UCI moves
    :param df:          DataFrame of known moves
    :param name:        Name of the opening according to a game record
    :param uci_str:     Str of moves in UCI notation to identify the opening by
    :return:            Name of the matched opening and the moves of that opening in UCI notation.
                        If no match was found, return 'Unknown', uci_str
    '''
    matched_moves = None
    matched_name = None

    # Get the base opening (by splitting on ":") to identify all openings with that prefix
    base_opening = name.split(':')[0]
    relevant_df = df.loc[df['name'].str.contains(base_opening, regex=False)]

    # Loop over all moves to check if the uci_str fits
    for name, moves in zip(relevant_df.name, relevant_df.uci):
        # As the moves are ordered by length, the longest shared prefix will be returned
        if uci_str.startswith(moves):
            matched_moves = moves
            matched_name = name

    return matched_name, matched_moves



