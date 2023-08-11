from anytree import NodeMixin, RenderTree
import hashlib
import numpy as np
from os.path import isfile
from pandas import DataFrame, concat, read_csv, Series
from pathlib import Path
from typing import Tuple

from src import DATA_DIRECTORY

# ============================================================================
# Tree structures
# ============================================================================
class OpeningNode(NodeMixin):
    '''
    Basic node class to create a tree structure where leafs have the same move prefix as their parents
    '''
    def __init__(self, opening_id: str, uci: str, pgn: str, parent=None, children=None):
        self.opening_id = opening_id
        self.uci = uci
        self.pgn = pgn
        self.parent = parent
        if children:
            self.children = children


class CountUpdater:
    '''
    Class to add counts (for games played, won, lost) from leaf openings to their respective base openings.

    Reason for update:
    As each opening is recorded based on the last move made, the probabilities of common base openings like Queen's Pawn
    are lower than their actual numbers. They are only counted if all moves after the base deviate from known openings
    '''
    def __init__(self, df: DataFrame, root_node: OpeningNode):
        self.full_df = df
        self.root_node = root_node
        self.cols = ['count_w', 'won_w', 'lost_w', 'count_b', 'won_b', 'lost_b']

    def update_player_counts(self, player_id: str):
        '''
        Update the counts (for games played, won, lost) for a given player by traversing through the tree of openings.
        :param player_id:   ID of the player on lichess; String
        '''
        # Get the part of the full_df with data specific to the player
        self.df = self.full_df.loc[self.full_df['id'] == player_id]

        # Get all unique openings played and use them to identify the base openings for that player
        # Base openings are those for which the player played no parent opening
        candidate_list = self.df.matched_id.unique().tolist()
        _, base_list = self._identify_base_openings(node=self.root_node,
                                                    candidate_list=candidate_list,
                                                    base_list=[])

        # Update the counts by going down the opening tree from each opening in the base_list
        for node in base_list:
            self._update_subtree_count(node=node, player_id=player_id)

        # Concat with the full dataframe and remove duplicates
        self.full_df = (concat([self.full_df, self.df])
                        .drop_duplicates(['id', 'matched_id'], keep='last')
                        .sort_values(['id', 'matched_id'], ascending=False)
                        .reset_index(drop=True))

    def _update_subtree_count(self, node: OpeningNode, player_id: str):
        '''
        Recursive function to update the counts starting from a specified node
        :param node:            Current node; Instance of OpeningNode
        :param player_id:       ID of the player on lichess; String
        :return:                Numpy array with the sum of the current counts and all sub_counts
        '''
        # Get the summed array of counts for all openings with the same initial moves as the current opening
        sub_counts = np.zeros(6)
        for child in node.children:
            sub_counts = np.add(sub_counts, self._update_subtree_count(node=child, player_id=player_id))

        # Get the count array for the current opening and add it to the sub_counts
        row = self.df.loc[self.df['matched_id'] == node.opening_id]
        own_count = np.zeros(6) if row.shape[0] == 0 else row[self.cols].values
        result = np.add(sub_counts, own_count)

        # Update the DataFrame with the new row if the result contains positive values
        if np.sum(result) > 0 and node.opening_id != '':
            df_new = DataFrame(data=[[player_id, node.opening_id, *result[0].tolist()]],
                               columns=['id', 'matched_id', *self.cols])

            # Insert a new row or update the existing one
            self.df = (concat([self.df, df_new])
                       .drop_duplicates(['matched_id'], keep='last')
                       .sort_values(['matched_id'], ascending=False)
                       .reset_index(drop=True))

        return result.copy()

    def _identify_base_openings(self, node, candidate_list, base_list, in_parent_branch=False):
        '''
        Recursive function to identify which openings from a list of candidates have no parent nodes in the tree
        :param node:                Current node; Instance of OpeningNode
        :param candidate_list:      List of opening IDs (str)
        :param base_list:           List of instances of OpeningNode
        :param in_parent_branch:    Whether a parent was already identified for the current subtree
        :return:
        '''
        if node.opening_id in candidate_list:
            candidate_list.remove(node.opening_id)
            if not in_parent_branch:
                base_list.append(node)
                in_parent_branch = True

        for child in node.children:
            candidate_list, base_list = self._identify_base_openings(child, candidate_list, base_list, in_parent_branch)

        return candidate_list, base_list


# ============================================================================
# DataFrame loading
# ============================================================================
class OpeningLoader:
    def __init__(self, path: Path = DATA_DIRECTORY / 'openings'):
        self.path = path
        self.df_path = path / 'openings.csv'

        self._load_df()
        self._create_tree()

    def get_root_node(self) -> OpeningNode:
        return self.tree.node

    def get_heritage_df(self):
        '''
        Creates a DataFrame with columns 'child_id' and 'parent_id' that records which event is the parent
        to another opening (Meaning the child opening starts with the moves of the parent)
        :return:    The DataFrame with heritage information
        '''
        base_openings = self.get_root_node().children
        self.heritage_df = DataFrame(columns=['child_id', 'parent_id'])

        for node in base_openings:
            self._update_heritage_df(node=node, parent_id=None)

        return self.heritage_df

    def get_color(self, opening_id: str = 'B00-2039-2459') -> str:
        df = self.df.copy(deep=True)

        # Find the right-most instance of a space.
        # If the char left of it is a point, the last move was made by white, otherwise by black
        pgn = df.loc[df['id'] == opening_id]['pgn'].values[0]
        space_pos = pgn.rfind(' ')

        return 'w' if pgn[space_pos - 1] == '.' else 'b'

    def _update_heritage_df(self, node: OpeningNode, parent_id: str):
        new_row = DataFrame(data=[[node.opening_id, parent_id]],
                            columns=['child_id', 'parent_id'])
        self.heritage_df = concat([self.heritage_df, new_row])
        for child in node.children:
            self._update_heritage_df(child, node.opening_id)


    def _create_tree(self):
        root_node = OpeningNode(opening_id='', uci='', pgn='')
        current_node = root_node
        for uci, pgn, opening_id in self.df.sort_values('uci')[['uci', 'pgn', 'id']].values:
            while not uci.startswith(current_node.uci):
                current_node = current_node.parent
            current_node = OpeningNode(opening_id=opening_id, uci=uci, pgn=pgn, parent=current_node)

        self.tree = RenderTree(root_node)

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



