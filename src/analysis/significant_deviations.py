import os
from pandas import concat, DataFrame, merge, read_parquet

from src import DATA_DIRECTORY
from src.parse.utils.openings import OpeningLoader

class Filterer:
    def __init__(self, file_name: str = 'test_cleaned.parquet'):
        # Set the opening loader
        self.opening_loader = OpeningLoader()

        # Define the out_path
        self.out_path = DATA_DIRECTORY / f'analysis/significant_openings/{file_name}'
        os.makedirs(os.path.dirname(self.out_path), exist_ok=True)

        # Build the Dataframes by
        # - Merging results of the Kolmogorov-Smirnov-Test with deviations from mean
        # - Adding the color that made the last move to each row
        # - Splitting by color
        ks_test_df = read_parquet(DATA_DIRECTORY / f'analysis/ks_test/{file_name}')
        country_df = read_parquet(DATA_DIRECTORY / f'output/countries/{file_name}')
        tmp_df = ks_test_df.merge(country_df, how='outer', on=['country', 'matched_id'])

        openings = tmp_df.matched_id.unique()
        color_df = DataFrame.from_dict({o: [self.opening_loader.get_color(o)] for o in openings},
                                       orient='index').reset_index().rename(
            columns={0: 'color', 'index': 'matched_id'})

        tmp_df = tmp_df.merge(color_df, on='matched_id')

        self.df_w = tmp_df.loc[tmp_df['color'] == 'w'].copy(deep=True)
        self.df_b = tmp_df.loc[tmp_df['color'] == 'b'].copy(deep=True)


    def get_significant_openings(self, mode: str = 'played', color='w',
                                 alpha: float = 0.01, min_dev: float = 1.0) -> DataFrame:
        '''
        Filter a Dataframe of county-opening-combinations with requirements of significance and effect size
        :param mode:        'played' or 'won'; Which probabilities to filter for
        :param color:       'w' or 'b'; Which color of opening to filter for
        :param alpha:       Desired level of significance
        :param min_dev:     Minimum number of standard deviations from global mean (effect size)
        :return:            DataFrame with country-opening-combinations that meet the requirements
        '''

        # Ensure correct input
        if mode not in ['played', 'won']:
            print('Please provide either "played" or "won" as mode.')
            return DataFrame()
        if color not in ['w', 'b']:
            print('Please provide either "w" or "b" as color.')
            return DataFrame()

        # Select color dataframe
        df = (self.df_w if color == 'w' else self.df_b).copy(deep=True)

        # Set the columns to be selected from self.df_w/self.df_b and how to rename them
        # (Duplicates for column selection)
        cols = {
            'matched_id': 'matched_id',
            'country': 'country',
            f'p_{color}_p_value': 'p_val_played',
            f'stand_p_{color}': 'dev_played',
            f'p_won_{color}_p_value': 'p_val_won',
            f'stand_p_won_{color}': 'dev_won',
            'color': 'color'
        }
        df = df.rename(columns=cols)

        # Filter the DataFrame for relevant rows and columns, add opening name and moves, and return it
        df = df.loc[(df[f'p_val_{mode}'] < alpha) & (abs(df[f'dev_{mode}']) > min_dev)][list(cols.values())]
        df = merge(df, self.opening_loader.df[['id', 'name', 'pgn']].set_index('id'),
                   left_on='matched_id', right_index=True)
        return df

    def create_dataframes(self):
        '''
        Iterate over all four combinations of mode and color and save the result as two DataFrames:
        1. DataFrame for playing
        2. DataFrame for winning/losing
        '''
        for mode in ['played', 'won']:
            df = DataFrame()
            for color in ['w', 'b']:
                df = concat([df, self.get_significant_openings(mode=mode, color=color)], axis=0)

            df.sort_values(f'dev_{mode}', ascending=False, inplace=True)
            df.to_csv(str(self.out_path).replace('.parquet', f'_{mode}.csv'))


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f'Identify significant country-opening-combinations based on {file_name}')
    print('-' * 50)
    filterer = Filterer(file_name=file_name)
    filterer.create_dataframes()