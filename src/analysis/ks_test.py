import os
import pandas as pd
from scipy.stats import kstest
from tqdm import tqdm

from src import DATA_DIRECTORY


def _ks_test_pair(x, y):
    '''
    Apply the two-sample Kolmogorov-Smirnov-Test to two arrays
    :param x:   First distribution; Array of floats
    :param y:   Second distribution; Array of floats
    :return:    Dictionary with the following keys
                - stat:     Statistic of the Kolmogorov-Smirnov-Test
                - p_value:  p-Value of the test statistic
                - sign:     Sign of the two-sample test,
                            +1 if the empirical dist. func. of x exceeds the empirical dist. func. of y
                            Otherwise -1.
    :param x:
    :param y:
    :return:
    '''
    try:
        ks = kstest(x, y)
        result = {'stat': ks.statistic, 'p_value': ks.pvalue, 'sign': ks.statistic_sign}
    except:
        result = {'stat': None, 'p_value': None, 'sign': None}
    return result


def apply_ks_test(row, global_array):
    '''
    Apply the Kolmogorov-Smirnov-Test to different distributions for one country-opening combination
    :param row:             pandas Series with values for
                            - p_w:      Probability distribution for playing the opening as white
                            - p_b:      Probability distribution for playing the opening as black
                            - p_won_w:  Probability distribution for winning as white if the opening was played
                            - p_won_b:  Probability distribution for winning as black if the opening was played
    :param global_array:    Array with four sub-arrays that contain the global distributions to be used as comparison
                            in the KS-Test
    :return:                Pandas Series with the values [PREFIX]_stat, [PREFIX]_p_value, and [PREFIX]_sign.
                            Prefixes are the four columns of the row.
    '''

    p_w_g, p_b_g, p_won_w_g, p_won_b_g = global_array[0]

    output = {'matched_id': row['matched_id'], 'country': row['country']}
    for prefix in ['p_w', 'p_b', 'p_won_w', 'p_won_b']:
        test_dict = {
            f'{prefix}_{key}': val for key, val in
            _ks_test_pair(row[f'{prefix}_dist'], eval(f'{prefix}_g')).items()
        }
        output = {**output, **test_dict}

    return pd.Series(output)
def run(file_name: str = 'test_cleaned.parquet'):
    # 1. Prepare folders
    in_path = DATA_DIRECTORY / f'analysis/distributions/{file_name}'
    out_dir = DATA_DIRECTORY / f'analysis/ks_test'
    out_path = out_dir / file_name
    os.makedirs(os.path.dirname(out_dir), exist_ok=True)

    # 2. Read the dataframes
    df_g = pd.read_parquet(str(in_path).replace('.p', '_global.p'))
    df_c = pd.read_parquet(str(in_path).replace('.p', '_country.p'))
    openings = df_g.matched_id.unique()

    # 3. Apply the Kolmogorov-Smirnov-Test to all country-opening combinations
    result_df = pd.DataFrame()
    for opening in tqdm(openings):
        # Restrict to global array and countries based on the opening
        opening_df = df_c.loc[df_c.matched_id == opening]
        global_array = df_g.loc[df_g.matched_id == opening][['p_w_dist_global',
                                                             'p_b_dist_global',
                                                             'p_won_w_dist_global',
                                                             'p_won_b_dist_global']].values

        iteration_df = opening_df.apply(lambda row: apply_ks_test(row, global_array), axis=1)
        result_df = pd.concat([result_df, iteration_df])

    # 4. Save the result
    result_df = result_df[result_df.columns.drop(list(result_df.filter(regex='_dist')))]
    result_df.to_parquet(str(out_path))

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f'Apply two sample Kolmogorov-Smirnov-Test to opening distributions of {file_name}')
    print('-' * 50)
    run(file_name=file_name)