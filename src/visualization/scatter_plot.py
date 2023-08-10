from adjustText import adjust_text
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse
from pandas import read_parquet
import seaborn as sns
import pandas as pd
from typing import Tuple

from src import DATA_DIRECTORY
from src.parse.utils.openings import OpeningLoader


class Plotter:
    def __init__(self, country_parquet: str):
        country_df = read_parquet(DATA_DIRECTORY / f'output/countries/{country_parquet}')
        known_countries_df = read_parquet(str(DATA_DIRECTORY / 'output/countries/known_countries.parquet'))
        known_countries = known_countries = {
            row['ISO_A2']: row['country'] for row in known_countries_df[['ISO_A2', 'country']].to_dict(orient='records')
        }
        self.country_df = country_df.loc[country_df['country'].isin(known_countries.keys())].copy(deep=True)
        self.country_df['country'] = self.country_df['country'].apply(lambda iso_a2: known_countries[iso_a2])

        mean_std_file = country_parquet.replace('.p', '_mean_std.p')
        self.global_df = read_parquet(DATA_DIRECTORY / f'openings/{mean_std_file}')

        self.opening_df = OpeningLoader().df

    def plot_opening(self, opening_id: str = 'B00-2039-2459', x_lim: Tuple = None, y_lim: Tuple = None,
                     add_ellipse=False):
        # 1. Identify which color made the last move and get the name of the opening
        color = self._identify_color(opening_id)
        name = self.opening_df.loc[self.opening_df['id'] == opening_id]['name'].values[0]

        # 2. Filter the country_df and get mean and stddev values
        filtered_df = self.country_df.loc[self.country_df['matched_id'] == opening_id] \
            [['country', f'p_{color}', f'p_won_{color}']]
        mean_p, stddev_p, mean_p_won, stddev_p_won = self._get_global_mean_std(opening_id=opening_id, color=color)

        # 3. Plot the result
        fig = plt.figure()
        ax = plt.subplot(111)

        # Add points for countries and for the global mean
        sns.scatterplot(data=filtered_df, x=f'p_{color}', y=f'p_won_{color}', ax=ax)
        sns.scatterplot(x=[mean_p], y=[mean_p_won], ax=ax)

        # Add labels to the five countries with the highest combined deviation from the global mean
        outlier_df = filtered_df.loc[(abs(filtered_df[f'p_{color}'] - mean_p) >= stddev_p) |
                                     (abs(filtered_df[f'p_won_{color}'] - mean_p_won) >= stddev_p_won)]

        labels = _label_point(outlier_df[f'p_{color}'], outlier_df[f'p_won_{color}'], outlier_df[f'country'])
        if len(labels) > 0:
            adjust_text(labels, only_move={'points': 'y', 'texts': 'y'})

        # Optionally add an ellipse for global standard deviations
        if add_ellipse:
            _add_stddev_ellipse(ax,
                                x_mean=mean_p,
                                y_mean=mean_p_won,
                                stddev_x=stddev_p,
                                stddev_y=stddev_p_won,
                                edgecolor='darkorange')

        # Axis labels and limits 
        ax.set_xlabel('Probability playing (%)')
        ax.set_ylabel('Probability winning (%)')

        if x_lim is not None:
            ax.set_xlim(x_lim)
        if y_lim is not None:
            ax.set_ylim(y_lim)

        plt.title(name)
        plt.show()

    def _identify_color(self, opening_id: str = 'B00-2039-2459') -> str:
        df = self.opening_df.copy(deep=True)

        # Find the right-most instance of a space.
        # If the char left of it is a point, the last move was made by white, otherwise by black
        pgn = df.loc[df['id'] == opening_id]['pgn'].values[0]
        space_pos = pgn.rfind(' ')

        return 'w' if pgn[space_pos - 1] == '.' else 'b'

    def _get_global_mean_std(self, opening_id: str = 'B00-2039-2459', color: str = 'w'):
        df = self.global_df.loc[self.global_df['matched_id'] == opening_id] \
            [[f'mean_p_{color}', f'std_p_{color}', f'mean_p_won_{color}', f'std_p_won_{color}']]
        return df.values[0].tolist()


def _add_stddev_ellipse(ax, x_mean: float, y_mean: float, stddev_x: float, stddev_y: float,
                        facecolor='none', **kwargs) -> Ellipse:
    '''
    Adapted from https://matplotlib.org/stable/gallery/statistics/confidence_ellipse.html
    Create an ellipse that reflects the global standard deviation for playing / winning with an opening
    :param ax:          Instance of matplotlib.axes.Axes
    :param x_mean:      Global mean for playing an opening
    :param y_mean:      Global mean for winning after playing an opening
    :param stddev_x:    Standard deviation for playing an opening
    :param stddev_y:    Standard deviation for winning after playing an opening

    :return:             An ellipse added to the ax
    '''
    ellipse = Ellipse((x_mean, y_mean),
                      width=2 * stddev_x,
                      height=2 * stddev_y,
                      facecolor=facecolor, **kwargs)

    return ax.add_patch(ellipse)


def _label_point(x, y, label):
    '''
    Add labels to points in a plot
    '''
    labels = []
    tmp_df = pd.concat({'x': x, 'y': y, 'label': label}, axis=1)
    for _, point in tmp_df.iterrows():
        labels.append(plt.text(point['x'], point['y'], str(point['label'])))
    return labels