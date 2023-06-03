import json
import os
import pandas as pd
import tqdm

class EnrichGeoJSON:
    def __init__(self, df_path: str = './geocoding/player_data_lookup_only_positive.parquet.gzip',
                geojson_path: str ='./map/static/json/countries.geojson'):
        '''
        Initialize a EnrichGeoJSON instance using paths to the player dataframe and countries.geojson
        :param df_path:         Path to the Dataframe containing player data
        :param geojson_path:    Path to the geoJSON to be updated
        :return:
        '''
        if not os.path.isfile(df_path):
            print(f'No file found under {df_path}. Please run "make parse_countries" as specified in the README.')
            exit(1)

        if not os.path.isfile(geojson_path):
            self._download_geojson(geojson_path)

        self.geojson_path = geojson_path
        self.player_df = pd.read_parquet(df_path)
        with open(geojson_path, 'r') as file:
            self.country_json = json.load(file)

        # Store num_countries to be referenced by the map application
        self.meta_json = {}
        self.meta_json_path = '/'.join(self.geojson_path.split('/')[:-1]) + '/enrichment_metadata.json'
        if os.path.isfile(self.meta_json_path):
            with open(self.meta_json_path, 'r') as file:
                self.meta_json = json.load(file)

        self.meta_json['num_countries'] = len(self.player_df.groupby('ISO_A3').count())

    def _download_geojson(self, geojson_path: str):
        import wget
        print(f'No file {geojson_path} found. Downloading countries.geojson to the specified path...')
        _ = wget.download(url='https://datahub.io/core/geo-countries/r/countries.geojson',
                          out=geojson_path)
        print('\n')

    def add_player_count(self):
        '''
        Update the countries.geojson file used for map visualization by adding player counts to each country feature
        '''
        # Count the players per country and turn the result into a dictionary
        player_count = self.player_df.groupby('ISO_A3')['username'].count()
        country_dict = {country: count for country, count in zip(player_count.index, player_count)}

        # Add the player count to each country
        for country_feature in tqdm.tqdm(self.country_json['features'], desc='Adding player counts to countries'):
            properties = country_feature['properties']
            properties['PLAYER_COUNT'] = country_dict.get(properties['ISO_A3'], 0)

    def add_e4_d4_split(self):
        '''
        Add the average share of E4 and D4 openings played per country of origin. Also add a position indicating if
        E4 or D4 dominates in comparison with the global averages.
        '''
        opening_df = self._calculate_e4d4_split()

        self.meta_json['d4_positions'] = opening_df.loc[opening_df['e4_share_standardized'] <= 0]['position'].max() - 1
        self.meta_json['e4_positions'] = opening_df['position'].max() - self.meta_json['d4_positions']

        opening_dict = {
            country: (position, e4, d4) for country, position, e4, d4
            in zip(opening_df.index,
                   opening_df.position,
                   opening_df.e4,
                   opening_df.d4)
                        }

        # Add the opening information to each country
        for country_feature in tqdm.tqdm(self.country_json['features'], desc='Adding e4/d4 information to countries'):
            properties = country_feature['properties']

            # If no information is available for a country, return 0, 0, 0
            country_tuple = opening_dict.get(properties['ISO_A3'], (0, 0, 0))
            properties['E4_D4_POS'] = country_tuple[0]
            properties['E4'] = country_tuple[1]
            properties['D4'] = country_tuple[2]


    def _calculate_e4d4_split(self) -> pd.DataFrame:
        '''
        Function to create a DataFrame for the E4/D4 split on country level.
        - Calculate the average share of E4 and D4 openings played per country of origin
        - Calculate the relative E4 share (only considering E4 and D4 share)
        - Standardize the E4 share to have negative values for countries with D4 and positive values for countries
          with E4 preference
        - Assign positions to each country that will be used in color coding

        Currently averages on player level, meaning that the number of games per player is not regarded
        (TODO: Potentially update this)
        Dummy function; Will be used to add actual E4/D4 information per country to the countries.geojson
        :return:    A DataFrame with four columns: e4_share_standardized, position, country_e4, country_d4
        '''
        # TODO: Remove in favor of correct approach
        import random
        def random_e4_d4(row):
            e4 = max(0.10, min(1.0, random.gauss(0.6, 0.10)))
            row['e4'] = e4
            row['d4'] = 0.9 - e4
            return row

        self.player_df = self.player_df.apply(random_e4_d4, axis=1)

        country_e4 = self.player_df.groupby('ISO_A3')['e4'].mean()
        country_d4 = self.player_df.groupby('ISO_A3')['d4'].mean()

        # TODO: The following code can probably stay the same (Confirm)
        # Bring values on one scale and standardize using mean and std:
        # - Positive values: e4 is more popular -> Number stored in num_e4
        # - Negative values: d4 is more popular -> Number stored in num_d4
        e4_share = country_e4 / (country_e4 + country_d4)

        # TODO: Calculate mean and standard deviation over all games instead of aggregated by player and by country
        e4_share_standardized = ((e4_share - e4_share.mean()) / e4_share.std()).sort_values()

        # Get the position corresponding to a normalized share
        positions = self._assign_pos_to_share(share_series=e4_share_standardized)

        # Combine all information into a single df
        opening_df = pd.concat([e4_share_standardized, positions, country_e4, country_d4], axis=1)
        opening_df.rename(columns={0: 'e4_share_standardized'}, inplace=True)
        opening_df.rename(columns={1: 'position'}, inplace=True)

        return opening_df


    def add_opening(self, name: str ='SICILIAN_DEFENSE'):
        '''
        Add the average probability for an opening played per country of origin. Also add a position indicating if
        the probability is higher or lower than the global average.
        '''
        opening_df = self._calculate_opening(name)

        self.meta_json['openings'] = self.meta_json.get('openings', []) + [name]
        self.meta_json[f'{name}_neg_positions'] = opening_df.loc[opening_df['standardized_prob'] <= 0]['position'].max() - 1
        self.meta_json[f'{name}_pos_positions'] = opening_df['position'].max() - self.meta_json[f'{name}_neg_positions']

        opening_dict = {
            country: (position, prob) for country, position, prob
            in zip(opening_df.index,
                   opening_df.position,
                   opening_df[name])
                        }

        # Add the opening information to each country
        for country_feature in tqdm.tqdm(self.country_json['features'], desc=f'Adding {name} probabilities to countries'):
            properties = country_feature['properties']

            # If no information is available for a country, return 0, 0
            country_tuple = opening_dict.get(properties['ISO_A3'], (0, 0))
            properties[f'{name}_POS'] = country_tuple[0]
            properties[name] = country_tuple[1]


    def _calculate_opening(self, name: str ='SICILIAN_DEFENSE') -> pd.DataFrame:
        '''
        Function to create a DataFrame for the probability of a specific opening on country level.
        - Calculate the probability for that opening being played per country of origin
        - Standardize the probability to have negative values for countries with lower probability than global average
          and positive values for countries with a higher probability
        - Assign positions to each country that will be used in color coding

        Currently averages on player level, meaning that the number of games per player is not regarded
        (TODO: Potentially update this)
        Dummy function; Will be used to add actual opening information per country to the countries.geojson
        :return:    A DataFrame with three columns: standardized_prob, position, [name]
        '''
        # TODO: Remove in favor of correct approach
        import random
        def random_prob(row):
            row[name] = max(0.05, min(0.20, random.gauss(0.125, 0.05)))
            return row

        self.player_df = self.player_df.apply(random_prob, axis=1)
        prob = self.player_df.groupby('ISO_A3')[name].mean()

        # TODO: The following code can probably stay the same (Confirm)
        # Bring values on one scale and standardize using mean and std:
        # - Positive values: Opening is more popular than global average
        # - Negative values: Opening is less popular than global average

        # TODO: Calculate mean and standard deviation over all games instead of aggregated by player and by country
        standardized_prob = ((prob - prob.mean()) / prob.std()).sort_values()

        # Get the position corresponding to a normalized share
        positions = self._assign_pos_to_share(share_series=standardized_prob)

        # Combine all information into a single df
        opening_df = pd.concat([standardized_prob, positions, prob], axis=1)
        opening_df.columns = ['standardized_prob', 'position', name]
        return opening_df

    @staticmethod
    def _assign_pos_to_share(share_series: pd.Series, num_positions: int = 500):
        share_max = share_series.max()
        share_min = share_series.min()
        diff = share_max - share_min
        step = diff / num_positions

        # Return the rounded value as position (1 is added as position 0 is reserved for unknown values)
        return share_series.apply(lambda x: round((x-share_min)/step)+1)




    def save_geojson(self):
        # Save the updated GEOJSON
        with open(self.geojson_path, 'w') as file:
            json.dump(self.country_json, file)
        with open(self.meta_json_path, 'w') as file:
            json.dump(self.meta_json, file)

        print(f'Saved enriched geoJSON to {self.geojson_path}\n')



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--df_path', type=str)
    args = parser.parse_args()

    print('\n' + '-' * 50)
    print(f'Enriching countries.geoJSON with player counts and E4/D4 data')
    print('-' * 50)
    if args.df_path is not None:
        enricher = EnrichGeoJSON(df_path=args.df_path)
    else:
        enricher = EnrichGeoJSON()

    enricher.add_player_count()
    enricher.add_e4_d4_split()

    openings = ['SICILIAN_DEFENSE', 'LATVIAN_GAMBIT', 'PIZZA_MAFIA_MANDOLINO']
    for opening in openings:
        enricher.add_opening(opening)

    enricher.save_geojson()