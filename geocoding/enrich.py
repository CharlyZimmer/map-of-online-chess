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
        Assigns each country a percentage for e4 and d4 openings.
        Currently averages on player level, meaning that the number of games per player is not regarded
        (TODO: Potentially update this)
        Dummy function; Will be used to add actual E4/D4 information per country to the countries.geojson
        '''
        # TODO: Remove in favor of correct approach
        import random
        def random_e4_d4(row):
            e4 = max(0.15, min(1.0, random.gauss(0.6, 0.1)))
            row['e4'] = e4
            row['d4'] = 0.85 - e4
            return row
        self.player_df = self.player_df.apply(random_e4_d4, axis=1)

        country_e4 = self.player_df.groupby('ISO_A3')['e4'].mean()
        country_d4 = self.player_df.groupby('ISO_A3')['d4'].mean()

        # TODO: Check but, the following code can probably stay the same
        # Bring values on one scale and normalize using mean and std:
        # - Positive values: e4 is more popular
        # - Negative values: d4 is more popular
        e4_share = country_e4 / (country_e4 + country_d4)
        norm_e4_share = ((e4_share - e4_share.mean()) / e4_share.std()).sort_values()

        # Combine all information into a single df and create a dictionary to update the json
        opening_df = pd.concat([norm_e4_share, country_e4, country_d4], axis=1)
        opening_dict = {country: (position, e4, d4)
                        for country, position, e4, d4 in zip(opening_df.index,
                                                             range(0, len(opening_df)),
                                                             opening_df.e4,
                                                             opening_df.d4)}

        # TODO: Output number of countries to reference midpoint in map application
        mid_point = len(opening_df) // 2

        # Add the opening information to each country
        for country_feature in tqdm.tqdm(self.country_json['features'], desc='Adding e4/d4 information to countries'):
            properties = country_feature['properties']
            country_tuple = opening_dict.get(properties['ISO_A3'], (mid_point, 'n/a', 'n/a'))
            properties['E4_D4_POS'] = country_tuple[0]
            properties['E4'] = country_tuple[1]
            properties['D4'] = country_tuple[2]



    def save_geojson(self):
        # Save the updated GEOJSON
        with open(self.geojson_path, 'w') as file:
            json.dump(self.country_json, file)
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
    enricher.save_geojson()