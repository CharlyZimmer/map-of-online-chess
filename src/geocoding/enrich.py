import itertools
import json
import os
from pandas import merge, read_parquet
from tqdm import tqdm

from src import DATA_DIRECTORY
from src.parse.utils.openings import OpeningLoader

class EnrichGeoJSON:
    def __init__(self, country_openings_parquet: str = "test_cleaned_openings.parquet"):
        '''
        :param country_openings_parquet:    File name of the parquet containing country-opening combinations
        '''
        countries_path = DATA_DIRECTORY / f'output/countries'
        json_dir = DATA_DIRECTORY.parent / 'map/static/json'

        # Define paths
        self.country_openings_path = countries_path / country_openings_parquet
        self.known_countries_path: str = countries_path / "known_countries.parquet"
        self.geojson_path = json_dir / 'countries.geojson'
        self.meta_json_path = json_dir / 'enrichment_metadata.json'

        # Load initial data
        self._load_json_files()
        self._load_and_filter_df()

    def get_color_positions(self, max_val: float = 4.0, min_val: float = -4.0, num_positions: int = 400):
        value_range = max_val - min_val
        step_size = value_range / num_positions

        # Update the meta_json with number of positive and negative positions (relevant for color coding)
        self.meta_json['positive_positions'] = round(abs(max_val) / value_range * num_positions)
        self.meta_json['negative_positions'] = round(abs(min_val) / value_range * num_positions)

        suffixes = [''.join(tup) for tup in itertools.product(['', 'won_'], ['w', 'b'])]
        for s in suffixes:
            self.country_df[f'pos_{s}'] = (self.country_df[f'stand_p_{s}']
                                           .apply(lambda p: self._get_position(p=p,
                                                                               min_val=min_val,
                                                                               step_size=step_size,
                                                                               num_positions=num_positions)))

    def add_openings(self):
        known_openings = self.country_df.groupby('matched_id')['matched_id'].count().index.tolist()
        df = OpeningLoader().df
        metadata = {}

        # Replace any NaN values to create a correct JSON
        self.country_df.fillna(value=0, inplace=True)

        suffixes = [''.join(tup) for tup in itertools.product(['', 'won_'], ['w', 'b'])]
        for opening in tqdm(known_openings, desc=f'Adding openings to geojson'):
            opening_df = self.country_df.loc[self.country_df['matched_id'] == opening]

            # Loop over all suffixes for the opening (probability of playing/winning for white and black)
            for s in suffixes:
                opening_dict = {
                    country: (position, probability)
                    for country, position, probability in
                    zip(opening_df.country, opening_df[f'pos_{s}'], opening_df[f'p_{s}'])
                }
                for country_feature in self.country_geojson['features']:
                    properties = country_feature['properties']

                    # If no information is available for a country, return 0, 0
                    country_tuple = opening_dict.get(properties['ISO_A3'], (0, 0))
                    properties[f'{opening}_POS_{s.upper()}'] = country_tuple[0]
                    properties[f'{opening}_{s.upper()}'] = country_tuple[1]

                # Save moves and name of the opening for the metadata
                row = df.loc[df['id'] == opening]
                metadata[opening] = {'name': row['name'].values[0], 'pgn': row['pgn'].values[0]}

        # Add moves and display name to openings in meta_json
        self.meta_json['openings'] = metadata


    def add_player_counts(self):
        count_dict = self.player_counts.set_index('country')['num_players'].to_dict()
        for country_feature in self.country_geojson['features']:
            properties = country_feature['properties']
            properties['PLAYER_COUNT'] = count_dict.get(properties['ISO_A3'], 0)

    def _load_and_filter_df(self):
        # 1. Load the parquet files
        country_df = read_parquet(self.country_openings_path)
        player_counts = read_parquet(str(self.country_openings_path).replace('.p', '_player_count.p'))
        known_country_df = read_parquet(self.known_countries_path)
        self.meta_json['num_countries'] = len(known_country_df)

        # 2. Add player counts and ISO_A3 string to the country df; ISO_A3 string to player_counts
        country_df = merge(country_df, player_counts, on='country', how='left')
        map_dict = known_country_df.set_index('ISO_A2')['ISO_A3'].to_dict()
        country_df['country'] = country_df['country'].apply(lambda iso_a2: map_dict.get(iso_a2, None))

        player_counts['country'] = player_counts['country'].apply(lambda iso_a2: map_dict.get(iso_a2, None))

        self.country_df = country_df.dropna(subset='country')
        self.player_counts = player_counts.dropna(subset='country')

    def _load_json_files(self):
        if not os.path.isfile(self.geojson_path):
            import wget
            print(f'No file {self.geojson_path} found. Downloading countries.geojson to the specified path...')
            _ = wget.download(url='https://datahub.io/core/geo-countries/r/countries.geojson',
                              out=str(self.geojson_path))
            print('\n')

        with open(self.geojson_path, 'r') as file:
            self.country_geojson = json.load(file)

        self.meta_json = {}
        if os.path.isfile(self.meta_json_path):
            with open(self.meta_json_path, 'r') as file:
                self.meta_json = json.load(file)

    def _save_json_files(self):
        with open(self.geojson_path, 'w') as file:
            json.dump(self.country_geojson, file)
        with open(self.meta_json_path, 'w') as file:
            json.dump(self.meta_json, file)

    @staticmethod
    def _get_position(p: float, min_val: float, step_size: float, num_positions: int):
        try:
            return min(num_positions + 1, max(round((p - min_val) / step_size) + 1, 1))
        except:
            return 0

    def run(self):
        '''
        Add additional information to each country feature
        - Player count: How many players were counted for that country
        - Openings:
            * Average probability for an opening played per country of origin.
            * Position indicating if the probability is higher or lower than the global average
        '''
        # Get positions for each country opening combination
        self.get_color_positions()

        # Expand the geojson
        self.add_openings()
        self.add_player_counts()

        # Save the updated json files
        self._save_json_files()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', type=str, required=True)
    args = parser.parse_args()

    file_name = args.parquet_file

    print('\n' + '-' * 50)
    print(f"Enriching countries.geoJSON based on {file_name}")
    print('-' * 50)
    enricher = EnrichGeoJSON(file_name)
    enricher.run()