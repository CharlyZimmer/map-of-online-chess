import json
import os
from pandas import merge, read_parquet
from tqdm import tqdm

from src import DATA_DIRECTORY

class EnrichGeoJSON:
    def __init__(self, country_openings_parquet: str = "test_cleaned_openings.parquet.gzip"):
        '''
        :param country_openings_parquet:    File name of the parquet containing country-opening combinations
        '''
        countries_path = DATA_DIRECTORY / f'output/countries'
        json_dir = DATA_DIRECTORY.parent / 'map/static/json'

        # Define paths
        self.country_openings_path = countries_path / country_openings_parquet
        self.known_countries_path: str = countries_path / "known_countries.parquet.gzip"
        self.geojson_path = json_dir / 'countries.geojson'
        self.meta_json_path = json_dir / 'enrichment_metadata.json'

        # Load initial data
        self._load_json_files()
        self._load_and_filter_df()

    def get_color_positions(self, min_players: int = 20_000, num_positions: int = 500):
        # Find the range of standardized values for countries with a minimum number of players
        value_range = self.country_df.loc[self.country_df['num_players'] > min_players]\
            .sort_values('standardized_share')['standardized_share']
        share_max = value_range.iloc[-1]
        share_min = value_range.iloc[0]
        share_range = share_max - share_min
        step_size = share_range / num_positions

        # Update the meta_json with number of positive and negative positions (relevant for color coding)
        self.meta_json['positive_positions'] = round(abs(share_max)/share_range*num_positions)
        self.meta_json['negative_positions'] = round(abs(share_min) / share_range * num_positions)

        # Assign each country/opening combination their position
        # - Outliers are set to min and max value
        # - 1 is added as position 0 is reserved for unknown values
        self.country_df['position'] = self.country_df['standardized_share']\
            .apply(lambda x: min(num_positions + 1, max(round((x - share_min) / step_size) + 1, 1)))

    def add_openings(self):
        known_openings = self.country_df.groupby('matched_name')['matched_name'].count().index.tolist()
        for opening in tqdm(known_openings, desc='Adding openings to geojson'):
            opening_df = self.country_df.loc[self.country_df['matched_name'] == opening]
            opening_dict = {
                country: (position, share)
                for country, position, share in
                zip(opening_df.country, opening_df.position, opening_df.share)
            }
            for country_feature in self.country_geojson['features']:
                properties = country_feature['properties']

                # If no information is available for a country, return 0, 0
                country_tuple = opening_dict.get(properties['ISO_A3'], (0, 0))
                properties[f'{opening}_POS'] = country_tuple[0]
                properties[opening] = country_tuple[1]

    def add_player_counts(self):
        # TODO: Remove the E4/D4 dummy in favor of correct data
        self.meta_json['d4_positions'] = 250
        self.meta_json['e4_positions'] = 250

        count_dict = self.player_counts.set_index('country')['num_players'].to_dict()
        for country_feature in self.country_geojson['features']:
            properties = country_feature['properties']
            properties['PLAYER_COUNT'] = count_dict.get(properties['ISO_A3'], 0)

            # TODO: Remove the E4/D4 dummy in favor of correct data
            properties['E4_D4_POS'] = 0
            properties['E4'] = 0
            properties['D4'] = 0

    def _load_and_filter_df(self):
        # 1. Load the parquet files
        country_df = read_parquet(self.country_openings_path)
        player_counts = read_parquet(str(self.country_openings_path).replace('openings', 'player_count'))
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