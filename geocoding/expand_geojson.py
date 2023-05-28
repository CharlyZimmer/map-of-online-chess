import json
import os
import pandas as pd
import tqdm

def add_player_count(df_path: str = './geocoding/player_data_lookup_only_positive.parquet.gzip',
                     geojson_path: str ='./map/static/json/countries.geojson'
                     ):
    '''
    Update the countries.geojson file used for map visualization by adding player counts to each country feature
    :param df_path:         Path to the Dataframe containing player data
    :param geojson_path:    Path to the geoJSON to be updated
    '''
    if not os.path.isfile(df_path):
        print(f'No file found under {df_path}. Please run "make parse_countries" as specified in the README.')
        exit(1)

    if not os.path.isfile(geojson_path):
        _download_geojson(geojson_path)

    # Load relevant data
    player_df = pd.read_parquet(df_path)
    with open(geojson_path, 'r') as file:
        country_json = json.load(file)

    # Count the players per country and turn the result into a dictionary
    player_count = player_df.groupby('ISO_A3')['username'].count()
    country_dict = {country: count for country, count in zip(player_count.index, player_count)}

    # Add the player count to each country
    for country_feature in tqdm.tqdm(country_json['features'], desc='Iterating over all countries'):
        properties = country_feature['properties']
        ISO_A3 = properties['ISO_A3']
        properties['PLAYER_COUNT'] = country_dict.get(ISO_A3, 0)

    # Save the updated GEOJSON
    with open(geojson_path, 'w') as file:
        json.dump(country_json, file)
    print(f'Saved geoJSON with player counts to {geojson_path}\n')



def _download_geojson(geojson_path: str):
    import wget
    print(f'No file {geojson_path} found. Downloading countries.geojson to the specified path...')
    _ = wget.download(url='https://datahub.io/core/geo-countries/r/countries.geojson',
                      out=geojson_path)
    print('\n')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--df_path', type=str)
    args = parser.parse_args()

    print('\n' + '-' * 50)
    print(f'Adding player counts to the countries.geoJSON')
    print('-' * 50)
    if args.df_path is not None:
        add_player_count(df_path=args.df_path)
    else:
        add_player_count()