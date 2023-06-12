import json
import pandas as pd
from geopy.geocoders import Nominatim
import os
from pandas import DataFrame
from pycountry import countries
from ratelimiter import RateLimiter
from tqdm import tqdm
from typing import Dict, Union

from src import DATA_DIRECTORY

class JSONParser:
    def __init__(self, json_file: str = 'player_data_lookup_only_positive', user_agent: str = 'abc@test.de'):
        """
        Initialize the parser with the path to the JSON file and a user_agent for geocoding with Nominatim
        :param json_file:   Name JSON-file to be parsed (In the data/players directory)
        :param user_agent:  String to use for identification with Nominatim (An email-address suffices)
        """
        self.json_path = DATA_DIRECTORY / f'players/{json_file}'
        self.user_agent = user_agent

    def parse_player_json(self):
        """
        Turn the json data under self.json_path into a pandas DataFrame.
        The columns are 'username' and the set of all keys under 'profile'.

        Sets the self.df attribute.
        """
        with open(self.json_path, "r") as file:
            player_json = json.load(file)

        # Gather all available attributes about a player
        profile_attr = list(
            set([k for player in player_json for k in player["profile"].keys()])
        )

        # Turn the json into a dictionary of lists for each attribute
        player_dict = {attr: [] for attr in (["username"] + profile_attr)}
        for player in player_json:
            player_dict["username"].append(player.get("username", None))
            for attr in profile_attr:
                player_dict[attr].append(player["profile"].get(attr, None))

        # Turn the dictionary into a DataFrame and get the full country names and ISO_A3 identifiers
        self.df = DataFrame(player_dict)
        self.df.rename(columns={"country": "ISO_A2"}, inplace=True)
        self.df = self.df.apply(self._parse_country_attr, axis=1)

        print("Identified country names and ISO_A3 codes for each player.")

    @staticmethod
    def _parse_country_attr(row: pd.Series) -> pd.Series:
        """
        Parse the ISO_A2 (alpha_2) string of a country into its name and ISO_A3 string.
        The second is used for matching with the countries.geojson
        :param row:      Row of a pandas DataFrame
        :return:         Updated row with values for country and ISO_A3
        """
        try:
            country = countries.get(alpha_2=row["ISO_A2"])
            row["country"] = country.name
            row["ISO_A3"] = country.alpha_3
        except:
            row["country"] = None
            row["ISO_A3"] = None
        return row

    def get_country_ids(
        self, countries_parquet="known_countries.parquet.gzip"
    ):
        """
        Gets the Nominatim place_id of all countries in self.df.country.
        Known country information are reused and unknown requested via Nominatim

        :param countries_parquet:   (Optional) Path to the file containing country locations.
                                    Default: './data/geocoding/known_countries.parquet.gzip'
                                    Will be used to read known locations and write results after completion
        """
        countries_dir = DATA_DIRECTORY / 'countries'
        os.makedirs(countries_dir, exist_ok=True)
        parquet_path = countries_dir / countries_parquet

        if not hasattr(self, "df"):
            self.parse_player_json()

        # Preparation: Try to load known locations, set geocoder and rate limiter
        try:
            known_df = pd.read_parquet(str(parquet_path))
            known_countries = known_df.set_index("country").to_dict(orient="index")
        except:
            known_countries = {}

        geolocator = Nominatim(user_agent=self.user_agent)
        nominatim_rate_limiter = RateLimiter(max_calls=1, period=1)

        # Get all country locations with progress bar
        tqdm.pandas(desc="Identifying country locations")
        self.df["place_id"] = self.df["country"].progress_apply(
            lambda country: self._get_place_id(
                location_name=country,
                geolocator=geolocator,
                known_locations=known_countries,
                rate_limiter=nominatim_rate_limiter,
            )
        )

        # Save the known countries as a parquet file
        known_df = DataFrame(
            [{"country": country, **loc} for country, loc in known_countries.items()]
        )
        known_df.to_parquet(str(parquet_path), compression="gzip")

        print(
            f"Added Nominatim country IDs for all players and saved known countries to {parquet_path}."
        )

    @staticmethod
    def _get_place_id(
        location_name: str,
        geolocator: Nominatim,
        known_locations: Dict[str, Dict],
        rate_limiter: RateLimiter,
    ) -> Union[int, None]:
        """
        Return the place_id of a location string as coded by Nominatim.
        Known locations are reused. Requests to Nominatim are subject to the limits set in rate_limiter

        :param location_name:       String of the location to be coded
        :param geolocator:          Instance of geopy.geocoders.Nominatim
        :param known_locations:     Dictionary of LOCATION_NAME: geopy.Location
        :param rate_limiter:        Instance of ratelimiter.RateLimiter for Nominatim requests
        :return:                    Place ID of the location_name
        """
        if location_name is None:
            return None
        if location_name not in known_locations:
            with rate_limiter:
                known_locations[location_name] = geolocator.geocode(location_name).raw

        return known_locations[location_name]["place_id"]

    def write_df(self):
        if not hasattr(self, "df"):
            self.parse_player_json()
            self.get_country_ids()

        out_path = str(self.json_path).replace(".json", ".parquet.gzip")
        self.df.to_parquet(out_path, compression="gzip")
        print(f"Saved player data to {out_path}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--json_file", type=str, required=True)
    parser.add_argument("--user_agent", type=str, required=True)
    args = parser.parse_args()

    print("\n" + "-" * 50)
    print(f"Parsing player countries for {args.json_file}")
    print("-" * 50)
    parser = JSONParser(json_file=args.json_file, user_agent=args.user_agent)
    parser.write_df()
