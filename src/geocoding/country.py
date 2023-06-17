from pandas import read_parquet, Series
from geopy.geocoders import Nominatim
import os
from pandas import DataFrame
from pycountry import countries
from ratelimiter import RateLimiter
from tqdm import tqdm
from typing import Dict

from src import DATA_DIRECTORY


class ParquetParser:
    def __init__(self, user_agent: str = 'abc@test.de',
                 known_countries_parquet: str = 'known_countries.parquet.gzip'):
        """
        Initialize the parser with the path to a parquet file with country information
        and a user_agent for geocoding with Nominatim
        :param user_agent:                  String to use for identification with Nominatim (An email-address suffices)
        :param known_countries_parquet:     (Optional) Name to the file containing country locations
                                            (In the data/output/countries directory)
                                            Default: 'known_countries.parquet.gzip'
                                            Will be used to read known locations and write results after completion
        """
        self.user_agent = user_agent

        # Prepare folder
        self.countries_dir = DATA_DIRECTORY / 'output/countries'
        os.makedirs(self.countries_dir, exist_ok=True)

        # Try to load a dataframe of known countries
        self.known_parquet_path = self.countries_dir / f'{known_countries_parquet}'
        if os.path.isfile(self.known_parquet_path):
            known_df = read_parquet(self.known_parquet_path)
            self.known_countries = known_df.set_index('ISO_A2').to_dict(orient='index')
        else:
            self.known_countries = {}

    def update_known_countries(
            self, new_countries_parquet="test_openings.parquet.gzip"
    ):
        """
        Add ISO_A2, ISO_A3, and country name to a parquet file of openings per counts.
        Also add the Nominatim place_id of new countries to the DataFrame of known countries.

        :param new_countries_parquet:   Filename of the parquet file (in data/parse/output/countries) to identify
                                        new countries from
        """
        # Load and prepare the dataframe (Keep only unique countries)
        parquet_path = self.countries_dir / new_countries_parquet
        df = read_parquet(parquet_path)

        country_df = df.groupby('country').count().reset_index()
        country_df.rename(columns={"country": "ISO_A2"}, inplace=True)

        # Expand the df with additional columns (ISO_A3 and country); Drop unidentified countries
        tqdm.pandas(desc='Expanding country information per country')
        country_df = country_df.progress_apply(self._parse_country_attr, axis=1)
        country_df = country_df.dropna(subset='country')

        # Set geocoder and rate limiter
        geolocator = Nominatim(user_agent=self.user_agent)
        nominatim_rate_limiter = RateLimiter(max_calls=1, period=1)

        # Get new country locations with progress bar
        tqdm.pandas(desc="Identifying country locations")
        country_df.progress_apply(
            lambda row: self._get_new_country(
                row=row,
                geolocator=geolocator,
                known_locations=self.known_countries,
                rate_limiter=nominatim_rate_limiter,
            ),
            axis=1
        )
        # Save the known countries as a parquet file
        known_df = DataFrame(
            [{"ISO_A2": country, **loc} for country, loc in self.known_countries.items()]
        )
        known_df.to_parquet(str(self.known_parquet_path), compression="gzip")
        print(f"Got Nominatim country IDs for new countries and saved updated file to {self.known_parquet_path}.")

    @staticmethod
    def _parse_country_attr(row: Series) -> Series:
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

    @staticmethod
    def _get_new_country(
            row: Series,
            geolocator: Nominatim,
            known_locations: Dict[str, Dict],
            rate_limiter: RateLimiter):
        """
        Add Nominatim information about a new country from Nominatim.
        Requests to Nominatim are subject to the limits set in rate_limiter
        :param row:                 Row of a pandas DataFrame
        :param geolocator:          Instance of geopy.geocoders.Nominatim
        :param known_locations:     Dictionary of LOCATION_NAME: geopy.Location
        :param rate_limiter:        Instance of ratelimiter.RateLimiter for Nominatim requests
        :return:                    Same row
        """
        iso_a2 = row['ISO_A2']
        if iso_a2 not in known_locations:
            iso_a3 = row['ISO_A3']
            location_name = row['country']
            with rate_limiter:
                try:
                    location = geolocator.geocode(location_name).raw
                except:
                    location = {}
                known_locations[iso_a2] = {'country': location_name,
                                           'ISO_A3': iso_a3,
                                           **location}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--file_name", type=str, required=True)
    parser.add_argument("--user_agent", type=str, required=True)
    args = parser.parse_args()

    print("\n" + "-" * 50)
    print(f"Updating known countries based on {args.file_name}")
    print("-" * 50)
    parser = ParquetParser(user_agent=args.user_agent)
    parser.update_known_countries(new_countries_parquet=args.file_name)
