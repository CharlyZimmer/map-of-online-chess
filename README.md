# Map of Online Chess
## Preparation
### Packages
Use the requirements file to install necessary packages with your package manager of choice. E.g.
```
pip install -r requirements.txt
```

### Map data
Add the geoJSON from [datahub](https://datahub.io/core/geo-countries) to [`./map/static/json`](./map/static/json) using the name `countries.geojson`.
On Linux, simply run:
```
wget -O ./map/static/json/countries.geojson https://datahub.io/core/geo-countries/r/countries.geojson
```

## Get country information for players
Run the following make command with the required arguments:
- `JSON_PATH`: Path to the .json-file with player data
- `USER_AGENT`: User agent to use with Nominatim (e.g. your mail-address)

```
make JSON_PATH=player_data_lookup_only_positive.json USER_AGENT='[NAME]@uni-leipzig.de'
```