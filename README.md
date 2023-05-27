# Map of Online Chess
## Preparation
Use the requirements file to install necessary packages with your package manager of choice. E.g.
```
pip install -r requirements.txt
```

## Get country information for players
Run the following make command with the required arguments:
- `JSON_PATH`: Path to the .json-file with player data
- `USER_AGENT`: User agent to use with Nominatim (e.g. your mail-address)

```
make JSON_PATH=player_data_lookup_only_positive.json USER_AGENT='[NAME]@uni-leipzig.de'
```