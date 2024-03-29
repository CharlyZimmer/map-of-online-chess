parse_countries:
	python3 -m src.geocoding.country --parquet_file ${COUNTRY_PARQUET} --user_agent ${USER_AGENT}
enrich_geojson:
	python3 -m src.geocoding.enrich --parquet_file ${COUNTRY_PARQUET}
prepare_data:
	python3 -m src.geocoding.country --file_name ${COUNTRY_PARQUET} --user_agent ${USER_AGENT}
	python3 -m src.geocoding.enrich --parquet_file ${COUNTRY_PARQUET}
