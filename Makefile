parse_countries:
	python3 -m src.geocoding.country --json_file ${JSON_FILE} --user_agent ${USER_AGENT}
enrich_geojson:
	@if [ -z "${PLAYER_DF}" ]; then\
		python3 -m src.geocoding.enrich;\
	else\
		python3 -m src.geocoding.enrich --player_df ${PLAYER_DF};\
	fi
prepare_data:
	python3 -m src.geocoding.country --json_file ${JSON_FILE} --user_agent ${USER_AGENT}
	$(eval PLAYER_DF = $(subst .json,.parquet.gzip,$(JSON_FILE)))
	python3 -m src.geocoding.enrich --player_df ${PLAYER_DF}
