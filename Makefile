parse_countries:
	python ./geocoding/country.py --json_path ${JSON_PATH} --user_agent ${USER_AGENT}
expand_geojson:
	@if [ -z "${DF_PATH}" ]; then\
		python ./geocoding/expand_geojson.py;\
	else\
		python ./geocoding/expand_geojson.py --df_path ${DF_PATH};\
	fi
prepare_data:
	python ./geocoding/country.py --json_path ${JSON_PATH} --user_agent ${USER_AGENT}
	$(eval DF_PATH = ./geocoding/$(subst .json,.parquet.gzip,$(JSON_PATH)))
	python ./geocoding/expand_geojson.py --df_path ${DF_PATH}
