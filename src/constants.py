import logging

MAX_LINES_ALLOWED_CENSUS = 10000
LOGGING_LEVEL = logging.DEBUG  # DEBUG/INFO for more/less detailed log
CENSUS_BATCH_URL = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
CENSUS_GEOCODER_FROM_COORD = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

TEMP_UNMATCH_CSV = "unmatched_census.csv"
TEMP_MATCH_CSV = "matched_census.csv"
