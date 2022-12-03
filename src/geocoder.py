import logging

import googlemaps
import requests
import yaml


class Geocoder:

    def __init__(self):
        with open("secret.yaml", "r") as stream:
            try:
                secret = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        default_logging = logging.INFO
        self.gmaps = googlemaps.Client(key=secret['key']['google_api'])
        logging.basicConfig(level=default_logging)
        self.logger = logging.getLogger()
        self.logger.setLevel(default_logging)

    async def _call_api_from_addr_to_lng_lat(self, addr: str):
        """
        Google will try to salvage everything from an address, rather than outright saying no match. For example,
        passing in "9999999 MSON , CHICAGO , NY , 60601" will give two result, one for Chicago, one for NY.
        If input is gibberish, it outputs `[]`

        This code, therefore, would take the first result or outputs None if Google or Census cannot find anything

        Args:
            addr: An address to get lat_long from
        Returns:
            A tuple that contains the (lat, long) value of given address
        """
        self.logger.info("Begin to call Google API")
        geocode_result = self.gmaps.geocode(addr)
        # await asyncio.sleep(3)
        self.logger.info("Finish calling Google API")
        return geocode_result

    async def _call_api_from_lat_lng_to_block(self, lng: float, lat: float):
        """
        Use the lng lat to tract, block data using Census API
        Args:
            lng: Longitude
            lat: Latitude
        Returns:
            A tuple of (tract, block group, block)
        """
        self.logger.info("Begin to call Census API")
        params = {
            'x': lng,
            'y': lat,
            'benchmark': 4,
            'vintage': 4,
            'format': "json"
        }
        response = requests.get('https://geocoding.geo.census.gov/geocoder/geographies/coordinates', params=params)
        # await asyncio.sleep(2)
        self.logger.info("Finish calling Census API.")
        return response

    def _parse_census_response(self, response):
        """

        Args:
            response:
                Expecting the response to have the following structure. todo what structure

        Returns:

        """
        self.logger.info("Begin parsing Census API")
        tract, block_group, block = None, None, None
        if response.status_code == 200:
            response_result = response.json()['result']
            geographies = response_result.get('geographies')
            if geographies:  # there is a match
                block_info, = geographies.get('2020 Census Blocks')
                block_group, block = int(block_info.get('BLKGRP')), int(block_info.get('BLOCK'))
                tract_info, = geographies.get('Census Tracts')
                tract = int(tract_info.get('TRACT'))
        else:
            logging.error(response['error'])
        self.logger.info("Finish parsing Census API")
        return tract, block_group, block

    def _parse_google_response(self, geocode_result) -> tuple:
        """
        Parse the result from Google API.
        Args:
            response:
                Expecting the response to have the following structure.
                {...
                    'geometry': {'location': {'lat': ..., 'lng': ...}
                ...}
        Returns:
            Tuple containing lng and lat of the response, (None, None) if
            the input cannot be parsed
        """
        self.logger.info("Begin parsing Google response")
        if len(geocode_result) == 1:  # exact match
            geocode_result, = geocode_result
        elif len(geocode_result) > 1:  # multiple matches
            geocode_result = geocode_result[0]
        else:
            self.logger.warning("Unable to get a coordinate for address")
            return None, None
        geometry = geocode_result.get('geometry')
        if geometry:
            location = geometry.get('location')
            if location:
                lng, lat = location.get('lng'), location.get('lat')
                self.logger.info("Finish parsing Google response")
                return lng, lat
            else:
                self.logger.warning("Unable to get a correct dict structure")
                return None, None
        else:
            self.logger.warning("Unable to get a correct dict structure")
            return None, None

    async def process(self, addr: str):
        """
        Convert an address to lat long
        Args:
            addr: An address

        Returns:
            A tuple of (tract, block group, block)
        """
        lng_lat_response = await self._call_api_from_addr_to_lng_lat(addr)
        lng, lat = self._parse_google_response(lng_lat_response)
        if lng and lat:
            census_response = await self._call_api_from_lat_lng_to_block(lng, lat)
            tract, block_group, block = self._parse_census_response(census_response)
            return tract, block_group, block
        else:
            return None, None, None
