import logging
from typing import OrderedDict

import googlemaps
import requests
import usaddress
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
        Parse the result from Google API. Since there is an autocorrection in place for the Google Geocoder API,
        its autocorrected address with also be returned to compare with the original input address
        Args:
            geocode_result:
                Expecting the response to have the following structure.
                {...
                    'address_components':
                    'formatted_address': str,
                    'geometry': {'location': {'lat': ..., 'lng': ...}
                ...}
        Returns:
            Tuple containing lng, lat, corrected address and addresses component of the response
            or a tuple with `None`s if the input cannot be parsed
        """
        self.logger.info("Begin parsing Google response")
        if len(geocode_result) == 1:  # exact match
            geocode_result, = geocode_result
        elif len(geocode_result) > 1:  # multiple matches
            geocode_result = geocode_result[0]
        else:
            self.logger.warning("Google API could not find any match for an address")
        try:
            lng = geocode_result['geometry']['location']['lng']
            lat = geocode_result['geometry']['location']['lat']
            formatted_address = geocode_result['formatted_address']
            address_component = geocode_result['address_components']
            return lng, lat, formatted_address, address_component
        except (KeyError, TypeError):
            self.logger.warning("Unable to get a correct dict structure")
        return None, None, None, None

    def _compare_address(self, address_1: str, parsed_adress: OrderedDict) -> bool:
        """
        Compare and answers if the two addresses is the same or not
        Args:
            address_1:
                A string address
            parsed_adress:
                A dictionary with the components of the address, this is the result from Google

        Returns:
            If the two addresses is the same or not
        """
        tagged, _ = usaddress.tag(address_1)

        # address number
        address_number_dict = next(x for x in parsed_adress if x['types'] == ['street_number'])
        if address_number_dict.get('long_name') != tagged.get('AddressNumber'):
            return False
        # street name
        street_name_dict = next(x for x in parsed_adress if x['types'] == ['route'])
        if street_name_dict.get('short_name').lower() != ' '.join(tagged.get('StreetName'),
                                                                  tagged.get('StreetNamePostType')).lower():
            return False
        # city name
        city_name_dict = next(x for x in parsed_adress if 'locality' in x['types'])
        if city_name_dict.get('short_name').lower() != ' '.join(tagged.get('StreetName'),
                                                              tagged.get('StreetNamePostType')).lower():
            return False
        # state name
        state_name_dict = next(x for x in parsed_adress if 'administrative_area_level_1' in x['types'])
        if tagged.get('StateName').lower() != state_name_dict.get('short_name').lower():
            return False
        # postal code
        postal_code_dict = next(x for x in parsed_adress['address_components'] if x['types'] == ['postal_code'])
        if postal_code_dict['long_name'] != tagged.get('ZipCode'):
            return False

        return True


    async def process(self, addr: str):
        """
        Convert an address to lat long. Also check if returned address from Google is the same as the address
        being passed to
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
