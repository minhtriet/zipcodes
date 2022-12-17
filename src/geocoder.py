import logging
from typing import Dict, List, Tuple

import aiohttp
import googlemaps
import usaddress
import yaml

from src import constants


class Geocoder:

    def __init__(self):
        with open("secret.yaml", "r") as stream:
            try:
                secret = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        self.gmaps = googlemaps.Client(key=secret['key']['google_api'])
        self.census_key = secret['key']['census_api']
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s',
                            level=constants.LOGGING_LEVEL)

    def _call_api_from_addr_to_lng_lat(self, addr: str):
        """
        Google will try to salvage everything from an address, rather than outright saying no match. For example,
        passing in "9999999 MSON , CHICAGO , NY , 60601" will give two result, one for Chicago, one for NY.
        If input is gibberish, it outputs `[]`

        This code, therefore, would take the first result or outputs None if Google or Census cannot find anything

        This code is not async because
        1/ Google uses `requests` and hence does not support it atm of writing
        2/ the Census request later down the flow should be awaited instead.

        Args:
            addr: An address to get lat_long from
        Returns:
            A tuple that contains the (lat, long) value of given address
        """
        self.logger.debug("Begin to call Google API")
        geocode_result = self.gmaps.geocode(addr)
        # await asyncio.sleep(3)
        self.logger.debug("Finish calling Google API")
        return geocode_result

    async def _call_api_from_lat_lng_to_block(self, lng: float, lat: float, session: aiohttp.ClientSession):
        """
        Use the lng lat to tract, block data using Census API
        Args:
            lng: Longitude
            lat: Latitude
            session: An async http session to make request
        Returns:
            A tuple of (tract, block group, block)
        """
        self.logger.debug("Begin to call Census API")
        params = {
            'x': lng,
            'y': lat,
            'benchmark': 4,
            'vintage': 4,
            'format': "json",
            'key': self.census_key
        }
        response = await session.get(constants.CENSUS_GEOCODER_FROM_COORD, params=params)
        # await asyncio.sleep(2)
        self.logger.debug("Finish calling Census API.")
        return response

    async def _parse_census_lng_lat_response(self, response) -> Tuple:
        """
        After submitting the lat and lng to Census Geocoder, they will return with a response with the tract
        data of the coordinate
        Args:
            response:
                A response from Census
        Returns:
            A tuple of (tract, block group, block)
        """
        self.logger.debug("Begin parsing Census API")
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
            self.logger.error(f'''Census server responded with failure code (Code: {response.status_code}).\n
            The error is {response.content}\n Please rerun the program. 
            If the problem persists please run at another time when the Census server is more stable.''')
        self.logger.debug("Finish parsing Census API")
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
        self.logger.debug("Begin parsing Google response")
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

    def _compare_address(self, address_1: str, parsed_adress: List[Dict]) -> bool:
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
        try:
            tagged, _ = usaddress.tag(address_1)
        except usaddress.RepeatedLabelError:
            # With a legit address like `368 N 750 W, American Fork, UT 84003, USA`, usaddress confused
            # 360 and 750 as address number. There has been multiple reports like this, not necessarily with
            # address number alone (https://github.com/datamade/usaddress/issues?page=2&q=is%3Aissue+is%3Aopen+RepeatedLabelError)
            return False

        try:
            # Get the dict values from Google response
            address_number_dict = next(x for x in parsed_adress if x['types'] == ['street_number'])
            street_name_dict = next(x for x in parsed_adress if x['types'] == ['route'])
            city_name_dict = next(x for x in parsed_adress if 'locality' in x['types'])
            state_name_dict = next(x for x in parsed_adress if 'administrative_area_level_1' in x['types'])
            postal_code_dict = next(x for x in parsed_adress if x['types'] == ['postal_code'])
        except StopIteration:
            return False

        # address number
        if address_number_dict.get('long_name') != tagged.get('AddressNumber'):
            return False
        # street name
        if street_name_dict.get('short_name').lower() != ' '.join(filter(None, [tagged.get('StreetName'),
                                                                                tagged.get(
                                                                                    'StreetNamePostType')])).lower():
            return False
        # city name
        if city_name_dict.get('short_name').lower() != tagged.get('PlaceName').lower():
            return False
        # state name
        if tagged.get('StateName').lower() != state_name_dict.get('short_name').lower():
            return False
        # postal code
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
            A tuple of (tract, block group, block, autocorrected_addr, if autocorrected_addr == addr)
        """
        lng_lat_response = await self._call_api_from_addr_to_lng_lat(addr)
        lng, lat, autocorrected_addr, addr_components = self._parse_google_response(lng_lat_response)
        if lng and lat:  # a result was found through Google
            census_response = await self._call_api_from_lat_lng_to_block(lng, lat)
            tract, block_group, block = self._parse_census_lng_lat_response(census_response)
            # check if the address returned is the same as the original address
            return tract, block_group, block, autocorrected_addr, self._compare_address(addr, addr_components)
        else:  #
            return None, None, None, None, None
