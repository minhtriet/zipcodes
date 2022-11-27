import asyncio
import googlemaps
import yaml
import requests
import logging

class Geocoder:

    def __init__(self):
        with open("secret.yaml", "r") as stream:
            try:
                secret = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        self.gmaps = googlemaps.Client(key=secret['key']['google_api'])

    async def from_addr_to_lat_lng(self, addr: str):
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
        geocode_result = self.gmaps.geocode(addr)
        if len(geocode_result) == 1:  # exact match
            geocode_result, = geocode_result
        elif len(geocode_result) > 1:  # multiple matches
            geocode_result = geocode_result[0]
        else:
            logging.warning(f"Unable to get a coordinate for address {addr}")
            return None, None
        geometry = geocode_result.get('geometry')
        if geometry:
            location = geometry.get('location')
            if location:
                lng = location.get('lng')
                lat = location.get('lat')
            else:
                logging.warning("Unable to get a correct dict structure")
        else:
            logging.warning("Unable to get a correct dict structure")
        return lng, lat

    async def from_lat_lng_to_block(self, lng, lat):
        params = {
            'x': lng,
            'y': lat,
            'benchmark': 4,
            'vintage': 4,
            'format': "json"
        }
        response = requests.get('https://geocoding.geo.census.gov/geocoder/geographies/coordinates', params=params)

        tract, block_group, block = None, None, None
        if response.status_code == 200:
            response_result = response.json()['result']
            geographies = response_result.get('geographies')
            if geographies:   # there is a match
                block_info, = geographies.get('2020 Census Blocks')
                block_group, block = int(block_info.get('BLKGRP')), int(block_info.get('BLOCK'))
                tract_info, = geographies.get('Census Tracts')
                tract = int(tract_info.get('TRACT'))
        else:
            logging.error(response['error'])
        return tract, block_group, block

