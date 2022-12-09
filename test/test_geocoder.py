from unittest import IsolatedAsyncioTestCase
from src import geocoder

class TestGeocoder(IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.gc = geocoder.Geocoder()
        self.correct_address_dict = [{'long_name': '5412', 'short_name': '5412', 'types': ['street_number']},
                        {'long_name': 'Youngstown Warren Road', 'short_name': 'Youngstown Warren Rd', 'types': ['route']},
                        {'long_name': 'Niles', 'short_name': 'Niles', 'types': ['locality', 'political']},
                        {'long_name': 'Howland Township', 'short_name': 'Howland Township', 'types': ['administrative_area_level_3', 'political']},
                        {'long_name': 'Trumbull County', 'short_name': 'Trumbull County', 'types': ['administrative_area_level_2', 'political']},
                        {'long_name': 'Ohio', 'short_name': 'OH', 'types': ['administrative_area_level_1', 'political']},
                        {'long_name': 'United States', 'short_name': 'US', 'types': ['country', 'political']},
                        {'long_name': '44446', 'short_name': '44446', 'types': ['postal_code']}]  # From Google
        self.correct_street = '5412 YOUNGSTOWN WARREN RD , NILES , OH , 44446'  # Census returns something like this

    async def test_from_addr_to_lat_lng_correct_addr(self):
        response = await self.gc._call_api_from_addr_to_lng_lat('70 W. MADISON , CHICAGO , IL , 60601')
        result = self.gc._parse_google_response(response)
        self.assertTrue(len(result) == 4)
        self.assertTrue(type(result[0]) == float)
        self.assertTrue(type(result[1]) == float)

    async def test_from_addr_to_lat_lng_wrong_addr(self):
        response = await self.gc._call_api_from_addr_to_lng_lat('70')
        result = self.gc._parse_google_response(response)
        self.assertTrue(len(result) == 4)
        self.assertTrue(result[0] is None)
        self.assertTrue(result[1] is None)

    async def test_from_lat_lng_to_block_wrong_lat_lng(self):
        response = await self.gc._call_api_from_lat_lng_to_block(41.88345, -87.628888)
        result = self.gc._parse_census_response(response)
        self.assertTrue(len(result) == 3)
        self.assertTrue(result[0] is None)
        self.assertTrue(result[1] is None)
        self.assertTrue(result[2] is None)

    async def test_from_lat_lng_to_block_correct_lat_lng(self):
        response = await self.gc._call_api_from_lat_lng_to_block(-87.628888, 41.88345)
        result = self.gc._parse_census_response(response)
        self.assertTrue(len(result) == 3)
        self.assertTrue(type(result[0]) == int)
        self.assertTrue(type(result[1]) == int)
        self.assertTrue(type(result[2]) == int)

    def test__compare_address_all_correct(self):
        self.assertTrue(self.gc._compare_address(self.correct_street, self.correct_address_dict))

    def test__compare_address_wrong_number(self):
        state_name_dict = next(x for x in self.correct_address_dict if 'street_number' in x['types'])
        state_name_dict['long_name'] = state_name_dict['short_name'] = '113'
        self.assertFalse(self.gc._compare_address(self.correct_street, self.correct_address_dict))

    def test__compare_address_wrong_street(self):
        street = '5412 YOUNGSTOWN WARREN ST, NOT NILES ANYMORE, OH, 44446'
        self.assertFalse(self.gc._compare_address(street, self.correct_address_dict))

    def test__compare_address_wrong_city(self):
        street = '5412 YOUNGSTOWN WARREN ST, NILES, OH, 44446'
        self.assertFalse(self.gc._compare_address(street, self.correct_address_dict))

    def test__compare_address_wrong_state(self):
        state_name_dict = next(x for x in self.correct_address_dict if 'administrative_area_level_1' in x['types'])
        state_name_dict['short_name'] = 'WA'
        self.assertFalse(self.gc._compare_address(self.correct_street, self.correct_address_dict))

    def test__compare_address_wrong_zipcode(self):
        zip_dict = next(x for x in self.correct_address_dict if 'postal_code' in x['types'])
        zip_dict['short_name'] = zip_dict['long_name'] = '4'
        self.assertFalse(self.gc._compare_address(self.correct_street, self.correct_address_dict))
