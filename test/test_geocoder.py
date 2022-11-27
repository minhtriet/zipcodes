from unittest import IsolatedAsyncioTestCase
from src import geocoder


class TestGeocoder(IsolatedAsyncioTestCase):

    async def test_from_addr_to_lat_lng_correct_addr(self):
        gc = geocoder.Geocoder()
        result = await gc.from_addr_to_lat_lng('70 W. MADISON , CHICAGO , IL , 60601')
        self.assertTrue(len(result) == 2)
        self.assertTrue(type(result[0]) == float)
        self.assertTrue(type(result[1]) == float)

    async def test_from_addr_to_lat_lng_wrong_addr(self):
        gc = geocoder.Geocoder()
        result = await gc.from_addr_to_lat_lng('70')
        self.assertTrue(len(result) == 2)
        self.assertTrue(result[0] is None)
        self.assertTrue(result[1] is None)

    async def test_from_lat_lng_to_block_wrong_lat_lng(self):
        gc = geocoder.Geocoder()
        result = await gc.from_lat_lng_to_block(41.88345, -87.628888)
        self.assertTrue(len(result) == 3)
        self.assertTrue(result[0] is None)
        self.assertTrue(result[1] is None)
        self.assertTrue(result[2] is None)

    async def test_from_lat_lng_to_block_correct_lat_lng(self):
        gc = geocoder.Geocoder()
        result = await gc.from_lat_lng_to_block(-87.628888, 41.88345)
        self.assertTrue(len(result) == 3)
        self.assertTrue(type(result[0]) == int)
        self.assertTrue(type(result[1]) == int)
        self.assertTrue(type(result[2]) == int)
