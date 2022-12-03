import unittest

import pandas as pd

from src.data_handler import DataHandler


class TestDataHandler(unittest.TestCase):

    def setUp(self) -> None:
        self.data_handler = DataHandler()

    def test_append_to_table(self):
        # set up
        address_df = pd.DataFrame({'Street address': ['5412 YOUNGSTOWN WARREN RD , NILES , OH , 44446',
                                                      '7701 MENTOR AVE , MENTOR , OH , 44060',
                                                      '1901 NW EXPRESSWAY , OKLAHOMA CITY , OK , 73118',
                                                      '71 ST. NICHOLAS DRIVE , NORTH POLE , AK , 99705']})
        tract_blkgrp_block = [(932701, 1, 1003), (206500, 2, 2055), (106503, 2, 2000), (1600, 3, 3028)]
        expected_df = pd.DataFrame({'Street address': ['5412 YOUNGSTOWN WARREN RD , NILES , OH , 44446',
                                                       '7701 MENTOR AVE , MENTOR , OH , 44060',
                                                       '1901 NW EXPRESSWAY , OKLAHOMA CITY , OK , 73118',
                                                       '71 ST. NICHOLAS DRIVE , NORTH POLE , AK , 99705'],
                                    'tract': [932701, 206500, 106503, 1600],
                                    'block_group': [1, 2, 2, 3],
                                    'block': [1003, 2055, 2000, 3028]
                                    })

        # act
        received_df = self.data_handler.append_to_table(address_df, tract_blkgrp_block)
        pd.testing.assert_frame_equal(expected_df, received_df)
