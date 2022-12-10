import asyncio
import io
import logging
import tempfile
from typing import List

import aiohttp
import numpy as np
import pandas as pd
import yaml

from src import constants


class DataHandler:
    """
    This class handle the first stage of the workflow:
    1. Splitting a big files to chunks
    2. Upload those chunks to Census batch
    3. Join them together at the end
    """

    def __init__(self):
        with open("secret.yaml", "r") as stream:
            try:
                secret = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        self.census_key = secret['key']['census_api']
        default_logging = logging.INFO
        logging.basicConfig(level=default_logging)
        self.logger = logging.getLogger()
        self.logger.setLevel(default_logging)

    def append_to_table(self, original_address_table: pd.DataFrame, tract_blkgrp_blk: list) -> pd.DataFrame:
        """
        Append tract, block groups and group to original_address_table
        Args:
            original_address_table:
                Contains addresses
            tract_blkgrp_blk:
                A list of tuples that contains tract, block group and block of an address, in that exact order
        Returns:
            Another DF that has three extra columns for tract, block group and block
        """
        tract_blkgrp_blk = pd.DataFrame(tract_blkgrp_blk, columns=['tract', 'block_group', 'block'])
        original_column_names = np.append(original_address_table.columns, tract_blkgrp_blk.columns)
        concated_df = pd.concat([original_address_table, tract_blkgrp_blk], axis=1, ignore_index=True)
        return concated_df.rename(dict(zip(concated_df.columns, original_column_names)), axis=1)
