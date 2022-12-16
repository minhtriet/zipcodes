import asyncio
import io
import logging
import tempfile

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
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s', level=constants.LOGGING_LEVEL)

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

    async def _post_batch_to_census(self, chunk, session: aiohttp.ClientSession):
        """
        Pass a chunk of the dataframe to Census. How to include the API key into a request is at
        https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf, page 21
        Args:
            chunk:
                A chunk of frame taken from the original big input file
            session:
                An async http session to make a request
        Returns:

        """
        with tempfile.NamedTemporaryFile(suffix='.csv') as tmp:
            chunk.to_csv(tmp.name, index=False, header=False)
            files = {
                'addressFile': open(tmp.name, 'rb'),
                # For benchmark, we use ACS layers numbering, documented at Page C-1 in appendix in
                # https://www2.census.gov/geo/pdfs/maps-data/data/Census_Geocoder_User_Guide.pdf.
                'benchmark': 'Public_AR_Current',
                'vintage': '4',
                # For layers, 8 is for tracts, 10 is for block groups, and 12 is for blocks.
                'layers': '10,12',
                'key': self.census_key
            }
            response = await session.post(constants.CENSUS_BATCH_URL, data=files)
            response_text = await response.text()
            return pd.read_csv(io.StringIO(response_text), header=None,
                               names=['ID', 'Street address', 'is_matched', 'match_type', 'cleaned_address', 'lat_lon',
                                      'tigerLine_id', 'side', 'state', 'county', 'tract', 'block'])

    async def batch_process_csv(self, filename: str) -> pd.DataFrame:
        """
        Split filename into batches and push them to Census
        Args:
            filename:
                The big CSV file name to parse, required to have the columns addressed in
                https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html#_Toc7768597.
                Namely, the columns are `Unique ID` (Just as a reference), `Street address`, `City`, `State`, `ZIP`
        Returns:
            A concatenated df with column names from list of processed dataframes
        """
        async with aiohttp.ClientSession() as session:
            self.logger.info("Starting to submit batches to Census API")
            processed_dfs = await asyncio.gather(*(self._post_batch_to_census(chunk, session)
                                                   for chunk in pd.read_csv(filename,
                                                                            chunksize=constants.MAX_LINES_ALLOWED_CENSUS,
                                                                            header=None)))
            self.logger.info("Finished submitting batches to Census API\n---------------")
            return processed_dfs
