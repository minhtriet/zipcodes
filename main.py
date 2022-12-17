import argparse
import asyncio
import logging
import os.path
from pathlib import Path

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

import src.constants
from src.data_handler import DataHandler
from src.geocoder import Geocoder


async def process_unmatched_csv(df):
    gs = Geocoder()
    if "Street address" not in df.columns:
        raise "Column address not found in the CSV file"
    async with aiohttp.ClientSession() as session:
        tuples = await tqdm_asyncio.gather(*(gs.process(v, session) for v in df['Street address'].values))
    return tuples


def validate_filename(filename):
    if not os.path.exists(filename):
        raise argparse.ArgumentTypeError(f"Cannot find file {filename} in path {os.getcwd()}")
    if (os.path.exists(src.constants.TEMP_UNMATCH_CSV)) and (os.path.exists(src.constants.TEMP_MATCH_CSV)):
        if (filename != src.constants.TEMP_UNMATCH_CSV):
            raise argparse.ArgumentTypeError(f'''Found a file named {src.constants.TEMP_UNMATCH_CSV}, which is a result
                of the previous run. You may want to pass it in instead. To overcome this error, please delete both
                {src.constants.TEMP_MATCH_CSV} and {src.constants.TEMP_UNMATCH_CSV} first and run the code again.''')
    return filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Address to Tracts, Block Groups and Blocks')
    parser.add_argument('-f', default='processed_address.csv', type=validate_filename,
                        help='''
                        The path of the addresses file, which should not have header columns. However, according to
                        https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html#_Toc7768597,
                        the columns are `Unique ID` (Just as a reference), `Street address`, `City`, `State`, `ZIP`
                        ''')
    args = parser.parse_args()

    logging.getLogger(__name__)
    logging.root.setLevel(src.constants.LOGGING_LEVEL)

    dh = DataHandler()
    if args.f != src.constants.TEMP_UNMATCH_CSV:
        # split the CSV and pushed the chunks to Census
        result_with_columns = asyncio.run(dh.batch_process_csv(args.f))

        matched = result_with_columns[result_with_columns['is_matched'] == 'Match'].drop('is_matched', axis=1)
        # Construct column "blockgroup" from 1st char of column "block"
        matched['blockgroup'] = matched['block'].astype(str).str[0]
        matched.to_csv(src.constants.TEMP_MATCH_CSV, index=False)

        unmatched_and_tie = result_with_columns[result_with_columns['is_matched'] != 'Match'].drop('is_matched', axis=1)
        unmatched_and_tie.to_csv(src.constants.TEMP_UNMATCH_CSV, index=False)
    else:
        matched = pd.read_csv(src.constants.TEMP_MATCH_CSV)
        unmatched_and_tie = pd.read_csv(src.constants.TEMP_UNMATCH_CSV)

    logging.info(f'''There are still {len(unmatched_and_tie)} addresses that Census could not find a match.\n
    They need to be posted to Google service.''')
    tracts = asyncio.run(process_unmatched_csv(unmatched_and_tie))
    unmatched_and_tie[['tract', 'blockgroup', 'block', 'autocorrected_addr', 'same_addr']] = tracts

    final_result = pd.concat([matched, unmatched_and_tie])

    # create path for finished file
    finished_file_name = str(Path(args.f).stem) + "_finished.csv"
    path_to_file = Path(args.f).parent
    final_result.to_csv(path_to_file.joinpath(finished_file_name), index=False)
    logging.info(f"DONE! Results are saved to {finished_file_name}")
