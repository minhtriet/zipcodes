import argparse
import asyncio
import logging
from pathlib import Path

import pandas as pd
from tqdm.asyncio import tqdm_asyncio

import src.constants
from src.data_handler import DataHandler
from src.geocoder import Geocoder

async def process_umatched_csv(df):
    gs = Geocoder()
    if "Street address" not in df.columns:
        raise "Column address not found in the CSV file"
    tuples = await tqdm_asyncio.gather(*(gs.process(v) for v in df['Street address'].values))
    return tuples


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Address to Tracts, Block Groups and Blocks')
    parser.add_argument('-f', type=str, default='portion_addr.csv',
                        help='''
                        The path of the addresses file, which should not have header columns. However, according to
                        https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html#_Toc7768597,
                        the columns are `Unique ID` (Just as a reference), `Street address`, `City`, `State`, `ZIP`
                        ''')

    args = parser.parse_args()

    logging.getLogger(__name__)
    logging.root.setLevel(src.constants.LOGGING_LEVEL)

    dh = DataHandler()
    # split the CSV and pushed the chunks to Census
    result_with_columns = asyncio.run(dh.batch_process_csv(args.f))

    matched = result_with_columns[result_with_columns['is_matched'] == 'Match'].drop('is_matched', axis=1)
    matched['blockgroup'] = matched['block'].astype(str).str[0]

    unmatched_and_tie = result_with_columns[result_with_columns['is_matched'] != 'Match'].drop('is_matched', axis=1)
    tracts = asyncio.run(process_umatched_csv(unmatched_and_tie))
    unmatched_and_tie[['tract', 'blockgroup', 'block', 'autocorrected_addr', 'same_addr']] = tracts

    final_result = pd.concat([matched, unmatched_and_tie])

    # create path for finished file
    finished_file_name = str(Path(args.f).stem) + "_finished.csv"
    path_to_file = Path(args.f).parent
    final_result.to_csv(path_to_file.joinpath(finished_file_name), index=False)
    logging.info(f"DONE! Results are saved to {finished_file_name}")
