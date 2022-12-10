import argparse
import asyncio

import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from pathlib import Path
from src.data_handler import DataHandler
from src.geocoder import Geocoder

import logging


async def process_csv(df):
    gs = Geocoder()
    if "Street address" not in df.columns:
        raise "Column address not found in the CSV file"
    tuples = await tqdm_asyncio.gather(*(gs.process(v) for v in df['Street address'].values))
    return tuples


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Address to Tracts, Block Groups and Blocks')
    parser.add_argument('-f', type=str, default='part_unmatch_and_tie.csv',
                        help='The path of the file containing the addresses')

    args = parser.parse_args()
    df = pd.read_csv(args.f)
    tracts = asyncio.run(process_csv(df))
    dh = DataHandler()
    df_with_tracts = dh.append_to_table(df, tracts)

    # create path for finished file
    finished_file_name = str(Path(args.f).stem) + "_finished.csv"
    path_to_file = Path(args.f).parent
    df_with_tracts.to_csv(path_to_file.joinpath(finished_file_name), index=False)
