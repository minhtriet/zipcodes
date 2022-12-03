import asyncio

import pandas as pd
from tqdm.asyncio import tqdm_asyncio

from src.geocoder import Geocoder


async def process_csv(csv_filename="part_unmatch_and_tie.csv"):
    gs = Geocoder()
    df = pd.read_csv(csv_filename)
    if "Street address" not in df.columns:
        raise "Column address not found in the CSV file"
    tuples = await tqdm_asyncio.gather(*(gs.process(v) for v in df['Street address'].values))
    return tuples


if __name__ == "__main__":
    tracts = asyncio.run(process_csv())
    print(tracts)
