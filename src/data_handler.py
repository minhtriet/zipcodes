import numpy as np
import pandas as pd


class DataHandler:

    def __init__(self):
        self.max_rows_per_file = 10000

    def _split_file_to_files_with_max_rows(self, filename):
        pass

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
