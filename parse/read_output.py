from pandas import DataFrame, read_parquet

from parse import PARSING_DIRECTORY

def load_parquet(file_stem: str ='test', folder: str = 'openings') -> DataFrame:
    file_path = PARSING_DIRECTORY / 'data/output' / folder / f'{file_stem}.parquet.gzip'
    return read_parquet(file_path)
