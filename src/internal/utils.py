import polars as pl
from polars.datatypes.group import FLOAT_DTYPES, INTEGER_DTYPES, NUMERIC_DTYPES


def is_float_dtype(dtype: pl.DataType) -> bool:
    return dtype in FLOAT_DTYPES


def is_int_dtype(dtype: pl.DataType) -> bool:
    return dtype in INTEGER_DTYPES


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    return dtype in NUMERIC_DTYPES
