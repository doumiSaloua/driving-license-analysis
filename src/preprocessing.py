from __future__ import annotations

import polars as pl


HISTORY_DTYPE = pl.List(
    pl.Struct(
        [
            pl.Field("valid", pl.Boolean),
            pl.Field("timestamp", pl.String),
            pl.Field(
                "context",
                pl.Struct(
                    [
                        pl.Field("country", pl.String),
                        pl.Field("clientType", pl.String),
                        pl.Field("appVersion", pl.String),
                        pl.Field("demo", pl.Boolean),
                    ]
                ),
            ),
        ]
    )
)


def load_raw_parquet(path: str) -> pl.LazyFrame:
    """
    Load the raw parquet file as a Polars LazyFrame.
    """
    return pl.scan_parquet(path)


def parse_history_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Parse the history JSON string column into a structured Polars column.
    """
    return lf.with_columns(
        pl.when(
            pl.col("history").is_null() |
            (pl.col("history").str.strip_chars() == "")
        )
        .then(None)
        .otherwise(
            pl.col("history").str.json_decode(HISTORY_DTYPE)
        )
        .alias("history_parsed")
    )


def add_history_parsing_flags(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add simple quality flags related to history parsing.
    """
    return lf.with_columns(
        [
            pl.col("history_parsed").is_not_null().alias("history_parsed_ok"),
            pl.col("history_parsed").list.len().alias("history_length"),
        ]
    )
    
def reorder_history_chronologically(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Reverse the parsed history list so that attempts are ordered
    from the earliest to the latest.
    """
    return lf.with_columns(
        pl.col("history_parsed")
        .list.reverse()
        .alias("history_ordered")
    )