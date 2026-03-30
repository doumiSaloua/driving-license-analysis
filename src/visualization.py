import plotly.express as px
import polars as pl

def add_time_features(df):
    """Add time-based columns to DataFrame."""
    return df.with_columns([
        (pl.col("first_timestamp_dt").dt.weekday().is_in([6, 7])).alias("is_weekend"),
        pl.when(pl.col("first_timestamp_dt").dt.hour().is_between(6, 11)).then(pl.lit("Morning"))
        .when(pl.col("first_timestamp_dt").dt.hour().is_between(12, 17)).then(pl.lit("Afternoon"))
        .when(pl.col("first_timestamp_dt").dt.hour().is_between(18, 22)).then(pl.lit("Evening"))
        .otherwise(pl.lit("Night")).alias("time_of_day"),
        pl.when(pl.col("first_timestamp_dt").dt.month().is_in([12, 1, 2])).then(pl.lit("Winter"))
        .when(pl.col("first_timestamp_dt").dt.month().is_in([3, 4, 5])).then(pl.lit("Spring"))
        .when(pl.col("first_timestamp_dt").dt.month().is_in([6, 7, 8])).then(pl.lit("Summer"))
        .otherwise(pl.lit("Autumn")).alias("season")
    ])