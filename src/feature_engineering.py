from __future__ import annotations

import polars as pl


def add_basic_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Create basic features from the chronologically ordered history column.
    """

    first_attempt = pl.col("history_ordered").list.first()
    last_attempt = pl.col("history_ordered").list.last()

    return lf.with_columns(
        [
            pl.col("history_ordered").list.len().alias("num_attempts"),
            first_attempt.struct.field("valid").alias("first_valid"),
            last_attempt.struct.field("valid").alias("last_valid"),
            first_attempt.struct.field("timestamp").alias("first_timestamp"),
            last_attempt.struct.field("timestamp").alias("last_timestamp"),
        ]
    )


def add_behavior_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Create behavior-related features.
    """

    return lf.with_columns(
        [
            (pl.col("num_attempts") == 0).alias("is_empty_history"),
            (pl.col("num_attempts") > 1).alias("has_multiple_attempts"),
            (pl.col("first_valid") != pl.col("last_valid")).alias("changed_outcome"),
            (
                (pl.col("first_valid") == False) &
                (pl.col("last_valid") == True)
            ).alias("improved_between_first_and_last"),
            (
                (pl.col("first_valid") == True) &
                (pl.col("last_valid") == False)
            ).alias("declined_between_first_and_last"),
        ]
    )

def add_time_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Create time-based features from Unix timestamps stored as strings.
    Handles NULL values gracefully.
    """
    return lf.with_columns(
        [
            # Convert string timestamps to integers, keep NULL as NULL
            pl.col("first_timestamp")
            .cast(pl.Int64, strict=False)
            .alias("first_timestamp_int"),
            
            pl.col("last_timestamp")
            .cast(pl.Int64, strict=False)
            .alias("last_timestamp_int"),
        ]
    ).with_columns(
        [
            # Convert to datetime only if not NULL
            pl.when(pl.col("first_timestamp_int").is_not_null())
            .then(pl.from_epoch(pl.col("first_timestamp_int"), time_unit="s"))
            .otherwise(None)
            .alias("first_timestamp_dt"),
            
            pl.when(pl.col("last_timestamp_int").is_not_null())
            .then(pl.from_epoch(pl.col("last_timestamp_int"), time_unit="s"))
            .otherwise(None)
            .alias("last_timestamp_dt"),
        ]
    ).with_columns(
        [
            # Calculate time difference only if both timestamps exist
            pl.when(
                pl.col("first_timestamp_int").is_not_null() & 
                pl.col("last_timestamp_int").is_not_null()
            )
            .then(pl.col("last_timestamp_int") - pl.col("first_timestamp_int"))
            .otherwise(None)
            .alias("time_to_last_attempt")
        ]
    )
def add_performance_features(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Create performance-related features.
    """

    return lf.with_columns(
        [
            pl.col("first_valid").alias("answered_correctly_first_try"),
            pl.col("last_valid").alias("answered_correctly_last_try"),

            (
                (pl.col("num_attempts") > 1) &
                (pl.col("last_valid") == True)
            ).alias("needed_multiple_attempts_to_succeed"),

            pl.when(pl.col("num_attempts") > 0)
            .then(pl.col("last_valid") == False)
            .otherwise(None)
            .alias("never_correct"),

            (
                pl.col("num_attempts") - 1
            ).alias("attempts_before_last"),

            pl.when(pl.col("num_attempts") > 1)
            .then(pl.col("time_to_last_attempt") / (pl.col("num_attempts") - 1))
            .otherwise(None)
            .alias("avg_time_between_attempts"),
        ]
    )

def build_analytical_dataset(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Select the final columns used for analysis.
    """

    return lf.select(
        [
            "_id",
            "userId",
            "questionId",
            "source",
            "worldId",
            "lastAnswerAt",
            "lastContext_country",
            "lastContext_clientType",
            "lastContext_appVersion",
            "lastContext_demo",
            "num_attempts",
            "first_valid",
            "last_valid",
            "first_timestamp",
            "last_timestamp",
            "first_timestamp_dt",  
            "last_timestamp_dt", 
            "time_to_last_attempt",
            "is_empty_history",
            "has_multiple_attempts",
            "changed_outcome",
            "improved_between_first_and_last",
            "declined_between_first_and_last",
            "answered_correctly_first_try",
            "answered_correctly_last_try",
            "needed_multiple_attempts_to_succeed",
            "never_correct",
            "avg_time_between_attempts",
            "history_ordered",
        ]
    )