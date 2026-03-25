from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote_plus

import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
from pymongo import MongoClient
from pymongo.server_api import ServerApi


def build_mongo_uri(username: str, password: str, cluster_host: str) -> str:
    """
    Build a MongoDB Atlas connection URI.
    """
    encoded_password = quote_plus(password)
    return (
        f"mongodb+srv://{username}:{encoded_password}@{cluster_host}/"
        f"?retryWrites=true&w=majority"
    )


def create_mongo_client(uri: str) -> MongoClient:
    """
    Create a MongoDB client using Server API v1.
    """
    return MongoClient(uri, server_api=ServerApi("1"))


def check_mongo_connection(uri: str) -> bool:
    """
    Check whether the MongoDB connection is successful.
    """
    client = create_mongo_client(uri)

    try:
        client.admin.command("ping")
        return True
    finally:
        client.close()


def get_sample_data(
    uri: str,
    db_name: str,
    collection_name: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Fetch a small sample of documents from a MongoDB collection.
    """
    client = create_mongo_client(uri)

    try:
        collection = client[db_name][collection_name]
        return list(collection.find().limit(limit))
    finally:
        client.close()


def build_projection() -> Dict[str, int]:
    """
    Define the fields to keep from MongoDB.
    """
    return {
        "_id": 1,
        "userId": 1,
        "questionId": 1,
        "history": 1,
        "valid": 1,
        "lastAnswerAt": 1,
        "lastContext.country": 1,
        "lastContext.clientType": 1,
        "lastContext.appVersion": 1,
        "lastContext.demo": 1,
        "source": 1,
        "worldId" : 1
    }


def to_string(value: Any) -> Optional[str]:
    """
    Convert a value to a stable string representation.
    """
    if value is None:
        return None

    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)

    return str(value)


def to_bool(value: Any) -> Optional[bool]:
    """
    Convert a value to boolean when possible.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        value_lower = value.strip().lower()
        if value_lower == "true":
            return True
        if value_lower == "false":
            return False

    return bool(value)


def flatten_document(document: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten one MongoDB document into a stable tabular structure.
    """
    last_context = document.get("lastContext", {}) or {}

    return {
        "_id": to_string(document.get("_id")),
        "userId": to_string(document.get("userId")),
        "questionId": to_string(document.get("questionId")),
        "history": to_string(document.get("history")),
        "valid": to_bool(document.get("valid")),
        "lastAnswerAt": to_string(document.get("lastAnswerAt")),
        "lastContext_country": to_string(last_context.get("country")),
        "lastContext_clientType": to_string(last_context.get("clientType")),
        "lastContext_appVersion": to_string(last_context.get("appVersion")),
        "lastContext_demo": to_bool(last_context.get("demo")),
        "source": to_string(document.get("source")),
        "worldId": to_string(document.get("worldId"))
    }


def build_arrow_schema() -> pa.Schema:
    """
    Define a fixed Arrow schema for Parquet export.
    """
    return pa.schema([
        pa.field("_id", pa.string()),
        pa.field("userId", pa.string()),
        pa.field("questionId", pa.string()),
        pa.field("history", pa.string()),
        pa.field("valid", pa.bool_()),
        pa.field("lastAnswerAt", pa.string()),
        pa.field("lastContext_country", pa.string()),
        pa.field("lastContext_clientType", pa.string()),
        pa.field("lastContext_appVersion", pa.string()),
        pa.field("lastContext_demo", pa.bool_()),
        pa.field("source", pa.string()),
        pa.field("worldId", pa.string())
    ])


def batch_iterator(cursor, batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    """
    Yield flattened MongoDB documents in batches.
    """
    batch: List[Dict[str, Any]] = []

    for document in cursor:
        batch.append(flatten_document(document))

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def export_mongo_to_parquet(
    uri: str,
    db_name: str,
    collection_name: str,
    output_path: str,
    batch_size: int = 10_000,
    query_filter: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Export MongoDB documents to a Parquet file in batches.
    """
    query_filter = query_filter or {}
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    schema = build_arrow_schema()
    client = create_mongo_client(uri)
    writer = None
    total_rows = 0

    try:
        collection = client[db_name][collection_name]

        cursor = collection.find(
            filter=query_filter,
            projection=build_projection(),
        ).batch_size(batch_size)

        try:
            writer = pq.ParquetWriter(output_file, schema)

            for batch in batch_iterator(cursor, batch_size=batch_size):
                table = pa.Table.from_pylist(batch, schema=schema)
                writer.write_table(table)
                total_rows += len(batch)
                print(f"Written rows: {total_rows:,}")

        finally:
            cursor.close()

    finally:
        if writer is not None:
            writer.close()
        client.close()

    print(f"Export completed: {output_file}")
    print(f"Total rows exported: {total_rows:,}")