import argparse
import json
import os
import uuid

import awswrangler as wr
import pandas as pd

from typing import Optional


def read(
    query: str,
    params=None,
    workgroup="platform-api-dev",
    max_cache_seconds=60 * 60 * 24 * 7,
    max_cache_query_inspections=2000,
    unload_approach=True,
    ctas_approach=False,
) -> Optional[pd.DataFrame]:
    """wrapper around AWS Athena

    Args:
        query (str): sql query
        params (dict, optional): mapping of sql parameters to values. Defaults to None.

    Returns:
        pd.DataFrame: pandas DataFrame
    """
    if params is None:
        params = {}
    try:
        return wr.athena.read_sql_query(
            query,
            "articles",
            workgroup=workgroup,
            s3_output=f"s3://temp.athena.signal/{str(uuid.uuid4())}.parquet",
            unload_approach=unload_approach,
            ctas_approach=ctas_approach,
            athena_cache_settings={
                "max_cache_seconds": max_cache_seconds,
                "max_cache_query_inspections": max_cache_query_inspections,
            },
            params=params,
        )
    except wr.exceptions.EmptyDataFrame:
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('dest')
    parser.add_argument('query')
    parser.add_argument('params')
    args = parser.parse_args()
    args = vars(parser.parse_args())

    with open(args['query'], 'r') as stream:
        query = stream.read()

    with open(args['params'], 'r') as stream:
        params = json.load(stream)

    documents_df = read(query, params)

    dirname = os.path.dirname(args['dest'])
    os.makedirs(dirname, exist_ok=True)

    if documents_df is not None:
        documents_df.to_parquet(args['dest'])
    else:
        raise ValueError('Athena query returned zero rows')