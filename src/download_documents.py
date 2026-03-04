# getting documents with tagged salesforce entity
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import athena


def get_query():
    query = """
        SELECT 
            id,
            story_id,
            title,
            summary,
            wiki_title,
            sentiment_decision,
            sentiment_positive
        FROM (
            SELECT 
                id,
                story_id,
                title,
                summary,
                elem.wiki_title AS wiki_title,
                elem.sentiment_tabs.decision AS sentiment_decision,
                elem.sentiment_tabs.positive AS sentiment_positive,
                ROW_NUMBER() OVER (PARTITION BY story_id ORDER BY elem.sentiment_tabs.positive DESC) AS rn
            FROM "articles"."articles_hudi_copy_on_write"
            CROSS JOIN UNNEST(entities) AS t(elem)
            WHERE story_published_partition_date BETWEEN '2025-10-01' AND '2026-01-01'
            AND elem.wiki_title = 'Salesforce.com'
        )
        WHERE rn = 1
    """
    return query


def download_document(dest: str, fmt: str = "parquet") -> None:
    query = get_query()
    df = athena.read(query)

    if df is None:
        raise ValueError("Athena query returned zero rows")

    os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)

    if fmt == "parquet":
        df.to_parquet(dest, index=False)
    elif fmt == "json":
        df.to_json(dest, orient="records", lines=True)
    else:
        raise ValueError(f"Unsupported format: {fmt}. Choose 'parquet' or 'json'.")

    print(f"Saved {len(df)} rows to {dest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Salesforce-tagged articles from Athena")
    parser.add_argument(
        "dest",
        nargs="?",
        default="data/download_sf_dataset.parquet",
        help="Output file path (default: data/download_sf_dataset.parquet)",
    )
    parser.add_argument(
        "--format",
        choices=["parquet", "json"],
        default="parquet",
        help="Output format (default: parquet)",
    )
    args = parser.parse_args()

    download_document(args.dest, fmt=args.format)
