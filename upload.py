import json

from google.cloud import bigquery
import pandas as pd

from config import (
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID,
    LOCATION
)


def upload():
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    df = pd.read_csv('member-upload-template.csv')
    json_data = json.dumps(df.to_dict(orient="records"))
    query = f"""
        MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` AS target
        USING (
            SELECT 
                JSON_VALUE(member, "$.email") AS email,
                JSON_VALUE(member, "$.label") AS label,
                JSON_VALUE(member, "$.note") AS note
            FROM UNNEST(JSON_QUERY_ARRAY(@members)) AS member
        ) AS source
        ON target.email = source.email
        WHEN NOT MATCHED THEN
            INSERT (email, label, note, created_at, updated_at, status)
            VALUES (source.email, source.label, source.note, TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND), 'pending')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("members", "STRING", json_data)
        ]
    )
    job = client.query(query, job_config=job_config)
    job.result()


if __name__ == "__main__":
    upload()