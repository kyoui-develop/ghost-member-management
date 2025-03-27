import jwt
import pendulum
from google.cloud import bigquery

from config import (
    GHOST_ADMIN_API_KEY,
    PROJECT_ID,
    DATASET_ID,
    TABLE_ID,
    LOCATION
)


def generate_jwt():
    id, secret = GHOST_ADMIN_API_KEY.split(':')
    iat = int(pendulum.now().timestamp())
    header = {'alg': 'HS256', 'typ': 'JWT', 'kid': id}
    payload = {
        'iat': iat,
        'exp': iat + 5 * 60,
        'aud': '/admin/'
    }
    return jwt.encode(payload, bytes.fromhex(secret), algorithm='HS256', headers=header)


def get_pending_members(count):
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    query = f"""
    SELECT email, label, note
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE status = 'pending'
    ORDER BY RAND()
    LIMIT {count}
    """
    job = client.query(query)
    return job.to_dataframe()


def get_active_members():
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    query = f"""
    SELECT email
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE status = 'active'
    """
    job = client.query(query)
    return job.to_dataframe()


def update_member_status(emails, status):
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    query = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    SET status = '{status}', updated_at = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND)
    WHERE email IN ({', '.join([f"'{email}'" for email in emails])})
    """
    job = client.query(query)
    job.result()


def upsert_memebrs(json_data):
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
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
        WHEN MATCHED THEN
            UPDATE SET 
                status = 'active',
                updated_at = TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND)
        WHEN NOT MATCHED THEN
            INSERT (email, label, note, created_at, updated_at, status)
            VALUES (source.email, source.label, source.note, TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND), TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), SECOND), 'active')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("members", "STRING", json_data)
        ]
    )
    job = client.query(query, job_config=job_config)
    job.result()