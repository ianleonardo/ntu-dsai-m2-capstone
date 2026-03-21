from google.cloud import bigquery
from .config import settings

def get_bigquery_client():
    return bigquery.Client(project=settings.GOOGLE_PROJECT_ID)

def query_bigquery(query: str, params=None):
    client = get_bigquery_client()
    job_config = bigquery.QueryJobConfig(query_parameters=params) if params else None
    query_job = client.query(query, job_config=job_config)
    return query_job.to_dataframe()
