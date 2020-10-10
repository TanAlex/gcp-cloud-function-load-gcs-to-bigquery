from google.cloud import bigquery
from typing import List
import json
import re
import time

client = bigquery.Client()
project = client.project
dataset_ref = bigquery.DatasetReference(project, 'data_test')
dataset_id = dataset_ref.dataset_id

# Retrieves the destination table and checks the length of the schema
table_id = "users"
table_ref = dataset_ref.table(table_id)
table = client.get_table(table_ref)
print("Table {} contains {} columns.".format(table_id, len(table.schema)))
print(table.schema)

def get_partition_sql_from_file_names(file_names: List[str]) -> str:
    """
    Summary: \n
        Function to extract partition info from the gcs file name to compose the SQL\r
        take all the partition pair like \r
        "/date=2020-10-01/hr=12/min=12/", "/date=2020-10-02/hr=12/min=13/" \r
        to sql like "date in ('2020-10-01','2020-10-01') and hr in (12) and min in (12,13)"

    Args:
        file_names (list[str]): A list of GCS file path strings

    Returns:
        str: The 
    """
    pattern = re.compile(r'\/(\w+)=([0-9\-]+)')
    kv = {}
    for file_name in file_names:
        for match in pattern.finditer(file_name):
            if match:
                k = match.group(1)
                v = match.group(2)
                if k in kv:
                    kv[k].append(v)
                else:
                    kv[k] = [v]
    statements = []
    for k in kv:
        # add single quote if v is not a digital number
        v = map(lambda v: f"'{v}'"  if not re.match(r'^\d+$', v) else v, kv[k])
        statements.append(f"{k} in ( "+ ",".join(v) + " )")
    return " and ".join(statements)


def load_files(gs_file_names: List[str]) -> None:
    """Function to load a list of GCS files

    Args:
        gs_file_names (list[str]): A list of GCS file path strings
    Returns:
        None: the output is written to Stackdriver Logging
    """

    partition_sql = get_partition_sql_from_file_names(gs_file_names)
    gs_file_names_string = ",".join([ f"'{f}'" for f in gs_file_names])
    sql = f"""
    SELECT 1 FROM data_test.users 
    WHERE {partition_sql} and _FILE_NAME in ({gs_file_names_string});
    """

    # when we gets triggered, the file is uploaded but it will take some time 
    # to show up in external_table, so we loop/wait for 3 minutes (18 round sleep 10s)
    loop_cnt = 0
    while (loop_cnt < 18):
        time.sleep(10)
        results = client.query(sql)
        print(f"resuls count: {len(list(results))}")
        if len(list(results)) > 0:
            loop_cnt = 1000
        loop_cnt += 1
    if loop_cnt < 1000:  # we timed out 
        print("timed out, the external table doesn't have the new uploaded data in GCS.")
        return
    sql = f"""
    SELECT * FROM data_test.bq_users 
    WHERE {partition_sql} and gcs_file_name in ({gs_file_names_string});
    """
    print(sql)
    results = client.query(sql)
    print(list(results))
    if len(list(results)) > 0:
        sql = f"""
    DELETE FROM data_test.bq_users 
    WHERE {partition_sql} and gcs_file_name in ({gs_file_names_string});
    """
        print(sql)
        results = client.query(sql)

    sql = f"""
    INSERT INTO data_test.bq_users
    SELECT *, _FILE_NAME as gcs_file_name FROM data_test.users
    WHERE {partition_sql} and _FILE_NAME in ({gs_file_names_string});
    """
    print(sql)
    query_job = client.query(sql)
    results = query_job.result()
    print(results)

def function(event, context):
    """Cloud Function to be triggered by Cloud Storage.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))
    gs_file_name = f"gs://{event['bucket']}/{event['name']}"
    print(f"bucket_name: {gs_file_name}")
    if gs_file_name.lower().endswith('json.gz'):
        load_files([gs_file_name])
    else:
        print(f"File: {gs_file_name} does not end with 'json.gz', skip loading.")

if __name__ == "__main__":
    # this is just for local testing
    gs_file_names = ["gs://warm-actor-291222_cloudbuild/test-data/date=2020-11-02/data-20201101-003011.json.gz",
    "gs://warm-actor-291222_cloudbuild/test-data/date=2020-09-01/data-20200901-000001.json.gz"]
    load_files(gs_file_names)
