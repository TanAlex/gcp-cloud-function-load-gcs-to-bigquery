from google.cloud import bigquery
import json
import re

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

# take all the partition pair like 
# "/date=2020-10-01/hr=12/min=12/", "/date=2020-10-02/hr=12/min=13/"
# to sql like "date in ('2020-10-01','2020-10-01') and hr in (12) and min in (12,13)"
def get_partition_sql_from_file_names(file_names):
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



# gs_file_names = ["gs://warm-actor-291222_cloudbuild/test-data/date=2020-11-02/data-20201101-003011.json.gz"]
def load_files(gs_file_names):
    partition_sql = get_partition_sql_from_file_names(gs_file_names)
    gs_file_names_string = ",".join([ f"'{f}'" for f in gs_file_names])
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

