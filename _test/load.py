from google.cloud import bigquery
client = bigquery.Client()
project = client.project
dataset_ref = bigquery.DatasetReference(project, 'my_dataset')
dataset_id = dataset_ref.dataset_id
filepath = 'path/to/your_file.csv'

# Retrieves the destination table and checks the length of the schema
table_id = "my_table"
table_ref = dataset_ref.table(table_id)
table = client.get_table(table_ref)
print("Table {} contains {} columns.".format(table_id, len(table.schema)))

# Configures the load job to append the data to the destination table,
# allowing field addition
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
# job_config.schema_update_options = [
#     bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
# ]
# In this example, the existing table contains only the 'full_name' column.
# 'REQUIRED' fields cannot be added to an existing schema, so the
# additional column must be 'NULLABLE'.
job_config.schema = [
    bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
]
# job_config.source_format = bigquery.SourceFormat.CSV
# job_config.skip_leading_rows = 1

with open(filepath, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # API request

job.result()  # Waits for table load to complete.
print(
    "Loaded {} rows into {}:{}.".format(
        job.output_rows, dataset_id, table_ref.table_id
    )
)

# Checks the updated length of the schema
table = client.get_table(table)
print("Table {} now contains {} columns.".format(table_id, len(table.schema)))