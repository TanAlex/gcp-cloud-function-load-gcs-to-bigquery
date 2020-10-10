# README

### Load from external table

## Setup environment if you run locally
export GOOGLE_APPLICATION_CREDENTIALS="path/to/GCP_Credential_file.json"

pipenv sync
pipenv shell

## Get the schema

```
bq show --schema --format=prettyjson warm-actor-291222:data_test.users 

[
  {
    "mode": "NULLABLE",
    "name": "title",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "age",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "rate",
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "id",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "date",
    "type": "DATE"
  }
]

# save that to table_schema.json
```
Add 
```
  {
    "mode": "Required",
    "name": "gcs_file_name",
    "type": "STRING"
  }
```
To the end of the table_schema.json so it will be used for the psudo column _FILE_NAME in external_table

```
# create a native table with same schema
# partitioned by `date` column
# partition auto expire after 86400s = 1 Day
# 2592000 
bq mk -t \
--schema table_schema.json \
--time_partitioning_field date \
--time_partitioning_type DAY \
--time_partitioning_expiration 2592000  \
--description "Demo partitioned table" \
--label env:dev \
warm-actor-291222:data_test.bq_users 

# insert from the view to the new table
INSERT INTO data_test.bq_users
SELECT title,age,rate,name,id,date FROM data_test.view_users;
```

To automatically insert load/insert new files to native table

```
SELECT * FROM data_test.bq_users WHERE date = '2020-11-02';

DELETE FROM data_test.bq_users 
WHERE date = '2020-11-02' and gcs_file_name in 
( SELECT DISTINCT _FILE_NAME as filename from data_test.users WHERE
date = '2020-11-02' );

INSERT INTO data_test.bq_users
SELECT *, _FILE_NAME as gs_file_name FROM data_test.users
WHERE date = '2020-11-02';
```

### Deploy cloud function to load

```
export MY_BUCKET=gs://warm-actor-291222_cloudbuild/
gcloud functions deploy load_from_external_table \
--source=load_from_external_table \
--entry-point=function \
--runtime python37 \
--timeout=400 \
--trigger-resource $MY_BUCKET \
--trigger-event google.storage.object.finalize
```