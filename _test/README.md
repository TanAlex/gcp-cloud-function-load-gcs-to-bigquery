# README

Try out simple cloud function `hello_gcs`

```
export MY_BUCKET=gs://warm-actor-291222_cloudbuild/
gcloud functions deploy hello_gcs \
--source=_test/hello_gcs \
--runtime python37 \
--trigger-resource $MY_BUCKET \
--trigger-event google.storage.object.finalize

```