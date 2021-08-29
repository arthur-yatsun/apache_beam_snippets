link on google cloud doc page
https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python

how to run
```
python -m apache_beam.examples.wordcount \
    --region DATAFLOW_REGION \
    --input gs://dataflow-samples/shakespeare/kinglear.txt \
    --output gs://STORAGE_BUCKET/results/outputs \
    --runner DataflowRunner \
    --project PROJECT_ID \
    --temp_location gs://STORAGE_BUCKET/tmp/
```

