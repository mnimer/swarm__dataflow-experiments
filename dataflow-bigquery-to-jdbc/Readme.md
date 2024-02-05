## Build
```
 gcloud builds submit
```

## Run Flex Template
```
 gcloud dataflow flex-template run dataflow-bigquery-to-jdbc-2 \
 --template-file-gcs-location=gs://argolis-playground_dataflow/templates/dataflow-bigquery-to-jdbc.json \
 --region=us-central1 \
 --parameters=bqTable="bigquery-public-data.baseball.games_post_wide",destDatabase="postgres",driver="org.postgresql.Driver",url="jdbc:postgresql://10.14.176.3:5432/postgres",username="postgres",password=""
```
# // bigquery-public-data.baseball.schedules,