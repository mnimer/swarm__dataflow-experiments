## Create BQ Table
```sql
CREATE TABLE mock_data.default_tests (
 id STRING,
 name STRING,
 date_created TIMESTAMP DEFAULT current_timestamp()
);
```

