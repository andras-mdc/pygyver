# Automating mock data and schema file creation

Convert data from a google spreadsheet to a sql cte for the mock files and the schema to the required json file.

[AT-2658](https://madecom.atlassian.net/browse/AT-2658)

## Format requirements

[Example spreadsheet](https://docs.google.com/spreadsheets/d/11DijUjFfTWshz_rDw2nZsFXXuu5TUVpd8w6EuuwXdRQ)

- schema on 'schema' tab, cte mock on 'acceptance' tab
- schema to have dataset.table in row 1
- schema to have columns 'name', 'type', 'description' (optional) and 'mode' (optional)
- acceptance to have scenarios as column A (optional)
- acceptance to have dataset.table names in row 1
- acceptance to have column names in row 2
- acceptance to have column types in row 3
- have empty fields as empty or 'NULL'
- cell functions are accepted
- have no other cells filled on the worksheets

## Run

- share gs with access token email - `USER@PROJECT.iam.gserviceaccount.com`
- if running in dev environment, make sure that the Google Drive API is enabled for the GCP project in the API library


### Run via Makefile (analytics-transformer)

NOTE:
- args spreadsheet key (KEY) and path+file name prefix (without extensions - FILEPATH) -> the script will create .json and .sql files from it
- run source-load-local-vars.sh and make sure that the GOOGLE_APPLICATION_CREDENTIALS env var is setup (should be added to build/local/local.env)
- make sure that the latest pygyver is installed in your local env, pip install --upgrade pygyver

```
make run-load-acceptance KEY=11DijUjFfTWshz_rDw2nZsFXXuu5TUVpd8w6EuuwXdRQ FILEPATH=path/attr_keyword_stats_organic
```


### Run individually

```python
from pygyver.etl.gs import load_gs_to_json_schema, load_gs_to_sql_mock
```

1. load schema example

```python
gs_to_json_schema(
    key: "11DijUjFfTWshz_rDw2nZsFXXuu5TUVpd8w6EuuwXdRQ",
    schema_path: "path/attr_keyword_stats_organic.json"
)
```

2. load mock cte sql example

```python
gs_to_sql_mock(
    key: "11DijUjFfTWshz_rDw2nZsFXXuu5TUVpd8w6EuuwXdRQ",
    mock_path: "path/attr_keyword_stats_organic.sql"
)
```

## Tests
- the two main functions (load_gs_to_json_schema, load_gs_to_sql_mock) are tested against their expected output for all datatypes


## Notes
- future functionality : adding capability to format REPEATED / RECORD nested columns properly. for now, just add them as normal columns and format the output files as needed (don't have '.' in column names)
