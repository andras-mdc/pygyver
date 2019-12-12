#  pygyver

Contains Data Enginner, Data Scientist and 3rd party integration tools capabilities

## Installation
py-test-utility can be installed via pip

```python 
pip install ??find the name??
```


## BigQuery ##

To access BigQuery you will need following environment variables:

- `BIGQUERY_PROJECT` - name of the project in BigQuery. Default project cannot be changed in the code.
- `BIGQUERY_ACCESS_TOKEN_PATH` - path to the json token file.

BigQuery functions wrap `bigrquery` functions to provide higher level API removing boilerplate instructions of the lower level API.

## [tdd_utility - module](../master/data_prep/README.md)

### class load_data(type,file,schema) 
Contains methods to extract the equivalent json from csv with nested and repeated records structures

#### Methods
- to_json()
    - if successfuls return the json obj extracted from the csv

- to_new_line_delimiter_file(output_file_name)
    - return 0 if successfuls 
    - create new line delimiter "output_file_name" file


