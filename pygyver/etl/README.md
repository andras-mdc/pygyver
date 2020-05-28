# Class BigQueryExecutor

BigQuery functions wrap `google.cloud.bigquery` functions to provide higher level API removing boilerplate instructions of the lower level API.

## Methods

### 

#  Class PipelineExecutor

Tool to build and run pipelines, it wraps pygyver.etl.dw, pygyver.etl.storage classes and more.

init: pipeline yaml file

## Methods
- run()
- run_test()

## YAML Definition

structure:

```
desc: 
datasets:
batches:    
release:
table_list:
```

### desc

pipeline description

### dataset

list of dataset referenced in the pipeline

```
['dataset1', 'dataset2', 'dataset3']
```
### table_list

list of all the tables either referenced or created in the pipeline

- table structures get copied from Prod to Local

```
table_list:
- flat_events.order_placed_v2
- staging.saleorder
- data.saleorder
- reporting.out_saleorder
```

### batches

- contains the list of batches to execute
- the batches are execute in serial
- task within the batch runs in parallel


```
batches:
-   desc:
    sheets:
-   desc:
    tables:
```

#### type of batch

- sheets

```
-   desc: load test spreadsheet into bigquery      
    sheets:                                                                 *** contains the list of table to be created
    -   table_desc: ref sheet1                                              *** descriptive
        load_google_sheet:                                                  *** contains the args of pygyver.etl.dw ->  load_google_sheet()
            table_id: ref_sheet1                                            *** mandatory
            dataset_id: test 
            googlesheet_key: 19Jmapr9G1nrMcW2QTpY7sOvKYaFXnw5krK6dD0GwEqU   *** mandatory 
            googlesheet_link: https://docs.google.com/spreadsheets/d/19Jmapr9G1nrMcW2QTpY7sOvKYaFXnw5krK6dD0GwEqU/edit#gid=0                                                      *** descriptive
    -   table_desc: make a second copy of ref sheet1 in it ref_sheet2 table
        load_google_sheet:
            table_id: ref_sheet2
            dataset_id: test
            googlesheet_key: 19Jmapr9G1nrMcW2QTpY7sOvKYaFXnw5krK6dD0GwEqU
            googlesheet_link: https://docs.google.com/spreadsheets/d/19Jmapr9G1nrMcW2QTpY7sOvKYaFXnw5krK6dD0GwEqU/edit#gid=0
```

- tables

```
-   desc: create table1 & table2 in staging                             *** descriptive
    tables:                                                             *** list of table to be created
    -   table_desc: table1                                              *** descriptive
        create_table:                                                   *** contains the args of pygyver.etl.dq -> create_table()
            table_id: table1                                            *** mandatory
            dataset_id: test                                            *** mandatory
            file: tests/sql/table1.sql                                  *** mandatory
        pk:                                                             *** define the composite pk
        -   col1
        -   col2
        mock_data:                                                      *** contains the info to test the sql
            mock_file: sql/mocked_for_table1.sql                        *** mock data file
            output_table_name: expected_output_something                *** output_table_name specified in the mock_file
    -   table_desc: table2
        create_table:
            table_id: table2
            dataset_id: test
            file: tests/sql/table2.sql
        pk:
        -   col1
        -   col2
        mock_data:  *** cte to test the sql file
            mock_file: sql/mocked_for_table2.sql                        
            output_table_name: expected_output_something                
```


### Release

to be defined

example
```
release:
  release_log: path_to_release_log.yml
  date: "2020-05-07"
  desc: "release of order-item to prod"
  files:
    - path_to_file1
    - path_to_file2
    - path_to_file3
```

# Usage

- for running the Pipeline

```
import pytest
pipeline = PipelineExecutor(
    "path_to_pipeline_folder/pipeline_name.yaml")     # initiate PipelineExecutor for Run
pipeline.run()                                        # run the pipeline
```

- for ci-cd (testing)

```
pipeline = PipelineExecutor(
    "order_items/order_items.yaml", 
    dry_run=True
    )                                                 # initiate PipelineExecutor for testing
pipeline.run_unit_tests()                             # runs unit tests
pipeline.copy_prod_structure()                        # copy table structure from prod
pipeline.run()                                        # run the pipeline on empty tables (dry_run)
pipeline.dry_run_clean()                              # cleans the dev/test environment
```
