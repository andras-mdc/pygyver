# Main features

- [PipelineExecutor](#pipelineexecutor)
  * [Getting Started](#Getting-started)
  * [YAML File Structure](#YAML-file-structure)
    + [Description](#Description)
    + [Batches](#Batches)
    + [Table List](#Table-List)
    + [Releases](#Releases)
    + [Example](#Example)
  * [Usage](#Usage)
    + [Execute Pipeline](#Execute-Pipeline)
    + [Execute Tests](#Execute-Tests)

## PipelineExecutor

> Uses YAML file to build data pipelines.

> Wraps several pygver modules (`dw`, `storage`...)

### Getting Started

`PipelineExecutor` takes `yaml_file` as a mandatory argument, which should be set to the path to your YAML file from the environment variable `PROJECT_ROOT`. For example, if my folder structure is a below:

```
src
    pipeline
        sql
        schema
        pipeline.py
        pipeline.yaml
```

with 

- `pipeline` as my pipeline folder
- `sql` as a folder containing SQL files
- `schema` as a folder contain schema files
- `pipeline.py` being the python file which will execute the product pipeline
- `pipeline.yaml` being the pipeline YAML file

and 

- `PROJECT_ROOT` set as `src`

`PipelineExecutor` can be initiated in `pipeline.py` with:

```python
from pygver.etl.pipeline import PipelineExecutor

pipeline = PipelineExecutor(
    yaml_file='pipeline/pipeline.yaml'
)
```

### YAML file structure

The YAML file should contain the following keys:

| Key | Description | Details |
| ------------- |-------------|-------------|
| `desc` | Description of the pipeline | [Description]() |
| `batches` | Contains the list of task to be executed. Task can be executed in serial or parallel | [Batches]() |
| `table_list` | List of all the tables either referenced or created in the pipeline | [Table List]() |
| `releases` | Execute release files | [Releases]() |

#### Description

A brief description of what the data pipeline does can be supplied via the `desc` key. The aim is to understand quickly what the data pipeline does.

Example:
```yaml
desc: This pipeline creates the table data.pipeline in BigQuery.
```

#### Batches

Specifies the task that need to be executed and how to execute them. 

For each item in `batches`, we should declare:

- a description of the batch using `desc`
- the type of data source we are trying to load data from. Can be `tables` (BigQuery) or `sheets` (Google sheets)
- Within the data source, the task we would like to execute.

Task available for `sheets`:
- load_google_sheet: Load data from Google Sheet into a BigQuery table. It uses the method `load_google_sheet` from `BigQueryExecutor`.

Tasks available for `tables`:
- create_table: Create a BigQuery table via a SQL file. It uses the method `create_table` from `BigQueryExecutor`.
- create_partition_table: Create a BigQuery partition table via a SQL file. It uses the method `create_partition_table` from `BigQueryExecutor`.

Args can be applied to each task in a similar manner than when using `BigQueryExecutor`.

Simple example with one task:
```yaml
batches:
  - desc: Create table in staging
    tables:
      - table_desc: Staging table for pipeline
        create_table:
          table_id: pipeline
          dataset_id: staging
          schema_path: pipeline/schema/staging_pipeline.json
          file: pipeline/sql/staging_pipeline.sql
        pk: [pk1, pk2]
        mock_data: 
          mock_file: product/mock/staging_pipeline.sql
          output_table_name: staging.pipeline
```

Tasks can be executed in serial in several batches by adding new item to `batches`:

Serial execution of two tasks:
```yaml
batches:
  - desc: Create table in staging
    tables:
      - table_desc: Staging table for pipeline
        create_table:
          table_id: pipeline
          dataset_id: staging
          schema_path: pipeline/schema/staging_pipeline.json
          file: pipeline/sql/staging_pipeline.sql
        pk: [pk1, pk2]
        mock_data: 
          mock_file: product/mock/staging_pipeline.sql
          output_table_name: staging.pipeline
  - desc: Create pipeline_2 in staging
    tables:
      - table_desc: Staging table for pipeline
        create_table:
          table_id: pipeline_2
          dataset_id: staging
          schema_path: pipeline/schema/staging_pipeline_2.json
          file: pipeline/sql/staging_pipeline_2.sql
        pk: [pk]
        mock_data: 
          mock_file: product/mock/staging_pipeline_2.sql
          output_table_name: staging.pipeline_2
```

Tasks can also executed in parallel by adding new item to the batch:

Parallel execution of two tasks:
```yaml
batches:
  - desc: Create table and pipeline_2 in staging
    tables:
      - table_desc: Staging table for pipeline
        create_table:
          table_id: pipeline
          dataset_id: staging
          schema_path: pipeline/schema/staging_pipeline.json
          file: pipeline/sql/staging_pipeline.sql
        pk: [pk1, pk2]
        mock_data: 
          mock_file: product/mock/staging_pipeline.sql
          output_table_name: staging.pipeline
      - table_desc: Staging table for pipeline
        create_table:
          table_id: pipeline_2
          dataset_id: staging
          schema_path: pipeline/schema/staging_pipeline_2.json
          file: pipeline/sql/staging_pipeline_2.sql
        pk: [pk]
        mock_data: 
          mock_file: product/mock/staging_pipeline_2.sql
          output_table_name: staging.pipeline_2
```


#### Table List

```yaml
table_list:
- data_lake.pipeline
- staging.pipeline
- data.pipeline
```


#### Releases

```yaml
releases:
  - date: 2020-07-24
    description: Execute release file
    python_files:
      - pipeline/releases/release_pipeline.py
```

#### Example

```yaml
desc: My data pipeline description

batches:

  - desc: Create table in staging
    tables:
      - table_desc: Staging table for pipeline
        create_table:
          table_id: pipeline
          dataset_id: staging
          schema_path: pipeline/schema/staging_pipeline.json
          file: pipeline/sql/staging_pipeline.json
        pk: [pk]

  - desc: Create table in data
    tables:
      - table_desc: Final table for pipeline
        create_table:
          table_id: pipeline
          dataset_id: data
          schema_path: pipeline/schema/data_pipeline.json
          file: pipeline/sql/data_pipeline.json
        pk: [pk]
        mock_data: 
          mock_file: product/mock/data_pipeline.sql
          output_table_name: data.pipeline   

table_list:
- data_lake.pipeline
- staging.pipeline
- data.pipeline

releases:
  - date: 2020-07-24
    description: Execute release file
    python_files:
      - pipeline/releases/release_pipeline.py
```

### Usage

#### Execute pipeline

A pipeline can be executed using the method `run`:

```python
from pygyver.etl.pipeline import PipelineExecutor

pipeline = PipelineExecutor(
    "pipeline.yaml"
)

pipeline.run()
```

### Execute tests

Two types are tests are available: unit test and dry run test.

#### Unit test

The unit test aim at testing the logic of a SQL query. To execute a unit test:

```python
from pygyver.etl.pipeline import PipelineExecutor

pipeline = PipelineExecutor(
    "promotions/promotions.yaml",
    dry_run=True
)

pipeline.run_unit_tests()
```

#### Dry run test

The dry run test aim at testing whether the pipeline can be executed using the structure currently set in production. To execute a dry run test, you will first need to copy the prod structure using `copy_prod_structure`, and then execute the pipeline using the `run` method. The `dry_run_clean` method should always be executed afterwards to clean up the environment: 

```python
from pygyver.etl.pipeline import PipelineExecutor

pipeline = PipelineExecutor(
    "promotions/promotions.yaml",
    dry_run=True
)

try:
    pipeline.copy_prod_structure()
    pipeline.run()
finally:
    pipeline.dry_run_clean()
```
