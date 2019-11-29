# tdd_utility - module

## class load_csv(file, type, schema) 
Contains methods to extract the equivalent json from csv with nested and repeated records structures


### Args
- file
    - path and file name of the file
    - mandatory
    - nested fields shall be separated by a dot "."  (i.e. item.id, item.quantity)

|order | item.id | item.quantity | delivery.address | delivey.postcode |
|---|---|---|---|---|
| A0001 | item1 | 5 | address1 | e13bp |
| | item2 | 1 | | |
| | item3 | 3 | | |
| A0002 | item4 | 4  | address4 | e13bp |
| | item1 | 4 | | |
| | item3 | 2 | | |
- type 
  - default = csv
  - list of types: type
- schema 
    - path and schema file name of the table schema
    - required if the file contain nested and repeated records
    - json format i.e. 
```json
[  
    {
      "mode": "NULLABLE", 
      "name": "order", 
      "type": "STRING"
    },  
    {
      "fields": [
        {
          "mode": "NULLABLE", 
          "name": "id", 
          "type": "STRING"
        },
        {
          "mode": "NULLABLE", 
          "name": "quantity", 
          "type": "STRING"
        }
      ], 
      "mode": "REPEATED", 
      "name": "item", 
      "type": "RECORD"
    }, 
    {
      "fields": [
        {
          "mode": "NULLABLE", 
          "name": "address", 
          "type": "STRING"
        }, 
        {
          "mode": "NULLABLE", 
          "name": "postcode", 
          "type": "STRING"
        }
      ], 
      "mode": "NULLABLE", 
      "name": "delivery", 
      "type": "RECORD"
    }
  ]
```

### Methods
- to_json()
    - if successfuls return the json extracted from the csv

- to_new_line_delimiter_file(output_file_name)
    - return 0 if successfuls 
    - create new line delimiter "output_file_name" file


### Usage

```python 
>>> from data_prep import tdd_utility as  tu
>>> mockdata_csv = tu.load_csv(
...     csv="path/to/filename/file.csv", 
...     schema="path/to/schema/schema.json") # initialise the object
>>> mockdata_json = mockdata_csv.to_json() # return the equivalent json
>>> mockdata_json = mockdata_csv.to_new_line_delimiter_file(output="path/output_file_name.json") # return output_file_name
```
