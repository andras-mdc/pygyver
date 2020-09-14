## Main features

### BigQueryExecutor

Python utility to perform task against the BigQuery API

#### Example

```python
from pygyver.etl.dw import BigQueryExecutor

# Login
db = BigQueryExecutor()

# Execute SQL query against BigQuery API
sql = "SELECT * FROM data.sale_order"
sale_order_data = db.execute_sql(
    sql=sql
)

# Execute SQL file against BigQuery API
sql_file = "src/extract_sale_order.sql"
sale_order_data = db.execute_file(
    file=sql_file
)
```
