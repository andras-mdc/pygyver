from pygyver.etl.lib import get_dataset_prefix
from pygyver.etl.dw import BigQueryExecutor

dataset_prefix = get_dataset_prefix()
target_dataset_id = (dataset_prefix if dataset_prefix else "") + "test"


bq = BigQueryExecutor()
bq.create_dataset(target_dataset_id)
bq.create_table(
    dataset_id=target_dataset_id,
    table_id="table1",
    file="tests/sql/table1.sql"
)
