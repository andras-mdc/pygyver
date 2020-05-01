""" Module to ETL data to generate pipelines """
from __future__ import print_function
import asyncio
from pygyver.etl.lib import extract_args
from pygyver.etl.dw import BigQueryExecutor
from pygyver.etl.toolkit import read_yaml_file


def async_run(func):
    def async_run(*args, **kwargs):
        asyncio.run(func(*args, **kwargs))
    return async_run


async def execute_func(func, **kwargs):
    func(**kwargs)
    return True


@async_run
async def execute_parallel(func, args, message='running task', log=''):
    """
    execute the functions in parallel for each list of parameters passed in args

    Arguments:
    func: function as an object
    args: list of function's args

    """
    tasks = []
    count = []
    for d in args:
        if log != '':
            print(f"{message} {d[log]}")
        task = asyncio.create_task(execute_func(func, **d))
        tasks.append(task)
        count.append('task')
    await asyncio.gather(*tasks)
    return len(count)


class PipelineExecutor:
    def __init__(self, yaml_file):
        self.yaml = read_yaml_file(yaml_file)
        self.bq = BigQueryExecutor()

    def create_tables(self, batch):
        batch_content = batch.get('tables', '')
        args = extract_args(batch_content, 'create_table')
        if args == []:
            raise Exception("tables in yaml is not well defined")
        result = execute_parallel(
                    self.bq.create_table,
                    args,
                    message='Creating table:',
                    log='table_id'
                    )
        return result

    def run_checks(self, batch):
        batch_content = batch.get('tables', '')
        args = extract_args(batch_content, 'create_table')
        args_pk = extract_args(batch_content, 'pk')
        for a, b in zip(args, args_pk):
            a.update({"primary_key": b})
        result = execute_parallel(
                    self.bq.assert_unique,
                    args,
                    message='Run pk_check on:',
                    log='table_id'
                    )
        return result

    def run_batch(self, batch):
        # *** create tables ***
        self.create_tables(batch)
        # *** exec pk check

    def run(self):
        batches_content = self.yaml.get('batches', '')
        batch_list = extract_args(batches_content, 'batch')
        for batch in batch_list:
            self.run_batch(batch)

    def run_test(self):
        # unit test
        # copy table schema from prod
        # dry run
        pass
