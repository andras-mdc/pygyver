""" Module to ETL data to generate pipelines """
from __future__ import print_function
import logging
import asyncio
from pygyver.etl.lib import extract_args
from pygyver.etl.dw import BigQueryExecutor
from pygyver.etl.toolkit import read_yaml_file


async def execute_func(func, **kwargs):
    func(**kwargs)
    return True


async def execute_parallel(func, args, message='running task', log=''):
    tasks = []
    count = []
    for d in args:
        if log != '':
            print(f"{message} {d[log]}")  # to be replaced with logging
        task = asyncio.create_task(execute_func(func, **d))
        tasks.append(task)
        count.append('task')
    await asyncio.gather(*tasks)
    return count


class PipelineExecutor:
    def __init__(self, yaml_file):
        self.yaml = read_yaml_file(yaml_file)
        self.bq = BigQueryExecutor()

    def create_tables(self, batch):
        batch_content = batch.get('batch','')
        args = extract_args(batch_content, 'table')
        if args == []:
            raise Exception("tables in yaml is not well defined")
        result = asyncio.run(
            execute_parallel(
                self.bq.create_table,
                args,
                message='Creating table:',
                log='table_id'
                )
            )
        return result

    def run_batch(self, batch):
        # *** create tables ***
        self.create_tables(batch)
        # *** exec pk check

    def run(self):
        for batch in self.yaml:
            self.run_batch(batch)

    def run_test(self):
        # unit test
        # copy table schema from prod
        # dry run
        pass
