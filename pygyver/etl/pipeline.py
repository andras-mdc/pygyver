""" Module to ETL data to generate pipelines """
from __future__ import print_function
import asyncio
from pygyver.etl.dw import read_sql 
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
        self.unit_test_list = self.extract_unit_tests()

    def create_tables(self, batch):
        batch_content = batch.get('tables', '')
        args = extract_args(batch_content, 'create_table')
        if args != []:            
            result = execute_parallel(
                        self.bq.create_table,
                        args,
                        message='Creating table:',
                        log='table_id'
                        )
            return result
            
    def load_google_sheets(self, batch):
        batch_content = batch.get('sheets', '')
        args = extract_args(batch_content, 'load_google_sheet')
        if args == []:
            raise Exception("load_google_sheet in yaml is not well defined")
        result = execute_parallel(
                    self.bq.load_google_sheet,
                    args,
                    message='Loading table:',
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
        ''' batch executor - this is a mvp, it can be widely improved '''
        # *** check load_google_sheets ***
        if not (batch.get('tables', '') == '' or extract_args(batch.get('tables', ''),  'load_google_sheet') == ''): 
            self.create_tables(batch)
        # *** check create_tables ***
        if not (batch.get('tables', '') == '' or extract_args(batch.get('tables', ''),  'create_table') == ''): 
            self.create_tables(batch)
        # *** exec pk check ***
        if not (batch.get('tables', '') == '' or extract_args(batch.get('tables', ''),  'create_table') == '' or extract_args(batch.get('tables', ''),  'pk') == ''):  
            self.create_tables(batch) 

    def run(self):
        # run batches
        batches_content = self.yaml.get('batches', '')
        batch_list = extract_args(batches_content, 'batch')
        for batch in batch_list:
            self.run_batch(batch)
        # run release (ToDo)

    def extract_unit_tests(self, batch_list=None):
        """ return the list of unit test: unit test -> file, mock_file, output_table_name(opt) """
        # extract sql files and mock data
        batch_list = batch_list or self.yaml.get('batches', '')

        # initiate args and argsmock
        args, args_mock = [] , []

        # extract file from create_table 
        for batch in batch_list:
            batch_content = batch.get('tables', '')
            args += extract_args(batch_content, 'create_table')
            args_mock += extract_args(batch_content, 'mock_data')            
        
        return_list = []
        for a, b in zip(args, args_mock):
            a.update(b)            
            return_list.append( dict(filter(lambda i:i[0] in ['mock_file', 'file', 'output_table_name'], a.items())))

        return return_list
        
    def extract_unit_test_value(self, unit_test_list):        
        for d in unit_test_list:
            d["sql"] = read_sql(d['file'])
            d["cte"] = read_sql(d['mock_file'])
            d.pop("file", None)
            d.pop("mock_file", None)
        return unit_test_list

    def run_unit_tests(self, yaml_content=None):
        yaml_content = yaml_content or self.yaml
        # extract unit tests
        list_unit_test = self.extract_unit_tests()
        args = self.extract_unit_test_value(list_unit_test)
        if args != []:            
            result = execute_parallel(
                        self.bq.assert_acceptance,
                        args,
                        message='Asserting sql',                        
                        )
            return result

<<<<<<< HEAD
    def dry_run(self):
        self.run()
        
    def run_test(self):        
        self.run_unit_tests() # unit test
        # copy table schema from prod
        self.run() # run pipeline on emppty table structure
=======
    def run_test(self):
        # unit test
        self.run_unit_tests()
        # copy table schema from prod
        # dry run
>>>>>>> c4d77b64a3958c1a97424bf9d9b1cba463618d72


