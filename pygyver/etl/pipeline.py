""" Module to ETL data to generate pipelines """
from __future__ import print_function
import asyncio
import logging
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
            logging.info(f"{message} {d[log]}")
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
        self.prod_project_id = 'copper-actor-127213'

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
        if not (batch.get('sheets', '') == '' or extract_args(batch.get('sheets', ''),  'load_google_sheet') == ''): 
            self.load_google_sheets(batch)
        # *** check create_tables ***
        if not (batch.get('tables', '') == '' or extract_args(batch.get('tables', ''),  'create_table') == ''): 
            self.create_tables(batch)
        # *** exec pk check ***
        if not (batch.get('tables', '') == '' or extract_args(batch.get('tables', ''),  'create_table') == '' or extract_args(batch.get('tables', ''),  'pk') == ''):  
            self.run_checks(batch) 

    def run(self):
        # run batches
        batch_list = self.yaml.get('batches', '')
        for batch in batch_list:
            self.run_batch(batch)
        # run release (ToDo)

    def extract_unit_tests(self, batch_list=None):
        """ return the list of unit test: unit test -> file, mock_file, output_table_name(opt) """
        # extract sql files and mock data
        batch_list = batch_list or self.yaml.get('batches', '')

        # initiate args and argsmock
        args, args_mock = [] , []

        # extracts files paths for unit tests 
        for batch in batch_list:            
            for table in batch.get('tables', ''):
                if table.get('create_table', '') != '' and table.get('mock_data', ''):                
                    args.append(table.get('create_table', ''))
                    args_mock.append(table.get('mock_data', ''))
        
        return_list = []
        for a, b in zip(args, args_mock):
            a.update(b)            
            return_list.append( dict(filter(lambda i:i[0] in ['mock_file', 'file', 'output_table_name'], a.items())))

        return return_list
        
    def extract_unit_test_value(self, unit_test_list):        
        for d in unit_test_list:
            d["sql"] = read_sql(d['file'])
            d["cte"] = read_sql(d['mock_file'])
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
                        log='file'                      
                        )
            return result

    def copy_prod_structure(self, table_list=''):
        args = []
        if table_list == '':
            table_list = self.yaml.get('table_list', '')
        # extract args        
        for table in table_list:
            args.append(
                {
                    "source_project_id" : self.prod_project_id,
                    "source_dataset_id" : table.split(".")[0], 
                    "source_table_id": table.split(".")[1],
                    "dest_dataset_id" : table.split(".")[0], 
                    "dest_table_id": table.split(".")[1]
                }                                    
            )            

        # copy tables structure
        if args != []:            
            result = execute_parallel(
                        self.bq.copy_table_structure,
                        args,
                        message='copy table structure for: ',  
                        log='source_table_id'                      
                        )
            return result


    def run_test(self):
        # unit test
        self.run_unit_tests()
        # copy table schema from prod
        # dry run


