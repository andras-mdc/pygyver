""" Module to ETL data to generate pipelines """
from __future__ import print_function
import random
import copy
import asyncio
import logging
from pygyver.etl.dw import read_sql
from pygyver.etl.lib import apply_kwargs
from pygyver.etl.lib import extract_args
from pygyver.etl.dw import BigQueryExecutor
from pygyver.etl.toolkit import read_yaml_file
from pygyver.etl.lib import add_dataset_id_prefix
from pygyver.etl.lib import bq_default_project


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


def extract_unit_test_value(unit_test_list):     
    utl = copy.deepcopy(unit_test_list)
    for d in utl:           
        file = d.pop('file')
        d["sql"] = read_sql(file=file, **d)
        d["cte"] = read_sql(file=d['mock_file'], **d)      
        d["file"] = file     
    return utl


def extract_unit_tests(batch_list=None, kwargs={}):
    """ return the list of unit test: unit test -> file, mock_file, output_table_name(opt) """

    # initiate args and argsmock
    args, args_mock = [] , []

    # extracts files paths for unit tests 
    for batch in batch_list:
        apply_kwargs(batch, kwargs)       
        for table in batch.get('tables', ''):
            if (table.get('create_table', '') != '' or table.get('create_partition_table', '') != '') and table.get('mock_data', '') != '':           
                args.append(table.get('create_table', ''))
                args.append(table.get('create_partition_table', ''))
                args_mock.append(table.get('mock_data', ''))
    
    return_list = []
    for a, b in zip(args, args_mock):
        a.update(b)                        
    return args
    

class PipelineExecutor:
    def __init__(self, yaml_file, dry_run=False, *args, **kwargs):
        self.kwargs = kwargs
        self.yaml = read_yaml_file(yaml_file)
        self.dry_run_dataset_prefix = None
        if dry_run:
            self.dry_run_dataset_prefix = random.sample(range(1,1000000000),1)[0]
            add_dataset_id_prefix(obj=self.yaml, prefix=self.dry_run_dataset_prefix, kwargs=self.kwargs)
        self.bq = BigQueryExecutor()
        # self.unit_test_list = extract_unit_tests()
        self.prod_project_id = 'copper-actor-127213'
    
    def dry_run_clean(self):
        if self.dry_run_dataset_prefix is not None:
            if bq_default_project() != self.prod_project_id:
                for table in self.yaml.get('table_list', ''):
                    dict_ = {
                        "dataset_id": table.split(".")[0]
                    }
                    apply_kwargs(dict_, self.kwargs)
                    dataset_id = dict_['dataset_id']
                    if self.bq.dataset_exists(dataset_id= str(self.dry_run_dataset_prefix) + "_" + dataset_id):
                        self.bq.delete_dataset(dataset_id=str(self.dry_run_dataset_prefix) + "_" + dataset_id, delete_contents=True)

    def create_tables(self, batch):
        args = [] # initiate args
        batch_content = batch.get('tables', '')
        args = extract_args(content=batch_content, to_extract='create_table', kwargs=self.kwargs)
        for a in args:
            apply_kwargs(a, self.kwargs)
            a.update({"dry_run_dataset_prefix": self.dry_run_dataset_prefix})
        if args != []:
            result = execute_parallel(
                        self.bq.create_table,
                        args,
                        message='Creating table:',
                        log='table_id'
                        )
            return result

    def create_partition_tables(self, batch):
        args = [] # initiate args
        batch_content = batch.get('tables', '')
        args = extract_args(content=batch_content, to_extract='create_partition_table', kwargs=self.kwargs)
        for a in args:
            apply_kwargs(a, self.kwargs)
            a.update({"dry_run_dataset_prefix": self.dry_run_dataset_prefix})
        if args != []:            
            result = execute_parallel(
                        self.bq.create_partition_table,
                        args,
                        message='Creating partition table:',
                        log='table_id'
                        )
            return result
 
    def load_google_sheets(self, batch):
        args = [] # initiate args
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
        args, args_pk = [] , [] # initiate args
        batch_content = batch.get('tables', '')
        args = extract_args(batch_content, 'create_table') # adding create_table args to args
        for a in args: # adding dry_run_dataset_prefix to args
            a.update({"dry_run_dataset_prefix": self.dry_run_dataset_prefix})
        args_pk = extract_args(batch_content, 'pk')
        for a, b in zip(args, args_pk): # adding pk args to args
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
        # *** check create_partition_tables ***
        if not (batch.get('tables', '') == '' or extract_args(batch.get('tables', ''),  'create_partition_table') == ''): 
            self.create_partition_tables(batch)
        # *** exec pk check ***
        if not (batch.get('tables', '') == '' or extract_args(batch.get('tables', ''),  'create_table') == '' or extract_args(batch.get('tables', ''),  'pk') == ''):  
            self.run_checks(batch)

    def run(self):
        # run batches
        batch_list = self.yaml.get('batches', '')
        for batch in batch_list:
            apply_kwargs(batch, self.kwargs)
            self.run_batch(batch)
        # run release (ToDo)

    def run_unit_tests(self, batch_list=None):
        batch_list = batch_list or self.yaml.get('batches', '')
        # extract unit tests
        list_unit_test = extract_unit_tests(batch_list, self.kwargs)
        args = extract_unit_test_value(list_unit_test)
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
            _dict = {
                "source_project_id" : self.prod_project_id,
                "source_dataset_id" : table.split(".")[0], 
                "source_table_id": table.split(".")[1],
                "dest_dataset_id" : str(self.dry_run_dataset_prefix) + "_" + table.split(".")[0], 
                "dest_table_id": table.split(".")[1]
                }
            apply_kwargs(_dict, self.kwargs)    
            args.append(_dict)

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
