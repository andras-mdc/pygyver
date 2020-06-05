""" Module to ETL data to generate pipelines """
from __future__ import print_function

import copy
import time
import random
import logging
import threading
import numpy as np
import concurrent.futures
from pygyver.etl.dw import read_sql 
from pygyver.etl.lib import apply_kwargs
from pygyver.etl.lib import extract_args
from pygyver.etl.dw import BigQueryExecutor
from pygyver.etl.toolkit import read_yaml_file
from pygyver.etl.lib import bq_default_project
from pygyver.etl.lib import add_dataset_id_prefix



def execute_parallel(func, args, message='running task', log=''):
    """
    execute the functions in parallel for each list of parameters passed in args

    Arguments:
    func: function as an object
    args: list of function's args

    """
    tasks = []
    count = []

    # logging.basicConfig(format=format, level=logging.INFO,
    #                      datefmt="%H:%M:%S")
   
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_func = {executor.submit(func, **arg): arg for arg in args}
        for future in concurrent.futures.as_completed(future_to_func):
            arg = future_to_func[future]
            try:
                ret = future.result()
            except AssertionError as ass_exc:
                logging.info(f"{message} {arg.get(log,'')}: failed")
                raise AssertionError
            except Exception as exc:
                logging.info('%r generated an exception: %s' % (arg, exc))
                raise Exception            
            else:
                logging.info(f"{message} {arg.get(log,'')}") 

def extract_unit_test_value(unit_test_list):     
    utl = copy.deepcopy(unit_test_list)
    for d in utl:      
        file = d.pop('file')
        d["sql"] = read_sql(file=file, **d)
        if 'mock_partition_date' in d:
            d["sql"] = d["sql"].format(
                partition_date=d['mock_partition_date']
            )
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
                if table.get('create_table', '') != '':    
                    args.append(table.get('create_table', ''))
                if table.get('create_partition_table', '') != '':
                    args.append(table.get('create_partition_table', ''))
                args_mock.append(table.get('mock_data', ''))
    
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
    
    def remove_dataset(self, dataset_id):
        if self.bq.dataset_exists(dataset_id):
            self.bq.delete_dataset(dataset_id, delete_contents=True)

    def dry_run_clean(self, table_list=''):
        if self.dry_run_dataset_prefix is not None:
            if bq_default_project() != self.prod_project_id:        
                args_dataset = []

                if table_list == '':
                    table_list = self.yaml.get('table_list', '')

                for table in table_list:
                    dict_ = {
                        "dataset_id": table.split(".")[0]
                    }                
                    apply_kwargs(dict_, self.kwargs)
                    args_dataset.append(
                        dict_                                   
                    )                

                for dataset in args_dataset:                    
                    value = dataset.get('dataset_id', '')
                    dataset['dataset_id'] = str(self.dry_run_dataset_prefix) + "_" + value
                
                args_dataset = [dict(t) for t in {tuple(d.items()) for d in args_dataset}]

                # drop datasets 
                if args_dataset != []:            
                    execute_parallel(
                                self.remove_dataset,
                                args_dataset,
                                message='delete dataset: ',  
                                log='dataset_id'                      
                                )                        

    def create_tables(self, batch):
        args = [] # initiate args
        batch_content = batch.get('tables', '')
        args = extract_args(content=batch_content, to_extract='create_table', kwargs=self.kwargs)
        for a in args:
            apply_kwargs(a, self.kwargs)
            a.update({"dry_run_dataset_prefix": self.dry_run_dataset_prefix})
        if args != []:            
            execute_parallel(
                        self.bq.create_table,
                        args,
                        message='Creating table:',
                        log='table_id'
                        )

    def create_partition_tables(self, batch):
        args = [] # initiate args
        batch_content = batch.get('tables', '')
        args = extract_args(content=batch_content, to_extract='create_partition_table', kwargs=self.kwargs)
        for a in args:
            apply_kwargs(a, self.kwargs)
            a.update({"dry_run_dataset_prefix": self.dry_run_dataset_prefix})
        if args != []:            
            execute_parallel(
                        self.bq.create_partition_table,
                        args,
                        message='Creating partition table:',
                        log='table_id'
                        )
 
    def load_google_sheets(self, batch):
        args = [] # initiate args
        batch_content = batch.get('sheets', '')
        args = extract_args(batch_content, 'load_google_sheet')
        if args == []:
            raise Exception("load_google_sheet in yaml is not well defined")
        execute_parallel(
                    self.bq.load_google_sheet,
                    args,
                    message='Loading table:',
                    log='table_id'
                    )

    def run_checks(self, batch):
        args, args_pk = [] , [] # initiate args
        batch_content = batch.get('tables', '')
        args = extract_args(batch_content, 'create_table') # adding create_table args to args
        for a in args: # adding dry_run_dataset_prefix to args
            a.update({"dry_run_dataset_prefix": self.dry_run_dataset_prefix})
        args_pk = extract_args(batch_content, 'pk')
        for a, b in zip(args, args_pk): # adding pk args to args
            a.update({"primary_key": b})
        execute_parallel(
                    self.bq.assert_unique,
                    args,
                    message='Run pk_check on:',
                    log='table_id'
                    )        

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
            execute_parallel(
                        self.bq.assert_acceptance,
                        args,
                        message='Asserting sql',  
                        log='file'                      
                        )

    def copy_prod_structure(self, table_list=''):

        args, args_dataset = [], []

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

        for dataset in np.unique( [ str(self.dry_run_dataset_prefix) + "_" + table.split(".")[0] for table in table_list ] ): 
            _dict = {"dataset_id" : dataset}
            apply_kwargs(_dict, self.kwargs)                                    
            args_dataset.append(
                _dict                                    
            )     

        # create datasets for dry run
        if args_dataset != []:            
            execute_parallel(
                        self.bq.create_dataset,
                        args_dataset,
                        message='create dataset for: ',  
                        log='dataset_id'                      
                        )

        # copy tables structure
        if args != []:            
            execute_parallel(
                        self.bq.copy_table_structure,
                        args,
                        message='copy table structure for: ',  
                        log='source_table_id'                      
                        )


    def run_test(self):
        # unit test
        self.run_unit_tests()
        # copy table schema from prod
        # dry run
