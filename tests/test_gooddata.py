import os
import sys
import unittest
from unittest import mock
from pygyver.etl.gooddata import execute_schedule

# This method will be used by the mock to replace api_get_schedules with 0 returned objects
def mocked_api_get_schedules_0_schedules(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code
    
    return MockResponse('{"schedules":{"items":[]}}', 100)

# This method will be used by the mock to replace api_get_schedules with 1 returned objects
def mocked_api_get_schedules_1_schedules(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code
    
    return MockResponse('{"schedules":{"items":[{"schedule":{"params":{"GDC_DATALOAD_DATASETS":"data.dan"}}}]}}', 100)

# This method will be used by the mock to replace api_post_execution
def mocked_api_post_execution(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code
    
    if kwargs['schedule_id'] == 'schedule_0':
        return MockResponse('', 400)
    elif kwargs['schedule_id'] == 'schedule_1':
        return MockResponse('{"execution":{"links":{"self":"/schedule_1"}}}', 200)
    elif kwargs['schedule_id'] == 'schedule_2':
        return MockResponse('{"execution":{"links":{"self":"/schedule_2"}}}', 200)
    elif kwargs['schedule_id'] == 'schedule_3':
        return MockResponse('{"execution":{"links":{"self":"/schedule_3"}}}', 200)
    return MockResponse(None, 404)

# This method will be used by the mock to replace api_get_status
def mocked_api_get_status(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code
    
    if kwargs['url'] == '/schedule_1':
        return MockResponse('{"execution":{"status":"SCHEDULED"}}', 100)
    elif kwargs['url'] == '/schedule_2':
        return MockResponse('{"execution":{"status":"OK"}}', 100)
    elif kwargs['url'] == '/schedule_3':
        return MockResponse('{"execution":{"status":"FAILURE"}}', 100)
    return MockResponse(None, 404)

def mocked_time_sleep(seconds):
    sys.exit('sleep has been initiated')

@mock.patch.dict(os.environ, { 'GOODDATA_DOMAIN': 'gooddata_domain','GOODDATA_PROJECT': 'gooddata_project'})
@mock.patch('pygyver.etl.gooddata.auth_cookie', return_value='mysupersecretauthcookie')
@mock.patch('pygyver.etl.gooddata.api_post_execution', side_effect=mocked_api_post_execution)
@mock.patch('pygyver.etl.gooddata.api_get_status', side_effect=mocked_api_get_status)
class GoodDataFunctions(unittest.TestCase):
    
    @mock.patch('pygyver.etl.gooddata.api_get_schedules', side_effect=mocked_api_get_schedules_0_schedules)
    def test_schedule_bad_response(self, mocked_api_get_schedules_0_schedules, mocked_api_post_execution, mocked_api_get_status, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for bad response capture
        """
        self.assertRaises(ValueError, execute_schedule, 'schedule_0')

    @mock.patch('pygyver.etl.gooddata.time.sleep', side_effect=mocked_time_sleep)
    @mock.patch('pygyver.etl.gooddata.api_get_schedules', side_effect=mocked_api_get_schedules_0_schedules)
    def test_schedule_sleep(self, mock_interval_sleep, mocked_api_get_schedules_0_schedules, mocked_api_post_execution, mocked_api_get_status, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for SCHEDULED status sleep
        """
        with self.assertRaises(SystemExit) as cm:
            execute_schedule('schedule_1')
            self.assertEqual(cm.exception, "sleep has been initiated")

    @mock.patch('pygyver.etl.gooddata.api_get_schedules', side_effect=mocked_api_get_schedules_0_schedules)
    def test_schedule_complete(self, mocked_api_get_schedules_0_schedules, mocked_api_post_execution, mocked_api_get_status, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for completed schedule
        """
        self.assertEqual(execute_schedule('schedule_2'), 'OK')
    
    @mock.patch('pygyver.etl.gooddata.api_get_schedules', side_effect=mocked_api_get_schedules_0_schedules)    
    def test_schedule_failure(self, mocked_api_get_schedules_0_schedules, mocked_api_post_execution, mocked_api_get_status, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for failed schedule
        """
        self.assertRaises(ValueError, execute_schedule, 'schedule_3')
    
    @mock.patch('pygyver.etl.gooddata.time.sleep', side_effect=mocked_time_sleep)
    @mock.patch('pygyver.etl.gooddata.api_get_schedules', side_effect=mocked_api_get_schedules_1_schedules)    
    def test_schedule_wait(self, mock_interval_sleep, mocked_api_get_schedules_1_schedules, mocked_api_post_execution, mocked_api_get_status, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for status sleep when there is an existing schedule running
        """
        with self.assertRaises(SystemExit) as cm:
            execute_schedule('schedule_4')
            self.assertEqual(cm.exception, "sleep has been initiated")

