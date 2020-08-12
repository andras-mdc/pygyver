import os
import sys
import unittest
from unittest import mock
from pygyver.etl.gooddata import execute_schedule

# This method will be used by the mock to replace requests.post
def mocked_requests_post(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code
    
    if kwargs['url'] == 'gooddata_domain/gdc/projects/gooddata_project/schedules/schedule_0/executions':
        return MockResponse('', 400)
    elif kwargs['url'] == 'gooddata_domain/gdc/projects/gooddata_project/schedules/schedule_1/executions':
        return MockResponse('{"execution":{"links":{"self":"/schedule_1"}}}', 200)
    elif kwargs['url'] == 'gooddata_domain/gdc/projects/gooddata_project/schedules/schedule_2/executions':
        return MockResponse('{"execution":{"links":{"self":"/schedule_2"}}}', 200)
    elif kwargs['url'] == 'gooddata_domain/gdc/projects/gooddata_project/schedules/schedule_3/executions':
        return MockResponse('{"execution":{"links":{"self":"/schedule_3"}}}', 200)
    return MockResponse(None, 404)

# This method will be used by the mock to replace requests.get
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code
    
    if kwargs['url'] == 'gooddata_domain/schedule_1':
        return MockResponse('{"execution":{"status":"SCHEDULED"}}', 100)
    elif kwargs['url'] == 'gooddata_domain/schedule_2':
        return MockResponse('{"execution":{"status":"OK"}}', 100)
    elif kwargs['url'] == 'gooddata_domain/schedule_3':
        return MockResponse('{"execution":{"status":"FAILURE"}}', 100)
    return MockResponse(None, 404)

def mocked_time_sleep(seconds):
    sys.exit('sleep has been initiated')

@mock.patch.dict(os.environ, { 'GOODDATA_DOMAIN': 'gooddata_domain','GOODDATA_PROJECT': 'gooddata_project'})
@mock.patch('pygyver.etl.gooddata.auth_cookie', return_value='mysupersecretauthcookie')
@mock.patch('requests.post', side_effect=mocked_requests_post)
@mock.patch('requests.get', side_effect=mocked_requests_get)
class GoodDataFunctions(unittest.TestCase):
    
    def test_schedule_bad_response(self, mock_get, mock_post, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for bad response capture
        """
        self.assertRaises(ValueError, execute_schedule, 'schedule_0')

    @mock.patch('pygyver.etl.gooddata.time.sleep', side_effect=mocked_time_sleep)
    def test_schedule_sleep(self, mock_time_sleep, mock_get, mock_post, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for SCHEDULED status sleep
        """
        with self.assertRaises(SystemExit) as cm:
            execute_schedule('schedule_1')
            self.assertEqual(cm.exception, "sleep has been initiated")
   
    def test_schedule_complete(self, mock_get, mock_post, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for completed schedule
        """
        self.assertEqual(execute_schedule('schedule_2'), 'OK')

    def test_schedule_failure(self, mock_get, mock_post, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule and tests for failed schedule
        """
        self.assertRaises(ValueError, execute_schedule, 'schedule_3')
