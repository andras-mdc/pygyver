import os
import unittest
from unittest import mock
from pygyver.etl.gooddata import execute_schedule

# This method will be used by the mock to replace requests.post
def mocked_requests_post(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data
    
    if kwargs['url'] == 'my_gooddata_domain/gdc/projects/bad_request/schedules/my_schedule_id/executions':
        print("We have a bad request!")
        return MockResponse(None, 400)
    elif kwargs['url'] == 'http://someotherurl.com/anothertest.json':
        return MockResponse({"key2": "value2"}, 200)

    return MockResponse(None, 404)


class GoodDataFunctions(unittest.TestCase):
    """ Testing Execution of the Main """
    def setUp(self):
        """
        Sets DB.
        """
        print("setUp")

    @mock.patch.dict(os.environ, {
        'GOODDATA_DOMAIN': 'my_gooddata_domain',
        'GOODDATA_PROJECT': 'bad_request'
    })
    @mock.patch('pygyver.etl.gooddata.auth_cookie')
    @mock.patch('requests.post', side_effect=mocked_requests_post)
    def test_execute_schedule_bad_request(self, mock_post, mock_auth_cookie):
        """
        Executes gooddata.py function execute_schedule
        """
        mock_auth_cookie.return_value = "mysupersecretauthcookie"
        execute_schedule(schedule_id='my_schedule_id')
        

    def tearDown(self):
        print("TearDown")
