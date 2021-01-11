# -*- coding: utf-8 -*-
""" Zendesk Tests """

import os
import json
import unittest
import sys

from unittest import mock
from pygyver.etl.zendesk import ZendeskDownloader


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code, headers=None):
            self.content = content
            self.status_code = status_code
            self.headers = headers
    print(kwargs['url'])
    if kwargs['url'] == 'https://madecom.zendesk.com/api/v2/ticket_fields.json':
        return MockResponse('{"ticket_fields": [{"id": 34}]}', 200)
    elif kwargs['url'] == 'https://madecom.zendesk.com/api/v2/tickets.json&start_time=1514851200':
        return MockResponse('{"tickets": [{"id": 34}], "next_page": "https://madecom.zendesk.com/api/v2/tickets/second_page.json"}', 200)
    elif kwargs['url'] == 'https://madecom.zendesk.com/api/v2/ticket_fields.json?include=comment_events':
        return MockResponse('{"tickets": [{"id": 22}], "next_page": "null"}', 200)
    elif kwargs['url'] == 'https://madecom.zendesk.com/api/v2/tickets/second_page.json':
        return MockResponse('{"tickets": [{"id": 31}], "next_page": null}', 200)
    elif kwargs['url'] == 'https://madecom.zendesk.com/api/v2/ticket_events.json&start_time=1514851200':
        return MockResponse(None, 429, {"retry-after": 1000})
    elif kwargs['url'] == 'https://madecom.zendesk.com/api/v2/custom_fields.json':
        return MockResponse(None, 999)
    return MockResponse(None, 404)


def mocked_set_auth_token():
    pass


def mocked_set_auth():
    pass


def mocked_time_sleep(seconds):
    sys.exit('sleep has been initiated')


@ mock.patch('pygyver.etl.zendesk.ZendeskDownloader.set_auth_token', side_effect=mocked_set_auth_token)
@ mock.patch('pygyver.etl.zendesk.ZendeskDownloader.set_auth', side_effect=mocked_set_auth)
class ZendeskDownloaderTestInput(unittest.TestCase):
    """ Testing ZendeskDownload Input and Error Handling """

    def test_date_format_error(self, mocked_set_auth_token, mocked_set_auth):
        """ Expects an error as the date format is incorrect"""
        self.assertRaisesRegex(ValueError,
                               "Incorrect date format.*",
                               ZendeskDownloader, uri='tickets', start_date='20182201')


@ mock.patch('pygyver.etl.zendesk.requests.get', side_effect=mocked_requests_get)
@ mock.patch('pygyver.etl.zendesk.ZendeskDownloader.set_auth_token', side_effect=mocked_set_auth_token)
@ mock.patch('pygyver.etl.zendesk.ZendeskDownloader.set_auth', side_effect=mocked_set_auth)
class ZendeskDownloaderTestScenario(unittest.TestCase):

    def test_null_url_value_error(self, mocked_set_auth_token, mocked_set_auth, mocked_requests_get):
        """ Tests that the request fails and raises a value error when uri is not defined"""
        with self.assertRaises(KeyError):
            self.zendesk = ZendeskDownloader(start_date='2018-01-02 00:00:00')

    def test_api_runs_increment(self, mocked_set_auth_token, mocked_set_auth, mocked_requests_get):
        """ Tests that the request is being called with an increment start_date"""
        zendesk = ZendeskDownloader(
            uri="api/v2/ticket_fields"
        )
        zendesk.auth = ('user', 'token')
        data = zendesk.download()
        self.assertEqual(data, {"ticket_fields": [{"id": 34}]})

    def test_api_runs_sideload(self, mocked_set_auth_token, mocked_set_auth, mocked_requests_get):
        """ Tests that the request is being called with an sideload"""
        zendesk = ZendeskDownloader(
            uri="api/v2/ticket_fields",
            sideload="?include=comment_events"
        )
        zendesk.auth = ('user', 'token')
        data = zendesk.download()
        self.assertEqual(data, {"tickets": [{"id": 22}], "next_page": "null"})

    def test_api_continues_to_next_page(self, mocked_set_auth_token, mocked_set_auth, mocked_requests_get):
        """ Tests that the request is being called and the next page is recorded and called on the next download"""
        zendesk = ZendeskDownloader(
            uri="api/v2/tickets",
            start_date="2018-01-02 00:00:00"
        )
        zendesk.auth = ('user', 'token')
        data = zendesk.download()
        data = zendesk.download()
        self.assertEqual(data, {"tickets": [{"id": 31}], "next_page": None})

    @mock.patch('pygyver.etl.zendesk.sleep', side_effect=mocked_time_sleep)
    def test_api_sleeps_on_timeout(self, mocked_set_auth_token, mocked_set_auth, mocked_requests_get, mocked_time_sleep):
        """ Tests the request rate limit reached sleep"""
        with self.assertRaises(SystemExit) as cm:
            zendesk = ZendeskDownloader(uri="api/v2/ticket_events",
                                        start_date="2018-01-02 00:00:00")
            zendesk.auth = ('user', 'token')
            data = zendesk.download()
            self.assertEqual(cm.exception, "sleep has been initiated")

    def test_api_max_retries(self, mocked_set_auth_token, mocked_set_auth, mocked_requests_get):
        """ Tests the request max retries exception"""
        with self.assertRaises(ValueError) as cm:
            zendesk = ZendeskDownloader(uri="api/v2/custom_fields")
            zendesk.auth = ('user', 'token')
            data = zendesk.download()
            self.assertEqual(cm.exception, "HTTP Error 999.")
