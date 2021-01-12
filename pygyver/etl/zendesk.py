#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon June 17 16:27:12 2019

@author: kzaoui
"""

import os
import time
import re
import json
from time import sleep
import logging
import requests


class ZendeskDownloader:
    """ Zendesk Downloader.
    Arguments:
    - start_date (string): Optional. Timestamp format.
    - uri (string): Used to specify the endpoint.
    - sideload (string): Optional. Used to specify any additional parameters."""

    def __init__(self, start_date=None, uri=None, sideload=""):
        self.user = "analytics@made.com/token"
        self.base_url = "https://madecom.zendesk.com/"
        self.data = "{'Accept': 'application/json'}"
        self.start_date = start_date
        self.sideload = sideload
        self.uri = uri
        self.endpoint = None
        self.access_token = None
        self.page_number = 0
        self.next_page = None
        self.retry = 0
        self.max_retry = 3
        self.set_uri()
        self.set_start_date()
        self.set_auth_token()
        self.set_auth()
        self.set_increment_query()

    def set_start_date(self):
        """ Raises a ValueError start_date do not follow the format "%Y-%m-%d %H:%M:%S"
        """
        pattern = "%Y-%m-%d %H:%M:%S"
        if self.start_date is not None:
            try:
                logging.info("Start date: %s", self.start_date)
                self.start_date = int(time.mktime(
                    time.strptime(self.start_date, pattern)))
            except ValueError:
                raise ValueError("""Incorrect date format, must be '%Y-%m-%d %H:%M:%S',
                eg. '2018-12-31 22:45:32'""")

    def set_auth_token(self):
        """ Sets the auth needed to make the api call"""
        try:
            with open(os.environ["ZENDESK_ACCESS_TOKEN_PATH"]) as zendesk_file:
                data = json.load(zendesk_file)
                self.access_token = data["access_token"]
        except KeyError:
            raise KeyError("ZENDESK_ACCESS_TOKEN_PATH env variable needed")

    def set_auth(self):
        """ Sets auth param """
        self.auth = (self.user, self.access_token)

    def set_uri(self):
        """ Sets uri param """
        if self.uri is None:
            raise KeyError("URI param required")
        else:
            self.uri = "{}.json".format(self.uri)

    def set_increment_query(self):
        """ Sets increment query params """
        if self.start_date is None:
            self.increment_query = ""
        else:
            self.increment_query = """&start_time={}""".format(self.start_date)

    def set_endpoint(self):
        """ Sets the end point using the base_url, path, increment query based on page_number """
        self.page_number = self.page_number + 1
        self.retry = 1
        if self.page_number == 1:
            self.endpoint = self.base_url + self.uri + self.sideload + self.increment_query
        else:
            self.endpoint = self.data["next_page"]

    def set_next_page(self):
        page = self.data.get("next_page")
        if page:
            self.next_page = self.data['next_page']

    def keep_running(self):
        """ Keep fetching if response include 1000 responses or more """
        if self.next_page is None and self.page_number > 0:
            return False
        else:
            return True

    def api_call(self):
        """ Make the API request. Includes retry if reached minute limit"""
        logging.info("Requesting {}, page {}".format(self.uri, self.page_number))
        response = requests.get(url=self.endpoint, auth=self.auth)
        if response.status_code == 200:
            self.data = json.loads(response.content)
            self.set_next_page()
        elif response.status_code == 429:  # timed-out
            sleep_time = int(response.headers['retry-after'])
            logging.warning(
                'Too many requests 429. Waiting %s secs to retry…', sleep_time)
            sleep(sleep_time)
            self.api_call()
        elif response.status_code == 400:  # bad-request
            logging.info("Bad Request 400")
            self.data = []
        elif response.status_code == 403: # forbidden
            logging.info("Request forbidden 403")
        else:
            while self.retry < self.max_retry:
                logging.info("Error {}. Attempt {}/{} failed, retrying after 1 second...".format(response.status_code, self.retry, self.max_retry))
                sleep(1)
                self.retry = self.retry + 1
                self.api_call()
            logging.info("Reached max_retry, raising error")
            self.data = []
            raise ValueError("HTTP Error {}".format(response.status_code))

    def download(self):
        """ Download responses using correct endpoint depending on the page requested """
        if self.keep_running():
            self.set_endpoint()
            self.api_call()
            responses = self.data
        else:
            logging.info("No further call to be made. Closing loop.")
            responses = []
        return responses
