#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on: Mon June 17 16:27:12 2019
Updated on: Wed Feb 17 21:50:00 2021

@authors: kzaoui, dankruse1
"""

import os
import time
import re
import json
from time import sleep
import logging
import requests


class ZendeskDownloader:
    """
    Class to download data from a Zendesk API endpoint specified by params.
    """

    def __init__(self, start_date=None, uri=None, sideload=None, per_page=500, pagination=True):
        """
        Construct all the necessary attributes for the ZendeskDownloader instance
        
        Args:
          uri (str): API endpoint.
          start_date (str, optional): Timestamp format ('2018-12-31 22:45:32').
          sideload (str, optional): Additional parameters seperated by &.
          per_page (int, optional): Size of page to fetch from the API.
          pagination (bool, optional): Flag for if the endpoint has pagination.
          
        Examples:
          downloader = ZendeskDowloader(
            uri="api/v2/incremental/tickets",
            start_date="2018-12-31 22:45:32",
            sideload="include=comment&include=metric_sets",
            per_page=200,
            pagination=True
          )
        """
        self.user = "analytics@made.com/token"
        self.base_url = "https://madecom.zendesk.com/"
        self.start_date = start_date
        self.sideload = sideload
        self.uri = uri
        self.per_page = per_page
        self.pagination = pagination
        self.endpoint = None
        self.access_token = None
        self.page_number = 0
        self.next_page = None
        self.retry = 0
        self.max_retry = 3
        self.count = None
        self.params_list = []
        self.endpoint_params = ''
        self.set_start_date()
        self.set_auth_token()
        self.set_auth()
        self.set_uri()
        self.set_start_time_param()
        self.set_per_page_param()
        self.set_sideload_param()
        self.set_endpoint_params()
        
    def set_start_date(self):
        """Convert `self.start_date` to epoch timestamp.

        Raises:
          ValueError: If `self.start_date` does not follow the format "%Y-%m-%d %H:%M:%S"
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
        """Load access token file to `self.access_token`
        
        Raises:
          KeyError: if env var ZENDESK_ACCESS_TOKEN_PATH does not exist
        """
        try:
            with open(os.environ["ZENDESK_ACCESS_TOKEN_PATH"]) as zendesk_file:
                data = json.load(zendesk_file)
                self.access_token = data["access_token"]
        except KeyError:
            raise KeyError("ZENDESK_ACCESS_TOKEN_PATH env variable needed")

    def set_auth(self):
        """Set `self.auth` as required to access API based on `self.user` and `self.access_token` values
        """
        self.auth = (self.user, self.access_token)

    def set_uri(self):
        """Format the `self.uri` value for use in the endpoint 
  
        Raises:
          KeyError: if `self.uri` is None
        """
        if self.uri is not None:
          self.uri= "{}.json".format(self.uri)
        else:
          raise KeyError("URI param required")
            
    def set_start_time_param(self):
        """Format and append start_time param to params_list if `self.start_date` is not None
        """
        if self.start_date:
          param ="start_time={}".format(self.start_date)
          self.params_list.append(param)
            
    def set_per_page_param(self):
        """Format and append per_page param to params_list if `self.per_page` is not None
        """
        if self.pagination:
          param = "per_page={}".format(self.per_page)
          self.params_list.append(param)
        
    def set_sideload_param(self):
        """Format and append sideload param to params_list if `self.sideload` is not None
        """
        if self.sideload:
          param = self.sideload
          self.params_list.append(param)
        
    def set_endpoint_params(self):
        """Set `self.endpoint_params`
        
        Iterate through `self.params_list` generating a string of params integrating '?' and '&' where required
        """
        for i, k in enumerate(sorted(self.params_list)):
          if i == 0:
            self.endpoint_params = self.endpoint_params + "?{}".format(k)
          else:
            self.endpoint_params = self.endpoint_params + "&{}".format(k)
  
    def set_endpoint(self):
        """Set `self.endpoint`. 
        
        The first call sets the endpoint based on values from the provided uri and params.
        Subsequent calls set the endpoint based on the returned `next_page` value from the previous call.
        """
        self.page_number = self.page_number + 1
        self.retry = 1
        if self.page_number == 1:
            self.endpoint = self.base_url + self.uri + self.endpoint_params
            logging.info("Endpoint generated - {}".format(self.endpoint))
        else:
            self.endpoint = self.data["next_page"]

    def set_next_page(self):
        """Set `self.next_page` if `self.data` contains a next_page key
        """
        page = self.data.get("next_page")
        if page:
            self.next_page = self.data['next_page']
            
    def set_count(self):
        """Set `self.count` if `self.data` contains a next_page key
        """
        count = self.data.get("count")
        if count:
            self.count = self.data['count']
            logging.info("API response returned {} rows".format(self.count))

    def keep_running(self):
        """Check if the API fetch process is complete
        
          If `self.page_number` is less than 0 then the first request has not yet been sent and so the process should continue.
          If `self.pagination` is False then the endpoint does not have pagination and the process should stop after the first request
          If `self.count` is less than `self.per_page` then the number of returned records is less then the records per page. This signifies that all records have been returned and the process should stop.

        Returns:
          bool: `True` if the process should stop `False` if the process should continue
        """
        if self.page_number > 0 and (self.pagination == False or self.count < self.per_page) :
            return False
        else:
            return True

    def api_call(self):
        """ Make the API request and handle retry based response codes.
        
        After each request update `self.count` with the number of records returned, `self.next_page` with the next_page endpoint provided in the response.
        If the rate limit is reached (429) process sleeps for period defined in the response.

        Raises:
          ValueError: If there is an error response and max retires has been exhausted  
        """
        response = requests.get(url=self.endpoint, auth=self.auth)
        if response.status_code == 200:
            self.data = json.loads(response.content)
            self.set_count()
            self.set_next_page()
        elif response.status_code == 429:  # timed-out
            sleep_time = int(response.headers['retry-after'])
            logging.warning('Too many requests (429). Waiting %s secs to retry…', sleep_time)
            sleep(sleep_time)
            self.api_call()
        else:
            while self.retry < self.max_retry:
                logging.info("Error {}. Attempt {}/{} failed, retrying after 1 second...".format(response.status_code, self.retry, self.max_retry))
                sleep(1)
                self.retry = self.retry + 1
                self.api_call()
            logging.info("Reached max_retry, raising error")
            raise ValueError("HTTP Error {}".format(response.status_code))
            
    def download(self):
        """ Download API responses based on provided URI and params
        
        Returns
          obj: Object of API response data
        """
        if self.keep_running():
            self.set_endpoint()
            self.api_call()
            responses = self.data
        else:
            logging.info("No further call to be made. Closing loop.")
            responses = []
        return responses
