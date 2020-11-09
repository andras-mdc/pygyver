"""Facebook API"""
import os
import json
import logging
import pandas as pd
import datetime
import time
import numpy as np
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.serverside.event import Event
from facebook_business.adobjects.serverside.event_request import EventRequest
from facebook_business.adobjects.serverside.user_data import UserData
from facebook_business.adobjects.serverside.custom_data import CustomData
from facebook_business.exceptions import FacebookRequestError


def transform_campaign_budget(campaigns):
    """
    Transforms get_campaigns response.
    """
    out = []
    for campaign in campaigns:
        campaign_dict = dict(campaign)
        if "lifetime_budget" in campaign_dict:
            campaign_dict["budget"] = campaign_dict["lifetime_budget"]
            campaign_dict["budget_type"] = "lifetime_budget"
            del campaign_dict["lifetime_budget"]
        elif "daily_budget" in campaign_dict:
            campaign_dict["budget"] = campaign_dict["daily_budget"]
            campaign_dict['budget_type'] = "daily_budget"
            del campaign_dict["daily_budget"]
        out.append(campaign_dict)

    data = pd.DataFrame(out).rename(
        columns={
            "id": "campaign_id",
            "name": "campaign_name"
        }
    )
    return data


def build_predicted_revenue_events(df):
    """
    Creates a list of Facebook Event objects which can be pushed to the Facebook Conversions API.
    Also creates DataFrame for logging which can be used to stream insert to a BigQuery log table.

    :param df: A DataFrame with the events to build Facebook events for
    :type df: pd.DataFrame

    Returns: A tuple with a list of Facebook events and a DataFrame for logs
    rtype: (list of Event, pd.DataFrame)
    """
    events = []
    logs = []
    for index, row in df.iterrows():
        date_now = int(time.time())
        user_data = UserData(
            country_code=row['shop'],
            fbp=row['facebook_browser_id']
        )

        custom_data = CustomData(
            currency=row['currency'],
            value=row['predicted_revenue']
        )

        event = Event(
            event_name='Predicted revenue',
            event_time=date_now,
            user_data=user_data,
            custom_data=custom_data,
            data_processing_options=[]
        )
        events.append(event)

        logs.append(
            {
                "facebook_browser_id": row['facebook_browser_id'],
                "shop": row['shop'],
                "date_source": row['date'],
                "date_processed": datetime.datetime.fromtimestamp(date_now),
                "predicted_revenue": row['predicted_revenue'],
                "currency": row['currency']
            }
        )
    df_logs = pd.DataFrame(logs)
    return events, df_logs


def calculate_batches(total_events, batch_size):
    """
    Calculate number of batches to split events into given a batch size and taking into account evenly divisible batches
    :param total_events: The number of total events
    :type total_events: int
    :param batch_size: The max number of events in a batch
    :type batch_size: int

    Returns: The resulting number of batches
    rtype: int

    """
    batches = None
    try:
        batches = (total_events // batch_size)
        batches = batches + 1 if total_events % batch_size != 0 else batches
    except ZeroDivisionError:
        logging.error('Batch Size cannot be 0')
        raise
    except TypeError:
        logging.error('Total Events and Batch Size must be integers')
        raise
    return batches


def split_events_to_batches(events, batch_size=1000):
    """
    Divides a DataFrame of events into batches of the specified batch size (defaults to 1000).

    :param events: A DataFrame of Facebook Events to push to the conversions API
    :type events: pd.DataFrame
    :param batch_size: The max number of events in a batch
    :type batch_size: int

    Returns: A list of DataFrames
    rtype: list of pd.DataFrame

    """
    batched_df = []
    try:
        total_events = len(events.index)
    except TypeError:
        logging.warning("Events is null")

    if total_events > 0:
        logging.info('Batch limit set to %s', batch_size)
        if total_events > batch_size:
            batches = calculate_batches(total_events, batch_size)
            batched_df = np.array_split(events, batches)
            logging.info('Total %s events split into %s batches', total_events, batches)
        else:
            batched_df = [events]
            logging.info('Total %s events only requires 1 batch', total_events)
    else:
        logging.warning('No events split into batches')
    return batched_df


class FacebookExecutor:
    """ Facebook FacebookExecutor.
    Arguments:
        """
    def __init__(self):
        self.client = None
        self.access_token = None
        self.account = None
        self.account_id = None
        self.pixel_id = None
        self.set_api_config()

    def set_api_config(self):
        """
        Loads access_token from FACEBOOK_APPLICATION_CREDENTIALS.
        """
        try:
            with open(os.environ["FACEBOOK_APPLICATION_CREDENTIALS"]) as facebook_cred:
                data = json.load(facebook_cred)
                self.access_token = data["access_token"]
        except KeyError:
            raise KeyError("FACEBOOK_APPLICATION_CREDENTIALS env variable needed")
        self.set_client()

    def set_client(self):
        """
        Sets self.client using the access token.
        """
        self.client = FacebookAdsApi.init(access_token=self.access_token)

    def set_account(self, account_id):
        """ Sets account object
        """
        self.account_id = account_id
        self.account = AdAccount('act_{}'.format(self.account_id))
        logging.info("Initiated AdAccount object for account %s", self.account_id)

    def set_pixel_id(self, pixel_id):
        """ Sets the Pixel ID
        """
        self.pixel_id = pixel_id
        logging.info("Set the pixel_id as %s", self.pixel_id)

    def get_campaign_insights(self, account_id, fields, start_date, end_date):
        """
        Sets insights from the Facebook Insight API.
        Parameters:
            account_id: ID associated to the Facebook Account
            start_date: The start date
            end_date: The end date
            fields: list of field to be fetched
            start_date/end_date: defines the time range to get insights for (YYYY-mm-dd).
        """
        self.set_account(account_id)
        out = []
        params = {
            'effective_status': ['ACTIVE'],
            'level': 'campaign',
            'time_range': {
                'since': start_date,
                'until': end_date
                }
        }
        logging.debug("Downloading insights for account %s", self.account_id)
        logging.debug("fields: %s", fields)
        logging.debug("params: %s", params)
        campaign_insights = self.account.get_insights(
            params=params,
            fields=fields
        )

        for insight in campaign_insights:
            out.append(dict(insight))
        return out

    def get_active_campaigns(self):
        return self.account.get_campaigns(
            fields=['account_id', 'name', 'daily_budget', 'lifetime_budget'],
            params={
                'effective_status': ["ACTIVE"],
                'is_completed': False
            }
        )

    def get_active_campaign_budgets(self, account_id):
        """
        Fetches active campaign metadata from the Facebook API.
        Returns a dataframe with the following fields:
            - account_id
            - campaign_id
            - campaign_name
            - budget_type (daily_budget or lifetime_budget)
            - budget amount in account currency
        """
        self.set_account(account_id)
        campaigns = self.get_active_campaigns()
        out = transform_campaign_budget(campaigns)
        return out

    def update_daily_budget(self, account_id, campaign_id, new_budget):
        """
        Update the budget on the facebook API
        """
        self.set_account(account_id)
        campaigns = self.get_active_campaigns()
        for campaign in campaigns:
            if campaign.get_id() == campaign_id:
                from pygyver.etl.toolkit import configure_logging
                configure_logging()
                logging.info(
                    "Loading new budget for campaign %s",
                    campaign_id
                )
                logging.info(
                    "Current daily_budget for campaign %s: %s",
                    campaign_id,
                    campaign['daily_budget']
                )
                campaign.api_update(
                    params={'daily_budget': round(new_budget*100)}
                )
                logging.info(
                    "New daily_budget for campaign %s: %s",
                    campaign_id,
                    new_budget
                )

        return campaigns

    def push_conversions_api_events(self, events, test_event_code=None):
        """
        Pushes a list of Events to the Facebook Conversions API.

        :param events: A list of Facebook Events to push to the conversions API
        :type events: list of Event
        :param test_event_code: A test_event_code from Facebook Events Manager to mark these as test events
        :type test_event_code: str

        Returns: A dictionary with the parsed response from the Facebook API
        rtype: dict[str, str]
        """
        if len(events) > 1000:
            logging.error("The maximum number of events that Facebook accepts in a single API call is 1,000. "
                          "Please use the split_events_to_batches() function to split the events into batches")
            raise ValueError

        event_request = EventRequest(
            events=events,
            pixel_id=self.pixel_id,
        )

        # Add the test_event_code if one is given
        if test_event_code:
            event_request.test_event_code = test_event_code

        api_response = {}

        try:
            event_response = event_request.execute()
            logging.info('%s events pushed to Facebook Conversions API', event_response.events_received)
            api_response['status'] = 'API Success'
            api_response['fb_trace_id'] = event_response.fbtrace_id
            api_response['messages'] = '\n'.join(event_response.messages)
            api_response['total_events'] = event_response.events_received

        except FacebookRequestError as e:
            logging.error('There was a Facebook Conversions API error:\n\t%s', e)
            api_response['status'] = 'API Error'
            api_response['fb_trace_id'] = e.body()['error']['fbtrace_id']
            error_message = e.body()['error']['message']
            error_message = ': '.join([error_message, e.body()['error']['error_user_msg']])
            api_response['messages'] = error_message
            api_response['total_events'] = None

        return api_response
