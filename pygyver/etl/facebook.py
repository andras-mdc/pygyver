"""Facebook API"""
import os
import json
import logging
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

class FacebookDownloader:
    """ Facebook Downloader.
    Arguments:
        """
    def __init__(self):
        self.client = None
        self.access_token = None
        self.account = None
        self.account_id = None
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

    def get_campaign_insights(self, account_id, fields, start_date, end_date):
        """
        Sets insights from the Facebook Insight API.
        Parameters:
            account_id: ID associated to the Facebook Account
            fields: list of field to be fetched
            start_date/end_date: defines the timerange to get insights for (YYYY-mm-dd).
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
        logging.info("Downloading insights for account %s", self.account_id)
        logging.info("fields: %s", fields)
        logging.info("params: %s", params)
        campaign_insights = self.account.get_insights(
            params=params,
            fields=fields
        )

        for insight in campaign_insights:
            out.append(dict(insight))
        return out
