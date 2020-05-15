"""Facebook API"""
import os
import json
import logging
import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

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

class FacebookExecutor:
    """ Facebook FacebookExecutor.
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
