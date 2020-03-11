""" Facebook Tests """
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from pygyver.etl.facebook import transform_campaign_budget

def get_campaigns_mock():
    """
    Mock get_campaigns response.
    """
    return [
        {
            "account_id": "12345",
            "id": "111",
            "name": "Campaign 111",
            "lifetime_budget": "1000"
        },
        {
            "account_id": "12345",
            "id": "112",
            "name": "Campaign 112",
            "daily_budget": "50"
        }
    ]

def transform_campaign_budget_expected_outcome():
    """
    Expected Outcome from transform_campaign_budget() based on get_campaigns_mock().
    """
    data = [
        {
            "account_id": "12345",
            "campaign_id": "111",
            "campaign_name": "Campaign 111",
            "budget": "1000",
            "budget_type": "lifetime_budget"
        },
        {
            "account_id": "12345",
            "campaign_id": "112",
            "campaign_name": "Campaign 112",
            "budget": "50",
            "budget_type": "daily_budget"
        },
    ]
    return pd.DataFrame(data)

class FacebookDownloaderTest(unittest.TestCase):
    """ Facebook Downloader Test """

    def test_transform_campaign_budget(self):
        """
        Testing transform_campaign_budget() using get_campaigns_mock().
        """
        mock_data = get_campaigns_mock()
        result = transform_campaign_budget(mock_data)
        assert_frame_equal(
            result,
            transform_campaign_budget_expected_outcome()
        )

if __name__ == "__main__":
    unittest.main()
