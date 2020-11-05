""" Facebook Tests """
import unittest
import pandas as pd
from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal
from pygyver.etl.facebook import transform_campaign_budget
from pygyver.etl.facebook import build_predicted_revenue_events
from pygyver.etl.dw import BigQueryExecutor
from pygyver.etl.dw import read_sql


db = BigQueryExecutor()


def get_predicted_revenue_mock():

    sql = read_sql(
        file='tests/sql/unit_predicted_revenue_mocked.sql'
    )

    df1 = db.execute_sql(
        sql=sql,
    )

    return df1


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


class FacebookExecutorTest(unittest.TestCase):
    """ Facebook Executor Test """

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

    def test_build_predicted_revenue_events(self):
        """
        Testing build_predicted_revenue_events() using get_predicted_revenue_mock().
        """

        predicted_revenue_events = get_predicted_revenue_mock()
        result = build_predicted_revenue_events(predicted_revenue_events)
        df_result = result[1]
        assert_series_equal(predicted_revenue_events["date"], df_result["date_source"], check_names=False)
        assert_series_equal(predicted_revenue_events["predicted_revenue"], df_result["predicted_revenue"])
        assert_series_equal(predicted_revenue_events["currency"], df_result["currency"])
        assert_series_equal(predicted_revenue_events["facebook_browser_id"], df_result["facebook_browser_id"])
        assert_series_equal(predicted_revenue_events["shop"], df_result["shop"])


if __name__ == "__main__":
    unittest.main()
