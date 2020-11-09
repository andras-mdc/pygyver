""" Facebook Tests """
import unittest
import pandas as pd
import mock
from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal
from pygyver.etl.facebook import transform_campaign_budget
from pygyver.etl.facebook import build_predicted_revenue_events, calculate_batches, \
    split_events_to_batches, FacebookExecutor
from pygyver.etl.dw import BigQueryExecutor
from pygyver.etl.dw import read_sql
from facebook_business.adobjects.serverside.event_request import EventResponse
from facebook_business.exceptions import FacebookRequestError


db = BigQueryExecutor()

error_json = {
    "error": {
        "fbtrace_id": "test_fb_trace_id",
        "message": "Some generic message",
        "error_user_msg": "A more detailed message"
    }
}

context = mock.Mock().files = {'test': 'test'}


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


def fb_login_mock():
    True


def fb_api_mock():
    return EventResponse(events_received=3,
                         fbtrace_id='test_fbtrace_id',
                         messages='')


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

    def test_calculate_batches(self):
        """
        Testing build_predicted_revenue_events() using get_predicted_revenue_mock().
        """

        result = calculate_batches(10, 3)
        res_even = calculate_batches(10, 5)
        res_single_event = calculate_batches(1, 5)
        res_no_event = calculate_batches(0, 5)

        self.assertEqual(result, 4)
        self.assertEqual(res_even, 2)
        self.assertEqual(res_single_event, 1)
        self.assertEqual(res_no_event, 0)

    def test_split_events_to_batches(self):
        """
        Testing build_predicted_revenue_events() using get_predicted_revenue_mock().
        """

        predicted_revenue_events = get_predicted_revenue_mock()

        total_events = len(predicted_revenue_events)
        batch_size = 2

        batches = calculate_batches(total_events, batch_size)

        result = split_events_to_batches(predicted_revenue_events, batch_size)

        self.assertEqual(len(result), batches)

    @mock.patch('pygyver.etl.facebook.FacebookExecutor.set_api_config', side_effect=fb_login_mock)
    @mock.patch('facebook_business.adobjects.serverside.event_request.EventRequest.execute', side_effect=fb_api_mock)
    def test_push_conversions_api_events(self, fb_api_mock, fb_login_mock):
        """
        Testing push_conversions_api_events() using get_predicted_revenue_mock().
        """

        fbe = FacebookExecutor()
        fbe.set_pixel_id('1530331220624093')
        predicted_revenue_events = get_predicted_revenue_mock()
        events, log = build_predicted_revenue_events(predicted_revenue_events)
        result = fbe.push_conversions_api_events(events, 'TEST24777')

        self.assertEqual(result['status'], 'API Success')
        self.assertEqual(result['total_events'], len(predicted_revenue_events))

    @mock.patch('pygyver.etl.facebook.FacebookExecutor.set_api_config', side_effect=fb_login_mock)
    @mock.patch('facebook_business.adobjects.serverside.event_request.EventRequest.execute',
                side_effect=FacebookRequestError(message="test exception", request_context=context, http_status="404",
                                                 http_headers="some/headers", body=error_json))
    def test_push_conversions_api_events_error(self, fb_api_mock, fb_login_mock):
        """
        Testing push_conversions_api_events() using get_predicted_revenue_mock().
        """

        fbe = FacebookExecutor()
        fbe.set_pixel_id('1530331220624093')
        predicted_revenue_events = get_predicted_revenue_mock()
        events, log = build_predicted_revenue_events(predicted_revenue_events)
        result = fbe.push_conversions_api_events(events, 'TEST24777')

        self.assertEqual(result['status'], 'API Error')


if __name__ == "__main__":
    unittest.main()
