from datetime import datetime
import time
from unittest import TestCase

import pandas as pd
from cbcdb import DBManager

from ezyvetapi.main import EzyVetApi, StartEndAndDaysSet, MissingStartAndEndDate


class TestEzyVetApi(TestCase):

    def test__get(self):
        e = MockEzyVetAPI_test_get()
        e._db = MockDBManager_test_get()
        data = [{'id': 1, 'active': 1, 'testme': 'string value'}, {'id': 2, 'active': 1, 'testme': 'string value'},
                {'id': 3, 'active': 0, 'testme': 'string value'}, {'id': 4, 'active': 1, 'testme': 'string value'},
                {'id': 4, 'active': 1, 'testme': 'string value'}, {'id': 1, 'active': 1, 'testme': 'string value'},
                {'id': 2, 'active': 1, 'testme': 'string value'}, {'id': 3, 'active': 0, 'testme': 'string value'},
                {'id': 4, 'active': 1, 'testme': 'string value'}, {'id': 4, 'active': 1, 'testme': 'string value'}]
        e.get_api_mock_return_value = data

        res = e.get(2, 'v2', 'testing', dataframe_flag=True)

        self.assertTrue(isinstance(res, pd.DataFrame))

        test = res.loc[0, 'id']
        golden = 1
        self.assertEqual(golden, test)

        test = res.loc[8, 'testme']
        golden = 'string value'
        self.assertEqual(golden, test)

        res = e.get(2, 'v2', 'testing', dataframe_flag=False)

        self.assertTrue(isinstance(res, list))

        test = res[0]['id']
        golden = 1
        self.assertEqual(golden, test)

        test = res[8]['testme']
        golden = 'string value'
        self.assertEqual(golden, test)

    def test__get_by_ids(self):
        e = MockEzyVetAPI_test_get_by_id()
        data = [{'id': 1, 'active': 1, 'testme': 'string value'}, {'id': 2, 'active': 1, 'testme': 'string value'},
                {'id': 3, 'active': 0, 'testme': 'string value'}, {'id': 4, 'active': 1, 'testme': 'string value'},
                {'id': 4, 'active': 1, 'testme': 'string value'}, {'id': 1, 'active': 1, 'testme': 'string value'},
                {'id': 2, 'active': 1, 'testme': 'string value'}, {'id': 3, 'active': 0, 'testme': 'string value'},
                {'id': 4, 'active': 1, 'testme': 'string value'}, {'id': 4, 'active': 1, 'testme': 'string value'}]
        df_data = pd.DataFrame(data)
        e.get_api_mock_return_value = df_data
        e.golden = {'id': {'in': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}
        ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        res = e.get_by_ids(2, 'v2', 'testing', ids, dataframe_flag=True)
        self.assertTrue(isinstance(res, pd.DataFrame))
        test = res.loc[0, 'id']
        golden = 1
        self.assertEqual(golden, test)
        test = res.loc[8, 'testme']
        golden = 'string value'
        self.assertEqual(golden, test)

        e.golden = {'something': 'else', 'id': {'in': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}
        ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        params = {'something': 'else'}
        res = e.get_by_ids(2, 'v2', 'testing', ids, params=params, dataframe_flag=True)

        e.golden = {'something': 'else', 'id': {'in': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}
        ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        params = {'something': 'else'}
        res = e.get_by_ids(2, 'v2', 'testing', ids, params=params, dataframe_flag=False)
        self.assertTrue(isinstance(res, list))

        # Check just a single ID
        data = [{'id': 1, 'active': 1, 'testme': 'string value'}, {'id': 2, 'active': 1, 'testme': 'string value'}]
        df_data = pd.DataFrame(data)
        e.get_api_mock_return_value = df_data
        e.golden = {'something': 'else', 'id': {'in': [1]}}
        ids = 1
        params = {'something': 'else'}
        res = e.get_by_ids(2, 'v2', 'testing', ids, params=params, dataframe_flag=False)
        self.assertTrue(isinstance(res, list))
        test = res[0]['id']
        golden = 1
        self.assertEqual(golden, test)

    def test__build_params(self):
        e = EzyVetApi(test_mode=True)
        res = e._build_params()
        test = res['limit']
        self.assertEqual(200, test)

        res = e._build_params({'test_value': 'abc'})
        test = res['test_value']
        self.assertEqual('abc', test)

    def test__get_api_credentials(self):
        e = EzyVetApi(test_mode=True)
        db = MockEzyVetAPI_test__get_api_credentials()
        get_access_token = lambda x, y: 'updated_cache_token'
        # Test with no timeout.
        res = e._get_api_credentials(3, 'https://test', db, get_access_token, 10)
        test = res['access_token']
        golden = 'abc123'
        self.assertEqual(golden, test)

        # Test expired cache
        db.system_time = datetime(2021, 1, 1, 5, 44, 22)
        res = e._get_api_credentials(3, 'https://test', db, get_access_token, 10)
        test = res['access_token']
        golden = 'updated_cache_token'
        self.assertEqual(golden, test)

    def test__get_translation(self):
        e = MockEzyVetAPI_test_get()
        data = [
            {'id': 1, 'name': 'red'},
            {'id': 2, 'name': 'green'},
            {'id': 3, 'name': 'yellow'},
            {'id': 4, 'name': 'blue'},
            {'id': 5, 'name': 'purple'},
            {'id': 6, 'name': 'black'},
        ]
        e.get_api_mock_return_value = data
        res = e.get_translation(1, 'v2', 'testme')

        test = res[1]
        golden = 'red'
        self.assertEqual(golden, test)

        test = res[5]
        golden = 'purple'
        self.assertEqual(golden, test)

        test = len(res)
        golden = 6
        self.assertEqual(golden, test)

    def test__set_headers(self):
        e = EzyVetApi(test_mode=True)
        api_credentials = {'access_token': 'abc123'}
        # Test with no additional headers.
        res = e._set_headers(api_credentials)
        test = res['Authorization']
        golden = 'Bearer abc123'
        self.assertEqual(golden, test)

        # Test with additional headers.
        res = e._set_headers(api_credentials, {'some_other': 'header_value'})
        test = res['Authorization']
        golden = 'Bearer abc123'
        self.assertEqual(golden, test)

        test = res['some_other']
        golden = 'header_value'
        self.assertEqual(golden, test)

    def test__call_api(self):
        pass

    def test__get_data_from_api(self):
        e = MockEzyVetAPI_test_get_data_from_api()
        api_url = 'https://testme.test'
        params = {'and_integer': 2, 'a_list': ['hi', 'there'], 'a_dict': {'key', 'value'}}
        headers = {'Authorization': 'Bearer abc123'}
        endpoint = 'v2/testing'
        # Single page of results testing
        meta = {'items_total': 5,
                'items_page_total': 1}
        items = [
            {'testing': {'id': 1, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 2, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 3, 'active': 0, 'testme': 'string value'}},
            {'testing': {'id': 4, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 4, 'active': 1, 'testme': 'string value'}},
        ]
        data = {'meta': meta, 'items': items}
        e.get_api_mock_return_value = data

        db = MockDBManager_test_get()
        # Note that "e" is a mockup that replaces the _get_api native call.
        res = e._get_data_from_api(api_url, params, headers, endpoint, db, 10)
        test = res[0]['id']
        golden = 1
        self.assertEqual(golden, test)

        test = len(res)
        golden = 5
        self.assertEqual(golden, test)

        meta = {'items_total': 10,
                'items_page_total': 2,
                'items_page_size': 5}
        items = [
            {'testing': {'id': 1, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 2, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 3, 'active': 0, 'testme': 'string value'}},
            {'testing': {'id': 4, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 4, 'active': 1, 'testme': 'string value'}},
        ]
        data = {'meta': meta, 'items': items}
        e.get_api_mock_return_value = data
        res = e._get_data_from_api(api_url, params, headers, endpoint, e._call_api, 1)
        test = res[0]['id']
        golden = 1
        self.assertEqual(golden, test)

        test = res[6]['id']
        golden = 2
        self.assertEqual(golden, test)

        test = len(res)
        golden = 10
        self.assertEqual(golden, test)

        # Now test the rate limit feature

        meta = {'items_total': 200,
                'items_page_total': 20,
                'items_page_size': 5}
        items = [
            {'testing': {'id': 1, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 2, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 3, 'active': 0, 'testme': 'string value'}},
            {'testing': {'id': 4, 'active': 1, 'testme': 'string value'}},
            {'testing': {'id': 4, 'active': 1, 'testme': 'string value'}},
        ]
        data = {'meta': meta, 'items': items}
        e.get_api_mock_return_value = data
        # Set calls per minute to a really low number.
        e.test_rate_limit = True
        res = e._get_data_from_api(api_url, params, headers, endpoint, e._call_api, 1,
                                   calls_per_minute_limit=10, seconds_in_a_min=1)

    def test__build_date_filter(self):
        e = EzyVetApi(test_mode=True)
        tests = [
            {'id': 1, 'name': 'Start and End Date Set', 'start_date': datetime(2021, 1, 1, 5, 43, 12),
             'end_date': datetime(2021, 1, 10, 5, 43, 12), 'days': 0,
             'golden': {'test': {'gt': 1609504992.0, 'lte': 1610282592.0}}},

            {'id': 2, 'name': 'Start date set, no days', 'start_date': datetime(2021, 1, 1, 5, 43, 12),
             'end_date': None, 'days': 0, 'golden': {'test': {'gt': 1609504992.0}}},

            {'id': 3, 'name': 'Start date set, 10 days', 'start_date': datetime(2021, 1, 1, 5, 43, 12),
             'end_date': None, 'days': 10, 'golden': {'test': {'gt': 1609504992.0, 'lte': 1610368992.0}}},

            {'id': 4, 'name': 'End date set, no get_appointments', 'start_date': None,
             'end_date': datetime(2021, 1, 10, 5, 43, 12), 'days': 0, 'golden': {'test': {'lt': 1610282592.0}}},

            {'id': 5, 'name': 'End date set, 10 days', 'start_date': None,
             'end_date': datetime(2021, 1, 10, 5, 43, 12), 'days': 10,
             'golden': {'test': {'gt': 1609418592.0, 'lte': 1610282592.0}}},
            {'id': 6, 'name': 'End date with no time, no get_appointments', 'start_date': None,
             'end_date': datetime(2021, 1, 10), 'days': 0, 'golden': {'test': {'lt': 1610348399.0}}},
        ]
        for t in tests:
            test_name = t['name']
            print(f'test__build_date_filter: Testing "{test_name}"')
            test = e._build_date_filter(filter_field='test',
                                        start_date=t['start_date'],
                                        end_date=t['end_date'],
                                        days=t['days'])
            # datetime.fromtimestamp(timestamp)
            self.assertDictEqual(t['golden'], test)

        # Test Error Conditions. Start, end, and days set
        start_date = datetime(2021, 1, 1, 5, 43, 12)
        end_date = datetime(2021, 1, 10, 5, 43, 12)
        days = 10
        with self.assertRaises(StartEndAndDaysSet):
            test = e._build_date_filter(filter_field='test',
                                        start_date=start_date,
                                        end_date=end_date,
                                        days=days)

        # Test Error Conditions. No get_appointments or end date set
        start_date = None
        end_date = None
        days = 10
        with self.assertRaises(MissingStartAndEndDate):
            test = e._build_date_filter(filter_field='test',
                                        start_date=start_date,
                                        end_date=end_date,
                                        days=days)


class MockEzyVetAPI_test__get_api_credentials(TestCase):

    def __init__(self):
        # This is set so the time can be modified to test the access_token expire timeout.
        self.system_time = datetime(2021, 1, 1, 5, 34, 22)
        self.db_schema = 'test'
        super().__init__()

    def get_sql_list_dicts(self, sql, parmas):
        return [{
            'system_time': self.system_time,
            'access_token': 'abc123',
            'access_token_create_time': datetime(2021, 1, 1, 5, 32, 22),
            # No need for the other params for testings.
        }]

    def execute_simple(self, sql, params=None):
        golden = 'update test.ezy_vet_credentials set access_token=%s, access_token_create_time=%s where location_id = %s'
        self.assertEqual(golden, sql)

        test = params[0]
        golden = 'updated_cache_token'
        self.assertEqual(golden, test)


class MockEzyVetAPI_test_get(EzyVetApi, TestCase):
    """
    A mockup class of the EzyVet API to allow for certain method overrides.

    """

    def __init__(self):
        self.get_api_mock_return_value = None
        super().__init__(test_mode=True)
        self.test_rate_limit = False
        self.start_time = datetime.now()
        # The number of call in x seconds.
        self.rate_time_window = 4
        self.rate_counter = 1

    def _get_data_from_api(self,
                           api_url: str,
                           params: dict,
                           headers: dict,
                           endpoint: str,
                           db: DBManager,
                           location_id: int,
                           calls_per_minute_limit: int = 60,
                           seconds_in_a_min: int = 60) -> list:

        if self.test_rate_limit:
            elapsed_time = (datetime.now() - self.start_time).seconds
            print(f'There have been {self.rate_counter} calls in {elapsed_time} seconds')
            if self.rate_counter > self.test_rate_limit and elapsed_time >= self.rate_time_window:
                self.fail()

        return self.get_api_mock_return_value

    @staticmethod
    def _get_api_credentials(location_id, api_url, db, get_access_token, cache_limit=10):
        return {'system_time': datetime(2021, 1, 1, 9, 15, 4),
                'access_token': 'test_access_token',
                'access_token_create_time': datetime(2021, 1, 1, 9, 15, 4)
                }


class MockDBManager_test_get(EzyVetApi):

    def __init__(self):
        self.db_schema = 'test'
        super().__init__(test_mode=True)

    def get_sql_list_dicts(self, sql, params, **kwargs):
        return [{'system_time': datetime(2021, 1, 1, 9, 15, 4),
                 'access_token': 'test_access_token',
                 'access_token_create_time': datetime(2021, 1, 1, 9, 15, 4)
                 }]


class MockEzyVetAPI_test_get_data_from_api(EzyVetApi):
    """
    A mockup class of the EzyVet API to allow for certain method overrides.

    """

    def __init__(self):
        self.get_api_mock_return_value = None
        super().__init__(test_mode=True)

    def _call_api(self, url: str, headers: dict, params: dict, db, location_id) -> dict:
        time.sleep(0.25)
        return self.get_api_mock_return_value


class MockEzyVetAPI_test_get_by_id(EzyVetApi, TestCase):

    def __init__(self):
        self.get_api_mock_return_value = None
        self.golden = None
        super().__init__(test_mode=True)

    def get(self,
            location_id: int,
            endpoint_ver: str,
            endpoint_name: str,
            params: dict = None,
            headers: dict = None,
            dataframe_flag: bool = False
            ):
        self.assertDictEqual(self.golden, params)

        return self.get_api_mock_return_value
