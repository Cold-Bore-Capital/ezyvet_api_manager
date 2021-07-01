from datetime import datetime
from unittest import TestCase

from ezyvetapi.main import EzyVetApi


class TestEzyVetApi(TestCase):

    def test__build_params(self):
        e = EzyVetApi()
        res = e._build_params()
        test = res['limit']
        self.assertEqual(200, test)

        res = e._build_params({'test_value': 'abc'})
        test = res['test_value']
        self.assertEqual('abc', test)

    def test__get_api_credentials(self):
        e = EzyVetApi()
        db = MockAPICredentialsDBManager()
        get_access_token = lambda x, y: 'updated_cache_token'
        # Test with no timeout.
        res = e._get_api_credentials(3, 'https://test', 10, db, get_access_token)
        test = res['access_token']
        golden = 'abc123'
        self.assertEqual(golden, test)

        # Test expired cache
        db.system_time = datetime(2021, 1, 1, 5, 44, 22)
        res = e._get_api_credentials(3, 'https://test', 10, db, get_access_token)
        test = res['access_token']
        golden = 'updated_cache_token'
        self.assertEqual(golden, test)

    def test__get_endpoint_df(self):
        pass

    def test__set_headers(self):
        e = EzyVetApi()
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

    def test__get_data_from_api(self):
        e = MockEzyVetAPI()
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
        res = e._get_data_from_api(api_url, params, headers, endpoint, e.get_api)
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
        res = e._get_data_from_api(api_url, params, headers, endpoint, e.get_api)
        test = res[0]['id']
        golden = 1
        self.assertEqual(golden, test)

        test = res[6]['id']
        golden = 2
        self.assertEqual(golden, test)

        test = len(res)
        golden = 10
        self.assertEqual(golden, test)


class MockAPICredentialsDBManager(TestCase):

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


class MockEzyVetAPI(EzyVetApi):
    """
    A mockup class of the EzyVet API to allow for certain method overrides.

    """

    def __init__(self):
        self.get_api_mock_return_value = None
        super().__init__()

    def get_api(self,
                location_id: int,
                endpoint_name: str,
                endpoint_ver: str,
                params: dict = None,
                headers: dict = None):
        return self.get_api_mock_return_value
