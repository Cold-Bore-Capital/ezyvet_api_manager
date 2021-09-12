from datetime import datetime
from unittest import TestCase

import pandas as pd
from cbcdb import DBManager
from ezyvetapi import EzyVetApi

from ezyvetapi.models.contacts import Contacts


class TestContacts(TestCase):

    def test__get_address_df(self):
        c = Contacts(1, db=DBManagerMock())
        data = [
            {'id': 1, 'address_physical': 1},
            {'id': 2, 'address_physical': 2},
            {'id': 3, 'address_physical': 3},
            {'id': 4, 'address_physical': 4},
            {'id': 5, 'address_physical': 5}
        ]
        df = pd.DataFrame(data)

        res = c._get_address_df(1, df, EzyVetMock_get_address_df())
        test = res.iloc[0, 0]
        golden = 1
        self.assertEqual(golden, test)

        test = res.iloc[1, 1]
        golden = 'value 2'
        self.assertEqual(golden, test)

    def test_get_contact_details_df(self):
        c = Contacts(1, db=DBManagerMock())
        data = {'id': ['1', '2', '3', '4', '5'],
                'address_physical': ['1', '2', '3', '4', '5']}
        df = pd.DataFrame(data)

        res = c._get_contact_details_df(1, df, EzyVetMock_get_contact_details(), c._set_data_types)
        test = res.iloc[0, 0]
        golden = 'value 1'
        self.assertEqual(golden, test)

        test = res.iloc[1, 0]
        golden = 'value 2'
        self.assertEqual(golden, test)

    def test__make_contacts_df(self):
        c = Contacts(1, db=DBManagerMock())
        address_df = pd.DataFrame(
            [
                {"id": "1", "active": "1", "name": "Main Address", "street_1": "123 Test", "street_2": "",
                 "suburb": "Newton", "city": "Test", "region": "Auckland", "post_code": "89321", "country_id": "153",
                 "state": "AZ", "longitude": "", "latitude": ""
                 },
                {"id": "2", "active": "1", "name": "Main Address", "street_1": "456 Test", "street_2": "",
                 "suburb": "Newton",
                 "city": "Test", "region": "Auckland", "post_code": "24562", "country_id": "153", "state": "AZ",
                 "longitude": "", "latitude": ""
                 },
                {"id": "3", "active": "1", "name": "Main Address", "street_1": "789 Test", "street_2": "",
                 "suburb": "Newton",
                 "city": "Test", "region": "Auckland", "post_code": "34521", "country_id": "153", "state": "AZ",
                 "longitude": "", "latitude": ""
                 },
            ])

        contacts_df = pd.DataFrame([
            {"id": "7", "active": "1", "created_at": "1521505900", "modified_at": "1529881473", "code": "200465",
             "first_name": "Abc", "last_name": "Def", "business_name": "", "is_business": "0",
             "is_customer": "1", "is_supplier": "0", "is_vet": "0", "is_syndicate": "0", "is_staff_member": "0",
             "stop_credit": "OK", "address_physical": "1", "address_postal": "1", "ownership_id": "1"},
            {"id": "8", "active": "1", "created_at": "1521505900", "modified_at": "1529881473", "code": "200465",
             "first_name": "Craig", "last_name": "Smith", "business_name": "", "is_business": "0",
             "is_customer": "1", "is_supplier": "0", "is_vet": "0", "is_syndicate": "0", "is_staff_member": "0",
             "stop_credit": "OK", "address_physical": "2", "address_postal": "2", "ownership_id": "1"},
            {"id": "9", "active": "1", "created_at": "1521505900", "modified_at": "1529881473", "code": "200465",
             "first_name": "Frank", "last_name": "Smithy", "business_name": "demo test business", "is_business": "1",
             "is_customer": "0", "is_supplier": "1", "is_vet": "0", "is_syndicate": "0", "is_staff_member": "0",
             "stop_credit": "OK", "address_physical": "3", "address_postal": "3", "ownership_id": "1"}
        ])

        res = c._make_contacts_df(1, address_df, contacts_df, c._clean_int_values)
        test = len(res)
        golden = 3
        self.assertEqual(golden, test)

        test = res.loc[0, 'street_1']
        golden = '123 Test'
        self.assertEqual(golden, test)

    def test__set_data_types(self):
        c = Contacts(1, db=DBManagerMock())
        data = {'contact_id': ['1', '2', '3', '4'],
                'contact_detail_type_id': ['1', '2', '3', '4']}
        df = pd.DataFrame(data)
        c._set_data_types(df)

        test = str(df.contact_id.dtype)
        golden = 'int64'
        self.assertEqual(golden, test)

        test = str(df.contact_detail_type_id.dtype)
        golden = 'float64'
        self.assertEqual(golden, test)

    def test__clean_int_values(self):
        c = Contacts(1, db=DBManagerMock())

        data = {'contact_modified': ['1224556', '1244566', '2546112', ''],
                'longitude': ['012.3567', '123.56221', '41.2355', '46.2133']}
        df = pd.DataFrame(data)
        dtype_translation = {'contact_modified': 'int64', 'longitude': str}
        df = c._clean_int_values(df, dtype_translation)
        golden = 0
        test = df.iloc[3, 0]
        self.assertEqual(golden, test)

        test = str(df.contact_modified.dtype)
        golden = 'int64'
        self.assertEqual(golden, test)

    # def test__remove_existing_contacts(self):
    #     self.fail()

    def test__create_datetime_column(self):
        c = Contacts(1, db=DBManagerMock())

        df = pd.DataFrame({'contact_created': [1625122800, 1623581989, 1624013989],
                           'contact_modified': [1625209200, 1623691989, 1624413989]})

        c._create_datetime_column(df)

        golden = pd.Timestamp(2021, 7, 1, 7, 0, 0)
        test = df.loc[0, 'datetime_created']
        self.assertEqual(golden, test)

        golden = pd.Timestamp(2021, 7, 2, 7, 0, 0)
        test = df.loc[0, 'datetime_modified']
        self.assertEqual(golden, test)

    def test__apply_contact_details(self):
        c = Contacts(1, db=DBManagerMock())
        row = pd.Series({'ezyvet_id': 1})
        df = pd.DataFrame({'contact_id': [1, 1, 3, 4],
                           'contact_detail_type_id': [3, 1, 3, 4],
                           'value': ['5551234567', 'bo@email.com', 'toby@gmail.com', 'clover@email.com']})

        res = c._apply_contact_details(row, df)

        test = res['email_address']
        golden = 'bo@email.com'
        self.assertEqual(golden, test)

        test = res['phone_number']
        golden = 5551234567
        self.assertEqual(golden, test)


class EzyVetMock_get_address_df(EzyVetApi):

    def __init__(self, test_mode=True):
        super().__init__(test_mode)

    def get_by_ids(self,
                   location_id: int,
                   endpoint_ver: str,
                   endpoint_name: str,
                   ids,
                   id_field: str = 'id',
                   params: dict = None,
                   dataframe_flag: bool = False):
        return pd.DataFrame([
            {'id': 1, 'test': 'value 1', 'created_at': datetime(2021, 1, 1), 'modified_at': datetime(2021, 1, 1),
             'for_resource': 1},
            {'id': 2, 'test': 'value 2', 'created_at': datetime(2021, 1, 1), 'modified_at': datetime(2021, 1, 1),
             'for_resource': 1},
            {'id': 2, 'test': 'value 3', 'created_at': datetime(2021, 1, 1), 'modified_at': datetime(2021, 1, 1),
             'for_resource': 1}
        ])


class EzyVetMock_get_contact_details(EzyVetApi, TestCase):

    def __init__(self, test_mode=True):
        super().__init__(test_mode)

    def get_by_ids(self,
                   location_id: int,
                   endpoint_ver: str,
                   endpoint_name: str,
                   ids,
                   id_field: str = 'id',
                   params: dict = None,
                   dataframe_flag: bool = False):
        return pd.DataFrame(
            {'id': ['1', '2', '3', '4'], 'test': ['value 1', 'value 2', 'value 3', 'value 4'],
             'created_at': [datetime(2021, 1, 1), datetime(2021, 1, 1), datetime(2021, 1, 1), datetime(2021, 1, 1)],
             'modified_at': [datetime(2021, 1, 1), datetime(2021, 1, 1), datetime(2021, 1, 1), datetime(2021, 1, 1)],
             'for_resource': ['1', '2', '3', '4'], 'active': ['1', '1', '1', '1'],
             'contact_id': ['1', '2', '3', '4'],
             'contact_detail_type_id': ['1', '2', '3', '4']})


class DBManagerMock(DBManager):

    def __init__(self, test_mode=True):
        super().__init__(test_mode=test_mode)

    def execute_simple(self, sql, params=None, **kwargs):
        pass
