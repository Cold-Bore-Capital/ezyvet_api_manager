from unittest import TestCase

import pandas as pd

from ezyvetapi.main import EzyVetApi
from ezyvetapi.models.appointments import Appointments


class TestAppointments(TestCase):

    def test__truncate_description_col(self):
        a = Appointments(1, MockDBManager())
        data = {'description': ['this is some short text', '''this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. 
        this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text.
        this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text.
        this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text.
        this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text.
        this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text. this is some really long text.''']}

        df = pd.DataFrame(data)
        a._truncate_description_col(df)
        test = len(df.loc[1, 'description'])
        golden = 1999
        self.assertEqual(golden, test)

        test = len(df.loc[0, 'description'])
        golden = 23
        self.assertEqual(golden, test)

    def test__remove_columns(self):
        a = Appointments(1, MockDBManager())
        df = pd.DataFrame({'location_id': [1, 2, 3, 4, 5],
                           'division_id': [1, 2, 3, 4, 5],
                           'ezyvet_id': [1, 2, 3, 4, 5],
                           'created_at': [1, 2, 3, 4, 5],
                           'modified_at': [1, 2, 3, 4, 5],
                           'active': [1, 2, 3, 4, 5],
                           'start_at': [1, 2, 3, 4, 5],
                           'type_id': [1, 2, 3, 4, 5],
                           'status_id': [1, 2, 3, 4, 5],
                           'description': [1, 2, 3, 4, 5],
                           'cancellation_reason': [1, 2, 3, 4, 5],
                           'animal_id': [1, 2, 3, 4, 5],
                           'consult_id': [1, 2, 3, 4, 5],
                           'contact_id': [1, 2, 3, 4, 5],
                           'sales_resource': [1, 2, 3, 4, 5],
                           'resource_id': [1, 2, 3, 4, 5],
                           'ownership_id': [1, 2, 3, 4, 5],
                           'primary_resource_name': [1, 2, 3, 4, 5],
                           'sales_resource_name': [1, 2, 3, 4, 5],
                           'datetime_created': [1, 2, 3, 4, 5],
                           'datetime_modified': [1, 2, 3, 4, 5],
                           'datetime_start_at': [1, 2, 3, 4, 5],
                           'is_medical': [1, 2, 3, 4, 5],
                           'appt_type_id': [1, 2, 3, 4, 5],
                           'first_appt': [1, 2, 3, 4, 5],
                           'kill_me': [1, 2, 3, 4, 5],
                           'kill_me_2': [1, 2, 3, 4, 5]
                           })

        valid_col_list = ['location_id', 'division_id', 'ezyvet_id', 'created_at', 'modified_at', 'active', 'start_at',
                          'type_id', 'status_id', 'description', 'cancellation_reason', 'animal_id', 'consult_id',
                          'contact_id', 'sales_resource', 'resource_id', 'ownership_id', 'primary_resource_name',
                          'sales_resource_name', 'datetime_created', 'datetime_modified', 'datetime_start_at',
                          'is_medical', 'appt_type_id', 'first_appt']
        df = a._remove_columns(df)
        columns = list(df.columns)

        if 'kill_me' in columns:
            self.fail()

        if 'kill_me_2' in columns:
            self.fail()

        if len(columns) != len(valid_col_list):
            self.fail()

    def test__set_data_types(self):
        a = Appointments(1, MockDBManager())
        data = {'animal_id': [1, 1, 2, 3, 4, None], 'consult_id': [1, 1, 2, 3, 4, None],
                'contact_id': [1, 1, 2, 3, 4, None], 'active': [1, 0, 0, 1, 0, 0]}
        df = pd.DataFrame(data)
        res = a._set_data_types(df)
        dt = res.dtypes
        self.assertEqual(dt['animal_id'], 'Int64')
        self.assertEqual(dt['consult_id'], 'Int64')
        self.assertEqual(dt['contact_id'], 'Int64')
        self.assertEqual(dt['active'], int)

    def test__remove_block_out_bookings(self):
        a = Appointments(2, MockDBManager())
        df = pd.DataFrame({'type_id': [1, 2, 3, 4, 5, 6]})

        a._remove_block_out_bookings(2, df, {2: [2, 4]})
        test = len(df['type_id'])
        golden = 4
        self.assertEqual(golden, test)

    def test__add_resource_data(self):
        a = Appointments(1, MockDBManager())
        data = {'resource_id': [1, 2, 3],
                'sales_resource': [1, 2, 3]}
        df = pd.DataFrame(data)
        a._add_resource_data(2, df, MockForAddResourceData())
        test = df.loc[0, 'ownership_id']
        golden = 1
        self.assertEqual(golden, test)

        test = df.loc[2, 'primary_resource_name']
        golden = 'Jack'
        self.assertEqual(golden, test)

    def test__set_primary_resource_id(self):
        a = Appointments(1, MockDBManager())
        data = {"resources": [
            # This is a list of lists.
            [{"id": 305},
             {"id": 1797}],
            [{"id": 21},
             {"id": 642}],
            None
        ]}
        df = pd.DataFrame(data)
        a._set_primary_resource_id(df)
        test = df.loc[0, 'resource_id']
        golden = 305
        self.assertEqual(golden, test)

    def test__translate_id_fields(self):
        a = Appointments(1, MockDBManager())
        data = {'type_id': [1, 2, 3],
                'status_id': [1, 2, 3]}
        df = pd.DataFrame(data)
        a._translate_id_fields(2, df, MockForTranslateIdFields())
        test = df.loc[0, 'type_id']
        golden = 'ABC'
        self.assertEqual(golden, test)

        test = df.loc[1, 'status_id']
        golden = 'DEF'
        self.assertEqual(golden, test)

    def test___create_is_medical_column(self):
        a = Appointments(1, MockDBManager())
        data = {'appt_type_id': [1, 2, 3, 4, 5]}
        df = pd.DataFrame(data)

        is_medical_dict = {2: [1, 3, 5]}
        res = a._create_is_medical_column(2, df, is_medical_dict)
        golden = 1
        test = res.loc[0, 'is_medical']
        self.assertEqual(golden, test)

        golden = 0
        test = res.loc[1, 'is_medical']
        self.assertEqual(golden, test)


class MockDBManager:

    def __init__(self, **kwargs):
        pass


class MockForAddResourceData(EzyVetApi):

    def __init__(self):
        super().__init__(test_mode=True)

    def get(self,
            location_id: int,
            endpoint_ver: str,
            endpoint_name: str,
            params: dict = None,
            headers: dict = None,
            dataframe_flag: bool = False) -> pd.DataFrame:
        data = {'id': [1, 2, 3],
                'ownership_id': [1, 2, 3],
                'name': ['John', 'James', 'Jack']}
        return pd.DataFrame(data)

class MockForTranslateIdFields(EzyVetApi):

    def __init__(self):
        super().__init__(test_mode=True)

    def get_translation(self, location_id: int, endpoint_ver: str, endpoint_name: str) -> dict:
        data = {1: 'ABC', 2: 'DEF', 3: 'HIG'}
        return data