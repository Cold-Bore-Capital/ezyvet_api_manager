from unittest import TestCase

import pandas as pd

from ezyvetapi.models.shelter_animal_bookings import ShelterAnimalBooking


class TestEzyVetShelterAnimalBooking(TestCase):

    def test__rename_fields(self):
        db = MockDBManager()
        e = ShelterAnimalBooking(1, db)
        df = pd.DataFrame(columns=['shelter_resource_ownership_id', 'start_time', 'id', 'shelter_resource_name',
                                   'status', 'comment'])
        e._rename_fields(df)

        columns_golden = ['ownership_id', 'start_at', 'ezyvet_id', 'type_id', 'status_id', 'description']
        test = list(df.columns)
        for col in test:
            if col not in columns_golden:
                self.fail()

    def test__create_duration_column(self):
        db = MockDBManager()
        e = ShelterAnimalBooking(1, db)
        df = pd.DataFrame({'start_at': [1628053200, 1627362000, 1627534800],
                           'end_time': [1628139599, 1627923599, 1627837199]})
        e._create_duration_column(df)

        golden = 1440
        test = df.loc[0, 'duration']
        self.assertEqual(golden, test)
        golden = 5040
        test = df.loc[2, 'duration']
        self.assertEqual(golden, test)



class MockDBManager:

    def __init__(self, **kwargs):
        pass