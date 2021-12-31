from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
from cbcdb import DBManager
from ezyvetapi import EzyVetApi
from numpy.distutils.system_info import p

from ezyvetapi.models.model import Model


class ShelterAnimalBooking(Model):

    def __init__(self, location_id, db=None):
        super().__init__(location_id, db)

    def start(self, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """
        Controller Method for EzyVet Shelter Animal Bookings.

        Note, this model saves to the appointments table, joining the two types of data together.

        Args:
            start_date: Optional start date to pull bookings from.
            end_date: Optional end date to pull bookings to.

        Returns:
            None
        """
        print('Starting shelter animal bookings load')
        db = self.db
        ezy = EzyVetApi()
        if not start_date or not end_date:
            start_date, end_date = self.get_most_recent('appointments', 'modified_at', 'is_shelter_animal_booking',
                                                        True)

        params = {'active': True}
        bookings_df = ezy.get_date_range(self.location_id, 'v2', 'shelteranimalbooking', 'modified_at', params=params,
                                         start_date=start_date, end_date=end_date, dataframe_flag=True)

        if isinstance(bookings_df, pd.DataFrame):
            bookings_df['is_shelter_animal_booking'] = True
            self._rename_fields(bookings_df)
            self._assign_division_location_id(bookings_df, self.location_id)
            self._create_datetime_column(bookings_df)
            self._create_duration_column(bookings_df)
            bookings_df['description'] = bookings_df['description'].astype('str').str.slice(0, 500)
            bookings_df['is_medical'] = 0
            self._drop_column(bookings_df)
            return booking_df

    @staticmethod
    def _rename_fields(bookings_df: pd.DataFrame) -> None:
        """
        Renames dataframe fields to start alignment with appointments table.

        Args:
            bookings_df: The dataframe containing bookings entries.

        Returns:
            None
        """
        rename_dict = {'shelter_resource_ownership_id': 'ownership_id', 'start_time': 'start_at', 'id': 'ezyvet_id',
                       'shelter_resource_name': 'type_id', 'status': 'status_id', 'comment': 'description'}
        bookings_df.rename(columns=rename_dict, inplace=True)

    @staticmethod
    def _create_duration_column(bookings_df: pd.DataFrame):
        """
        Calculates the duration value from the start and end time.

        Args:
            bookings_df: The dataframe containing bookings entries.

        Returns:
            None
        """
        bookings_df['duration'] = round((bookings_df['end_time'] - bookings_df['start_at']) / 60)

    @staticmethod
    def _drop_column(bookings_df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes unused columns

        Args:
            bookings_df:  The dataframe containing bookings entries.

        Returns:
            An instance of the dataframe with columns dropped.
            @todo. Check if drop removes pass by ref. Return might not be needed.
        """
        drop_columns = ['end_time', 'shelter_resource_place_id', 'shelter_resource_id']
        bookings_df.drop(columns=drop_columns, inplace=True)
        return bookings_df

    @staticmethod
    def _save_bookings_to_appointments_tbl(bookings_df: pd.DataFrame, db: DBManager) -> None:
        """
        Saves appointment to the database.
        Args:
            bookings_df: Dataframe containing appointments, with columns matching database table.
            db: An instance of DBManager

        Returns:
            None
        """
        sql, params = db.build_sql_from_dataframe(bookings_df, 'appointments', 'gclick')
        db.execute_many(sql, params)


if __name__ == '__main__':
    e = ShelterAnimalBooking(3)
    e.start(datetime(2020, 1, 1), datetime(2020, 5, 1))
