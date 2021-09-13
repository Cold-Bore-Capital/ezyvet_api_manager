from ezyvetapi.main import EzyVetApi


from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
from cbcdb import DBManager

from ezyvetapi.models.model import Model


class Appointments(Model):

    def __init__(self, location_id, db=None):
        super().__init__(location_id, db)
        self._filtered_types = {2: [2, 4],
                                3: [2, 4],
                                4: [2, 4],
                                5: [2, 4],
                                6: [2, 4],
                                7: [2, 4],
                                8: [2, 4]}

        self._is_medical = {
            2: [18, 26, 27, 28, 31, 32, 33, 34, 35, 37, 39, 40, 41, 56, 59, 60],
            3: [18, 23, 28, 29, 30, 32, 33, 34, 35, 36, 37, 38, 40, 59, 60, 61],
            4: [],
            5: [18, 26, 27, 28, 31, 32, 33, 34, 35, 39, 91, 104, 107, 109, 110, 111],
            6: [18, 26, 27, 28, 29, 30, 31, 32, 33, 34, 39, 40, 42, 55, 56, 57, 63, 64],
            7: [18, 42, 43, 46, 56, 59, 60, 62, 65, 67, 68, 70, 72, 74, 76, 78],
            8: [18, 43, 47, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 65, 66, 67]}

    def get_appointments(self, start_date: datetime = None, end_date: datetime = None, ids: List[int] = None) -> pd.DataFrame:
        """
        Controller method for EzyVet appointments.

        Args:
            start_date: Optional get_appointments date to pull appointments from.
            end_date: Optional end date to pull appointments to.
            ids: TO BE IMPLEMENTED

        Returns:
            A dataframe containing appointments.
        """
        print('Starting appointments load')
        db = self.db
        ezy = EzyVetApi()
        if not start_date or not end_date:
            start_date, end_date = self.get_most_recent('appointments', 'modified_at', 'is_shelter_animal_booking', False)
        params = {'active': True}
        appointments_df = ezy.get_date_range(self.location_id, 'v2', 'appointment', 'modified_at', params=params,
                                             start_date=start_date, end_date=end_date, dataframe_flag=True)

        if isinstance(appointments_df, pd.DataFrame):
            self._remove_block_out_bookings(self.location_id, appointments_df, self._filtered_types)
            appointments_df.rename(columns={'id': 'ezyvet_id'}, inplace=True)
            self._translate_id_fields(self.location_id, appointments_df, ezy)
            self._set_primary_resource_id(appointments_df)
            self._add_resource_data(self.location_id, appointments_df, ezy)

            self._assign_division_location_id(appointments_df, self.location_id)
            appointments_df = self._set_data_types(appointments_df)
            self._create_is_medical_column(self.location_id, appointments_df, self._is_medical)
            self._create_datetime_column(appointments_df)
            appointments_df = self._remove_columns(appointments_df)
            self._truncate_description_col(appointments_df)
            # Set the flag to identify this is a med or grooming appt.
            appointments_df['is_shelter_animal_booking'] = False
            return appointments_df
            # self._remove_existing_appointments(self.location_id, appointments_df)
            # self._save_appointments(appointments_df, db)
            # self._first_appointment_flag(db, appointments_df['ezyvet_id'].tolist())

    @staticmethod
    def _get_new_appointments_df(location_id: int, params: dict, get_endpoint_df: callable) -> pd.DataFrame:
        """
        Retrieves new appointments from EzyVet API and saves to a dataframe.

        Args:
            location_id: ID number of location to query.
            params: Instance of get_most_recent method
            get_endpoint_df: Instance of get_endpoint_df method.

        Returns:

        """

        if params:
            params['active'] = True
        else:
            params = {'active': True}
        appointments_df = get_endpoint_df(location_id, 'v2', 'appointment', params)
        return appointments_df

    @staticmethod
    def _truncate_description_col(appointments_df: pd.DataFrame) -> None:
        """
        Truncates the description field to 2000 char.

        Args:
            appointments_df: Appointments dataframe.

        Returns:
            None
        """
        appointments_df['description'] = appointments_df['description'].str[:1999]

    @staticmethod
    def _remove_columns(appointments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes unused columns from appointments dataframe.

        Args:
            appointments_df: Appointments dataframe.

        Returns:
            Appointments dataframe with columns removed.
        """
        columns_to_keep = ['location_id', 'division_id', 'ezyvet_id', 'created_at', 'modified_at', 'active',
                           'start_at', 'type_id', 'status_id', 'description', 'cancellation_reason',
                           'animal_id', 'consult_id', 'contact_id', 'sales_resource', 'resource_id', 'ownership_id',
                           'primary_resource_name', 'sales_resource_name', 'datetime_created', 'datetime_modified',
                           'datetime_start_at', 'is_medical', 'appt_type_id', 'first_appt']
        appointments_df = appointments_df[columns_to_keep].copy()
        return appointments_df

    @staticmethod
    def _set_data_types(appointments_df: pd.DataFrame) -> pd.DataFrame:
        """
        Sets correct data types for DF columns.

        Args:
            appointments_df: Appointments dataframe.

        Returns:
            Appointments dataframe with correct dtypes assigned.
        """
        appointments_df = appointments_df.astype(
            dtype={'animal_id': 'Int64', 'consult_id': 'Int64', 'contact_id': 'Int64'})
        appointments_df['active'] = appointments_df['active'].astype(int)
        return appointments_df

    @staticmethod
    def _remove_block_out_bookings(location_id: int, appointments_df: pd.DataFrame, filtered_types: dict) -> None:
        """
        Removes block out type bookings.

        Args:
            location_id: Location ID to process.
            appointments_df: Dataframe containing appointments
            filtered_types: A dict containing appointment type ID's to remove.

        Returns:
            None
        """
        # If you get an error here, you should scroll up and check filtered_types under def get_appointments
        mask = appointments_df['type_id'].isin(filtered_types[location_id])
        appointments_df.drop(appointments_df[mask].index, inplace=True)
        appointments_df.reset_index(drop=True, inplace=True)

    @staticmethod
    def _add_resource_data(location_id: int, appointments_df: pd.DataFrame, ezy: EzyVetApi) -> None:
        """
        Converts resource ID values to names.

        Args:
            location_id: The location ID to process
            appointments_df: Dataframe containing appointments
            ezy: An instance of the EzyVet API Main class.

        Returns:
            None, Pass by ref.
        """
        resource_df = ezy.get(location_id, 'v1', 'resource', dataframe_flag=True)
        # resource_df = resource_df[['id', 'ownership_id', 'name']].copy()
        # resources = resource_df.to_list()
        resource_df.index = resource_df['id'].astype(int)
        appointments_df['ownership_id'] = appointments_df['resource_id'].apply(
            lambda x: resource_df.loc[x, 'ownership_id'])
        appointments_df['primary_resource_name'] = appointments_df['resource_id'].apply(
            lambda x: resource_df.loc[x, 'name'])
        appointments_df['sales_resource_name'] = appointments_df['sales_resource'].apply(
            lambda x: resource_df.loc[x, 'name'])

    @staticmethod
    def _set_primary_resource_id(appointments_df: pd.DataFrame) -> None:
        """
        Picks the first resource ID and sets as the resource_id.

        The returned dataframe contains a nested list of dictionaries in this format:
        "resources": [{"id": 305},
                      {"id": 1797}]

        This method takes the first resource ID in the list (305 in this example) and sets that as the 'resource_id'
        field.

        Args:
            appointments_df: Dataframe containing appointments

        Returns:
            None. Pass by ref.
        """
        mask = ~pd.isna(appointments_df['resources'])
        appointments_df.loc[mask, 'resource_id'] = appointments_df.loc[mask, 'resources'].apply(lambda x: x[0]['id'])

    @staticmethod
    def _translate_id_fields(location_id: int, appointments_df: pd.DataFrame, ezy: EzyVetApi) -> None:
        """
        Translates the Appointment Type and Appointment Status fields from a number to a text value.

        Args:
            location_id: The location ID to process
            appointments_df: Dataframe containing appointments
            ezy: An instance of the EzyVet API Main class.

        Returns:
            None, Pass by ref.
        """
        appointments_df['first_appt'] = np.nan
        appointments_df['appt_type_id'] = appointments_df['type_id']
        appointments_df['type_id'].replace(ezy.get_translation(location_id, 'v1', 'appointmenttype'), inplace=True)
        appointments_df['status_id'].replace(ezy.get_translation(location_id, 'v1', 'appointmentstatus'), inplace=True)

    @staticmethod
    def _create_is_medical_column(location_id: int, appointments_df: pd.DataFrame, is_medical: dict) -> pd.DataFrame:
        """
        Creates a column flagging if the appointment is medical.

        This method uses a dictionary of appointment type lookups to flag if an appointment is medical. The lookup dict
        is structured like this:

        {2: [18, 26, 27, 28, 31, 32, 33, 34, 35, 37, 39, 40, 41, 56, 59, 60],
         3: [18, 23, 28, 29, 30, 32, 33, 34, 35, 36, 37, 38, 40, 59, 60, 61]}

        In this example, 2 is the location ID, and the list contains the appointment type ID's that would be flagged as
        medical.

        Args:
            location_id: The location ID to process
            appointments_df: Dataframe containing appointments
            is_medical: A dictionary containing the location ID and list of appointment type IDs.

        Returns:
            The appointments dataframe.
        """
        appointments_df['is_medical'] = 0
        mask = (appointments_df['appt_type_id'].isin(is_medical[location_id])) & (
                appointments_df['is_medical'] == 0)
        appointments_df.loc[mask, 'is_medical'] = 1

        return appointments_df




if __name__ == "__main__":
    # for location in [6]:
    location = 3
    app = Appointments(location_id=location)
    app.get_appointments()
