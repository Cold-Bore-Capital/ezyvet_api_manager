import pandas as pd
from cbcdb import DBManager
from ezyvetapi import EzyVetApi
from datetime import datetime
from typing import List

from ezyvetapi.models.model import Model
import phonenumbers


class Contacts(Model):

    def __init__(self, location_id, db=None):
        super().__init__(location_id, db)

    def start(self, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """
        Add desc
        Args:
            start_date:
            end_date:

        Returns:

        """
        print('Starting contacts load')
        ezy = EzyVetApi()
        end_date, start_date = self._set_date_range(end_date, start_date)

        contacts_df = ezy.get_date_range(self.location_id,
                                         'v1',
                                         'contact',
                                         'modified_at',
                                         start_date=start_date,
                                         end_date=end_date,
                                         dataframe_flag=True)

        if not isinstance(contacts_df, pd.DataFrame):
            print('No new contacts found')
            return False

        address_df = self._get_address_df(self.location_id, contacts_df, ezy)

        contacts_detail_df = self._get_contact_details_df(self.location_id,
                                                          contacts_df,
                                                          ezy,
                                                          self._set_data_types)

        contacts_df = self._make_contacts_df(self.location_id, address_df, contacts_df, self._clean_int_values)

        contacts_df = contacts_df.apply(self._apply_contact_details, axis=1, args=(contacts_detail_df,))
        self._create_datetime_column(contacts_df)
        contacts_df = self._remove_unused_columns(contacts_df)
        return contacts_df

    def _set_date_range(self, end_date, start_date):
        """
        Set's the start and end date for the query pull.

        If a start and end date are provided into the method, those will be used. Otherwise, the most recent date from
        the contacts table will be used.

        Args:
            end_date:
            start_date:

        Returns:

        """
        start_date_tmp, end_date_tmp = self.get_most_recent('contacts', 'contact_modified')
        start_date = start_date if start_date else start_date_tmp
        end_date = end_date if end_date else end_date_tmp
        return end_date, start_date

    @staticmethod
    def _remove_unused_columns(contacts_df):
        columns_to_keep = ['location_id', 'ezyvet_id', 'active', 'contact_created', 'contact_modified', 'code',
                           'first_name', 'last_name', 'business_name', 'is_business', 'is_customer', 'is_supplier',
                           'is_vet', 'is_syndicate', 'is_staff_member', 'stop_credit', 'ownership_id',
                           'name', 'street_1', 'street_2', 'suburb', 'city',
                           'regions', 'post_code', 'country_id', 'state', 'longitude', 'latitude', 'email_address',
                           'phone_number', 'datetime_created',
                           'datetime_modified']
        contacts_df = contacts_df[columns_to_keep].copy()
        return contacts_df

    @staticmethod
    def _get_address_df(location_id: int,
                        contacts_df: pd.DataFrame,
                        ezy: EzyVetApi) -> pd.DataFrame:
        """
        Creates a DataFrame containing street addresses based on a list of ID's from the contacts.

        Args:
            location_id: The location ID in use.
            contacts_df: A dataframe containing the output from the EzyVet API contacts end point.
            ezy: An instance of the EzyVetApi class.

        Returns:
            A dataframe containing street addresses.
        """
        address_ids = contacts_df['address_physical'].values.tolist()
        address_df = ezy.get_by_ids(location_id,
                                    endpoint_ver='v1',
                                    endpoint_name='address',
                                    ids=address_ids,
                                    dataframe_flag=True)
        address_df.drop(columns=['created_at', 'modified_at', 'for_resource'], inplace=True)
        return address_df

    @staticmethod
    def _make_contacts_df(location_id: int,
                          address_df: pd.DataFrame,
                          contacts_df: pd.DataFrame,
                          clean_int_values: callable) -> pd.DataFrame:
        """
        Merges contacts and address information into a single dataframe and sets data types.

        Args:
            location_id: The location ID in use.
            address_df: A dataframe containing address details.
            contacts_df: A dataframe containing contact details.
            clean_int_values: An instance of the _clean_int_values method.

        Returns:
            A DataFrame containing contacts merged with addresses.
        """
        contacts_df = pd.merge(contacts_df, address_df, left_on='address_physical', right_on='id', how='left')
        # contacts_df.drop(columns=[], inplace=True)
        contacts_df.drop(columns=['address_postal', 'id_y', 'active_y', 'address_physical'],
                         inplace=True)
        contacts_df.rename(columns={'id_x': 'ezyvet_id', 'active_x': 'active', 'created_at': 'contact_created',
                                    'modified_at': 'contact_modified', 'region': 'regions'}, inplace=True)
        dtype_translation = {'ezyvet_id': 'int64', 'active': 'int64', 'contact_created': 'int64',
                             'contact_modified': 'int64',
                             'code': str, 'is_business': 'int64', 'is_customer': 'int64', 'is_supplier': 'int64',
                             'is_vet': 'int64', 'is_syndicate': 'int64', 'is_staff_member': 'int64',
                             'ownership_id': 'int64',
                             'country_id': 'int64', 'longitude': str,
                             'latitude': str}
        contacts_df = clean_int_values(contacts_df, dtype_translation)

        contacts_df['location_id'] = location_id
        contacts_df[['email_address', 'phone_number']] = None
        return contacts_df

    # function to clean int64 values in DataFrame
    @staticmethod
    def _clean_int_values(df, dtype_translation) -> pd.DataFrame:
        """
        Converts nulls and blank strings in numeric fields to zero and converts dtypes.
        Args:
            df: Dataframe to convert values in
            dtype_translation: A dict containing the key name and data type.
                               Example: {'contact_modified': 'int64', 'longitude': str}

        Returns:
            A dataframe with the numeric types converted.
        """
        for key, value in dtype_translation.items():
            if value == 'int64':
                mask = (df[key] == '') | (pd.isna(df[key]))
                df.loc[mask, key] = 0
        return df.astype(dtype=(dtype_translation))

    @staticmethod
    def _get_contact_details_df(location_id: int, contacts_df: pd.DataFrame, ezy: EzyVetApi, set_data_types: callable):
        """
        Creates a DF containing the contact details.

        Args:
            location_id: The location ID in use.
            contacts_df: A dataframe containing the output from the EzyVet API contacts end point.
            ezy: An instance of the EzyVetApi class.

        Returns:

        """
        contact_ids = contacts_df['id'].values.tolist()
        contacts_detail_df = ezy.get_by_ids(location_id, 'v1', 'contactdetail', contact_ids,
                                            id_field='contact_id', dataframe_flag=True)
        contacts_detail_df.drop(columns=['id', 'active', 'created_at', 'modified_at'],
                                inplace=True)
        set_data_types(contacts_detail_df)
        return contacts_detail_df

    @staticmethod
    def _set_data_types(contacts_detail_df: pd.DataFrame) -> None:
        """
        Sets appropriate data types for numeric columns

        Args:
            contacts_detail_df: DataFrame with contact details.

        Returns:
            None
        """
        contacts_detail_df['contact_id'] = contacts_detail_df['contact_id'].astype(int)
        mask = contacts_detail_df['contact_detail_type_id'].str.isnumeric()
        contacts_detail_df.drop(contacts_detail_df[~mask].index, inplace=True)
        contacts_detail_df['contact_detail_type_id'] = contacts_detail_df['contact_detail_type_id'].astype(float)

    @staticmethod
    def _remove_existing_contacts(db: DBManager) -> None:
        """

        Args:
            location_id: The location ID in use.
            contacts_id_list: A list of ezyvet ID's in the current contacts pull.
            db:

        Returns:

        """
        # This function is used to delete any duplicate contacts in the db who might have modified their information.
        sql = f'''delete from gclick.contacts
                where id in (
                select id
                from (select id
                           , ezyvet_id
                           , rank() over (partition by ezyvet_id order by id desc) as rank
                    from gclick.contacts c) sq
                where rank > 1);'''
        db.execute_simple(sql)

    @staticmethod
    def _create_datetime_column(contacts_df: pd.DataFrame) -> None:
        """
        Converts date columns to proper datetime format.

        Args:
            contacts_df: A dataframe containing contact details.

        Returns:
            None
        """
        contacts_df['datetime_created'] = pd.to_datetime(contacts_df['contact_created'], unit='s')
        contacts_df['datetime_modified'] = pd.to_datetime(contacts_df['contact_modified'], unit='s')

    @staticmethod
    def _apply_contact_details(row: pd.Series, contacts_details_df: pd.DataFrame) -> pd.Series:
        """
        Apply function to merge certain contact details data into a row from the contacts_df.

        Args:
            row: A row from the contacts_df DataFrame.
            contacts_details_df: A dataframe containing contact details such as email address and phone number.

        Returns:
            A row with the phone number and email merged.
        """
        mask = (contacts_details_df['contact_id'] == row.ezyvet_id) & (
                contacts_details_df['contact_detail_type_id'] == 1)
        if mask.sum():
            row['email_address'] = contacts_details_df[mask]['value'].tolist()[0]

        mask = (contacts_details_df['contact_id'] == row.ezyvet_id) & (
                (contacts_details_df['contact_detail_type_id'] == 3) | (
                 contacts_details_df['contact_detail_type_id'] == 4))
        if mask.sum():
            phone_number = contacts_details_df[mask]['value'].tolist()[0]
            if len(phone_number) >= 10:
                try:
                    phone_number = phonenumbers.parse(phone_number, 'US').national_number
                except:
                    phone_number = None
            else:
                phone_number = None
            row['phone_number'] = phone_number

        return row


if __name__ == '__main__':
    t = Contacts(location_id=3)
    t.start()
