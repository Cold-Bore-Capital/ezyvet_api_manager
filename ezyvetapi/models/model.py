from datetime import datetime
from typing import Tuple, Union, Any, Dict
import numpy as np
import pandas as pd
import pytz
from cbcdb import DBManager

from ezyvetapi.configuration_service import ConfigurationService


class Model:

    def __init__(self, location_id=None, db=None):
        self.location_id = location_id
        self._config = ConfigurationService()
        self.db = db if db else DBManager()

    def get_most_recent(self, table_name: str, modified_field: str,
                        type_field_filter: str = None, type_field_value: Any = None) -> Tuple[
        Union[None, datetime], datetime]:
        """
        Gets the most recent entry from specified table / modified field and returns a start_date and end_date.

        Args:
            table_name: Name of the table to query
            modified_field: The field containing the modified date.
            type_field_filter: An optional filter field. For example, in the appointments table there are both shelter
            animal bookings and medical / grooming appointments. A field is needed to identify between them.
            type_field_value: The value to match in the filter.

        Returns:
            (datetime to search from as start_date, datetime to search to as end_date)
        """
        params = [self.location_id]
        type_field_str = ''
        if type_field_filter:
            type_field_str = f'and {type_field_filter} = %s'
            params.append(type_field_value)

        sql = f'SELECT MAX({modified_field}) FROM gclick.{table_name} WHERE location_id = %s {type_field_str}'
        res = self.db.get_sql_single_item_list(sql, params)
        now_utc = datetime.utcnow()
        start_date = None
        end_date = now_utc
        if res[0]:
            start_date = res[0]
            start_date = datetime.utcfromtimestamp(start_date)
        return start_date, end_date

    @staticmethod
    def remove_out_of_limits_values(df: pd.DataFrame, max_limits: dict) -> pd.DataFrame:
        for key, limit in max_limits.items():
            mask = (df[key] > limit)
            df.loc[mask, key] = np.nan

        return df

    def _get_division_dict(self) -> Dict[int, int]:
        """
        Gets a translation dictionary to convert EzyVet ownership_id fields into division ID's

        Returns:
            A dictionary in the format {EzyVet sep ID: division ID)  example {7, 1102)
        """
        sql = f'SELECT division_id, ezy_vet_sep_id FROM bi.divisions_translation WHERE location_id = %s'
        division_id_list = self.db.get_sql_list_dicts(sql, [self.location_id])
        division_id_dict = {x['ezy_vet_sep_id']: x['division_id'] for x in division_id_list}
        return division_id_dict

    def _assign_division_location_id(self, df: pd.DataFrame, location_id: int) -> None:
        """
        Assign the division and location ID columns within the dataframe.

        Args:
            df: Dataframe to add division info to.
            location_id: Location ID in process.

        Returns:
            None
        """
        division_id_dict = self._get_division_dict()
        df['division_id'] = df['ownership_id'].apply(lambda x: division_id_dict[int(x)])
        df['location_id'] = location_id

    @staticmethod
    def _create_datetime_column(df: pd.DataFrame) -> None:
        """
        Converts unix timestamp fields to true datetime columns.

        Args:
            df: A dataframe with the required columns present

        Returns:
            None.
        """
        df['datetime_created'] = pd.to_datetime(df['created_at'], unit='s')
        df['datetime_modified'] = pd.to_datetime(df['modified_at'], unit='s')
        df['datetime_start_at'] = pd.to_datetime(df['start_at'], unit='s')