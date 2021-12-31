from datetime import datetime
from typing import List

import pandas as pd

from ezyvetapi.main import EzyVetApi
from ezyvetapi.models.model import Model


class Resources(Model):

    def __init__(self, location_id, db=None):
        super().__init__(location_id, db)

    def get_resources(self, start_date: datetime = None, end_date: datetime = None,
                      ids: List[int] = None) -> pd.DataFrame:

        print('Starting resource load')
        db = self.db
        ezy = EzyVetApi()
        if (not start_date or not end_date) and not ids:
            start_date, end_date = self.get_most_recent('resources', 'modified_at')
        params = {'active': True}
        resource_df = ezy.get_date_range(self.location_id, 'v1', 'resource', 'modified_at', params=params,
                                         start_date=start_date, end_date=end_date, dataframe_flag=True)

        if isinstance(resource_df, pd.DataFrame):
            return resource_df


if __name__ == "__main__":
    # for location in [6]:
    location = 3
    app = Resources(location_id=location)
    app.get_resources()
