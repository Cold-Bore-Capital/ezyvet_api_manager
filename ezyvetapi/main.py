import json
import time
from datetime import datetime
from datetime import timedelta
from typing import Dict, Any, Union, Callable, List

import pandas as pd
import requests
from cbcdb import DBManager

from ezyvetapi.configuration_service import ConfigurationService


class EzyVetApi:
    """
    Queries the EzyVet API.
    """

    def __init__(self, test_mode=False):
        self._config = ConfigurationService(test_mode)
        # In test mode the self._db value will be set externally by the unit test.
        self._db = DBManager() if not test_mode else None

    '''
    # Section - Public Methods
    '''

    def get(self,
            location_id: int,
            endpoint_ver: str,
            endpoint_name: str,
            params: dict = None,
            headers: dict = None,
            dataframe_flag: bool = False) -> Union[pd.DataFrame, None, list]:
        """
        Main function to get api data
        Args:
            location_id: Location ID to operate on.
            endpoint_name: endpoint to query
            endpoint_ver: version of the endpoint to use.
            headers: headers to dict format
            params: params to dict format
            dataframe_flag: When set to true, method will return results in a Pandas DataFrame format.

        Returns:
            If dataframe_flag is False: A list of dicts containing the data.
            If dataframe_flag is True: A Pandas DataFrame containing the data.
        """
        endpoint = f'{endpoint_ver}/{endpoint_name}'
        db = self._db
        params = self._build_params(params)
        api_credentials = self._get_api_credentials(location_id, self._config.ezy_vet_api, 10, db,
                                                    self.get_access_token)
        headers = self._set_headers(api_credentials, headers)
        api_url = self._config.ezy_vet_api
        output = self._get_data_from_api(api_url=api_url,
                                         params=params,
                                         headers=headers,
                                         endpoint=endpoint,
                                         call_api=self._call_api)
        if dataframe_flag:
            if output:
                return pd.DataFrame(output)
            else:
                return None
        return output

    def get_date_range(self,
                       location_id: int,
                       endpoint_ver: str,
                       endpoint_name: str,
                       date_filter_field: str,
                       params: dict = None,
                       start_date: datetime = None,
                       end_date: datetime = None,
                       days: int = None,
                       dataframe_flag: bool = False) -> Union[None, list, pd.DataFrame]:
        """
        Retrieves records for a specified date range.

        Args:
            location_id: Location ID to operate on.
            endpoint_name: endpoint to query
            endpoint_ver: version of the endpoint to use.
            date_filter_field: Name of the field to filter on. I.E. "modified_date"
            start_date: Optional. Start of date range.
            end_date: Optional. End of date range
            params: Optional parameters to include in filter.
            days: Optional. A number of days to set the start or end date of the range.
            dataframe_flag: When set to true, method will return results in a Pandas DataFrame format.

        Returns:
            dataframe_flag = False: A list of dicts containing the data.
            dataframe_flag = True: A DataFrame containing the data.
        """
        date_filter_params = self._build_date_filter(date_filter_field, start_date, end_date, days)
        if isinstance(params, dict):
            params.update(date_filter_params)
        else:
            params = date_filter_params
        return self.get(location_id, endpoint_ver, endpoint_name, params, dataframe_flag=dataframe_flag)

    def get_by_ids(self,
                  location_id: int,
                  endpoint_ver: str,
                  endpoint_name: str,
                  ids: Union[int, List[int]],
                  params: dict = None,
                  dataframe_flag: bool = False) -> Union[list, pd.DataFrame]:
        """
        Get's records from API by ID or list of ID's.

        Args:
            location_id: Location ID to operate on.
            endpoint_name: endpoint to query
            endpoint_ver: version of the endpoint to use.
            ids: Either an ID number as an int, or a list of IDs i.e. [24,56,21,67]
            params: Optional parameters to include in filter.
            dataframe_flag: When set to true, method will return results in a Pandas DataFrame format.

        Returns:
            dataframe_flag = False: A list of dicts containing the data.
            dataframe_flag = True: A DataFrame containing the data.
        """
        df = pd.DataFrame()
        if isinstance(ids, int):
            ids = [ids]
        for x in range(0, len(ids), 100):
            # This is just used for the print output.
            end = {x + 100} if len(ids) > 100 else len(ids)
            print(f'Getting records from {endpoint_ver}/{endpoint_name} IDs: {x}: {end} of {len(ids)}.')
            if params:
                params['id'] = {'in': ids[x: x + 100]}
            else:
                params = {'id': {'in': ids[x: x + 100]}}
            df_batch = self.get(location_id, endpoint_ver, endpoint_name, params, dataframe_flag=True)
            df = pd.concat([df, df_batch])
        if dataframe_flag:
            return df
        else:
            return df.to_dict('records')

    @staticmethod
    def get_access_token(api_url: str, api_credentials: Dict[str, Union[str, int]]) -> str:
        """
        Requests an access token from the EzyVet API

        Args:
            api_url: URL to the API
            api_credentials: A dict containing all API credentials

        Returns:
            A string containing an access token. Any prior access tokens will be invalidated when a new access token
            is retrieved.

        """
        '''
        Step 1: Get bearer token for authentication
        '''
        # Setup the parameters
        url = f"{api_url}v1/oauth/access_token"
        # This will be the body of the request
        payload = {
            'partner_id': api_credentials['partner_id'],
            'client_id': api_credentials['client_id'],
            'client_secret': api_credentials['client_secret'],
            'grant_type': 'client_credentials',
            'scope': 'read-receiveinvoice,read-diagnosticrequest,read-tagcategory,read-systemsetting,read-contactdetailtype,read-shelteranimalbooking,read-stocktransaction,read-webhookevents,read-presentingproblem,read-purchaseorder,read-country,read-productsupplier,read-animal,read-payment,read-consult,read-presentingproblemlink,read-ledgeraccount,read-diagnostic,read-therapeutic,read-diagnosticresultitem,read-address,read-species,read-plan,read-purchaseorderitem,read-wellnessplanmembership,read-vaccination,read-productminimumstock,read-transaction,read-integrateddiagnostic,read-stockadjustmentitem,read-wellnessplanmembershipstatusperiod,read-tag,read-invoice,read-contact,read-sex,read-animalcolour,read-batch,read-assessment,read-healthstatus,read-breed,read-invoiceline,read-wellnessplanbenefit,read-receiveinvoiceitem,read-separation,read-priceadjustment,read-user,read-resource,read-prescriptionitem,read-prescription,read-physicalexam,read-billingcredit,read-appointmentstatus,read-paymentmethod,read-tagname,read-taxrate,read-communication,read-wellnessplanmembershipoption,read-stockadjustment,read-appointmenttype,read-productgroup,read-webhooks,read-product,read-operation,read-history,read-diagnosticresult,read-paymentallocation,read-attachment,read-contactdetail,read-productpricing,read-contactassociation,read-wellnessplanbenefititem,read-appointment,read-jobqueue,read-wellnessplan'
        }
        # Note this is a post type, not a get. That allows for variables sent in the body.
        res = requests.post(url, data=payload)
        data = res.json()
        access_token = data['access_token']
        return access_token

    def get_translation(self, location_id: int, endpoint_ver: str, endpoint_name: str) -> dict:
        """
        Returns a translation dictionary to convert an id number into a string value

        Args:
            location_id: Location ID to query.
            endpoint_ver: version of the endpoint, v1 or v2
            endpoint_name: Name of the endpoint ex. animals

        Returns:
            Returns a dictionary in the format {1:'translation_name'}
        """
        df = self.get(location_id, endpoint_ver, endpoint_name, dataframe_flag=True)
        # df.to_dict(orient='split')
        translation = {int(x['id']): x['name'] for x in df.to_dict(orient='records')}
        return translation

    '''
    # Section - Private Methods
    '''

    @staticmethod
    def _build_params(params: Dict[str, Union[str, int]] = None) -> Dict[str, Union[str, int]]:
        """
        Builds a dictionary with query parameters and renders any dict or list values to a JSON string

        Args:
            params: A dictionary containing parameters.

        Returns:
            A dictionary containing parameters with keys rendered to JSON string if a dict or a list
        """
        if params:
            params['limit'] = 200
        else:
            params = {'limit': 200}
        output = {}
        for key, value in params.items():
            if isinstance(value, dict) or isinstance(value, list):
                output[key] = json.dumps(value)
            else:
                output[key] = value
        return output

    @staticmethod
    def _get_api_credentials(location_id: int,
                             api_url: str,
                             cache_limit: int,
                             db: DBManager,
                             get_access_token: Callable[[str, dict], str]) -> Dict[str, Any]:
        """
        Retrieves the API credentials for a location.

        Args:
            location_id: The ID to retrieve.
            api_url: Path to API.
            cache_limit: Number of minutes before access_token expires. The system will check the current time minus
                         the cache_limit value. If the access token is older, a new one will be requested.
            db: Instance of DBManager
            get_access_token: Instance of the _get_access_token() method.

        Returns:
            A dictionary containing the credentials.
        """
        schema = db.db_schema
        sql = f'select *, now() as system_time from {schema}.ezy_vet_credentials where location_id = %s'
        params = [location_id]
        res = db.get_sql_list_dicts(sql, params)
        if len(res) == 0:
            raise MissingEzyVetCredentials(f"No database record was found for location ID {location_id}")
        credentials = res[0]
        system_time = credentials['system_time'].replace(tzinfo=None)
        # Check if access_token is older than cache limit.
        expire_date = system_time - timedelta(minutes=cache_limit)
        if not credentials['access_token'] or expire_date > credentials['access_token_create_time']:
            credentials['access_token'] = get_access_token(api_url, credentials)
            sql = f'update {schema}.ezy_vet_credentials set access_token=%s, access_token_create_time=%s where location_id = %s'
            params = [credentials['access_token'], system_time, location_id]
            db.execute_simple(sql, params)
        return credentials

    @staticmethod
    def _set_headers(api_credentials: dict, headers: Union[dict, bool] = None) -> Dict[str, str]:
        """
        Sets the authorization headers for API call.

        Args:
            api_credentials: A dict containing the API credentials.
            headers: An optional pre-set header dict. If a value is set, the Authorization header will just be added.
                     If no value is set, the dict will be created with only the Authorization headers.

        Returns:
            A dict containing the authorization headers, along with any optional user specific headers.
        """
        if headers:
            headers['Authorization'] = f'Bearer {api_credentials["access_token"]}'
        else:
            headers = {'Authorization': f'Bearer {api_credentials["access_token"]}'}
        return headers

    @staticmethod
    def _get_data_from_api(api_url: str,
                           params: Dict[str, Any],
                           headers: Dict[str, str],
                           endpoint: str,
                           call_api: Callable[[str, dict, dict], dict]) -> Union[bool, list]:
        """
        Retrieves data from the EzyVet API.

        This method will call the API and get an initial set of data, along with metadata about the full result set. If
        more than one page, the method will continue to call the API getting the next batch of data until complete.

        Args:
            api_url: EzyVet URL
            params: Query parameters
            headers: Headers for EzyVet authentication.
            endpoint: The name of the endpoint in the format v1/name. Example, v2/appointment.
            call_api: Instance of _call_api method

        Returns:
            A dictionary containing the requested data.
        """
        url = f'{api_url}{endpoint}'
        data = call_api(url, headers, params)

        if 'items_total' in data['meta']:
            record_count = data['meta']['items_total']
        else:
            print(f'items_total not found \n {data}')
            return False
        pages = int(data['meta']['items_page_total'])
        print(f'Returned {record_count} records over {pages} pages.')
        output = []
        if len(data['items']):
            output += data['items']
        else:
            print('No results returned')
            return False
        if pages > 1:
            # Get the next page of data. EzyVet will only return 10 records per page so a pagination call needs to be
            # made.
            for page_num in range(2, pages + 1):
                # Add a "page" variable to the params
                params['page'] = page_num
                data = call_api(url, headers, params)
                page_item_count = data['meta']['items_page_size']
                print(f'Page {page_num} has {page_item_count} records.')
                output += data['items']
        output = [x[endpoint.split('/')[1]] for x in output]

        return output

    def _call_api(self, url: str, headers: dict, params: dict) -> dict:
        """
        Initiates connection to API.

        Args:
            url: URL with endpoint to query.
            headers: Auth headers
            params: Query parameters.

        Returns:
            A dict containing the query results.
        """
        res = requests.get(url, headers=headers, params=params)
        if res.status_code != 200:
            sleep_time = self._config.server_retry_sleep_time
            print(f'Server replied with status code {res.status_code}. Retrying in {sleep_time / 60} minutes. ')
            time.sleep(sleep_time)
            res = requests.get(url, headers=headers, params=params)
            if res.status_code != 200:
                print(res.text)
                raise EzyVetAPIError(f'Api returned non-200 status code. res: {res.status_code} '
                                     f'res.text: {res.text}')
        data = res.json()
        return data

    @staticmethod
    def _build_date_filter(filter_field: str, start_date: datetime = None, end_date: datetime = None,
                           days: int = 0) -> dict:
        """
        Creates a date filter set returned as a dict.

        End date will always be inclusive of the full day if a time is not set.

        Behavior:
        a. start_date and end_date set: If both a start_date and end_date are provided, the method will create a between
        filter.

        b. start_date, end_date = None: A filter will be created for any value greater than the start date.

        c. start_date = None, end_date set: A filter will be created for any value less than the end date.

        d. If either a start date, or an end date are set plus days, a date range will be created. For example, if
           start_date is set with 5 days, a date range spanning the start date to five days in the future will be
           created.

        Args:
            filter_field: Name of the field to filter on. I.E. "modified_date"
            start_date: Optional. Start of date range.
            end_date: Optional. End of date range
            days: Optional. A number of days to set the start or end date of the range.

        Returns:
            A dictionary containing the appropriate date range.
        """
        # Check to make sure that at least a start or end date exists.
        output = {filter_field: None}
        if end_date:
            # For end date to be inclusive, it must have a time at the end of the day.
            # If end_date has no time information, assume full day and correct here.
            time_test = end_date.hour + end_date.minute + end_date.second
            if time_test == 0:
                end_date = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)

        if start_date and not end_date:
            start_timestamp = time.mktime(start_date.timetuple())
            if days:
                end_date = start_date + timedelta(days=days)
                end_timestamp = time.mktime(end_date.timetuple())
                return {filter_field: {'gt': start_timestamp, 'lte': end_timestamp}}
            else:
                return {filter_field: {'gt': start_timestamp}}
        elif end_date and not start_date:
            end_timestamp = time.mktime(end_date.timetuple())
            if days:
                start_date = end_date - timedelta(days=days)
                start_timestamp = time.mktime(start_date.timetuple())
                return {filter_field: {'gt': start_timestamp, 'lte': end_timestamp}}
            else:
                return {filter_field: {'lt': end_timestamp}}
        elif start_date and end_date:
            if days:
                raise StartEndAndDaysSet('You cannot set the start date, end date, and days.')
            else:
                start_timestamp = time.mktime(start_date.timetuple())
                end_timestamp = time.mktime(end_date.timetuple())
                return {filter_field: {'gt': start_timestamp, 'lte': end_timestamp}}
        else:
            raise MissingStartAndEndDate("You must set either a start or end date for build_date_filter.")


class MissingStartAndEndDate(Exception):
    pass


class StartEndAndDaysSet(Exception):
    pass


class EzyVetAPIError(requests.exceptions.HTTPError):
    pass


class MissingEzyVetCredentials(Exception):
    pass


if __name__ == '__main__':
    e = EzyVetApi()
    e.get(2, None, 'v2/appointment', None, None)
