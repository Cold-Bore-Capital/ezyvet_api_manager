import json
import time
from datetime import timedelta
from typing import Dict, Any, Union, Callable

import pandas as pd
import requests
from cbcdb import DBManager

from ezyvetapi.configuration_service import ConfigurationService


class EzyVetApi:
    """
    Queries the EzyVet API.
    """

    def __init__(self):
        self._config = ConfigurationService()

    '''
    # Section - Public Methods
    '''

    def get(self, location_id: int, endpoint_ver: str, endpoint_name: str, params: dict = None, headers: dict = None):
        """
        Main function to get api data
        Args:
            location_id: Location ID to operate on.
            endpoint_name: endpoint to query
            endpoint_ver: version of the endpoint to use.
            headers: headers to dict format
            params: params to dict format

        Returns:
            A list of dicts containing the data
        """
        endpoint = f'{endpoint_ver}/{endpoint_name}'
        db = DBManager()
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
        return output

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

    def get_endpoint_df(self, location_id: int, endpoint_ver: str, endpoint_name: str, params=None) -> pd.DataFrame:
        """
        Returns the results of an API query as a dataframe.

        Args:
            location_id: Location ID to query
            endpoint_name: Name of endpoint. I.E. appointments.
            endpoint_ver: End point version in format `v2`
            params: Optional set of parameter filters for the query.

        Returns:

        """
        res = self.get(location_id=location_id,
                       endpoint_ver=endpoint_ver,
                       endpoint_name=endpoint_name,
                       params=params)
        if not res:
            return False

        df = pd.DataFrame(res)
        return df

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
        df = self.get_endpoint_df(location_id, endpoint_ver, endpoint_name)
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
                           params: Dict[str, Union[str, str]],
                           headers: Dict[str, str],
                           endpoint: str,
                           call_api: callable) -> Union[bool, list]:
        """
        Retrieves data from the EzyVet API.

        This method will call the API and get an initial set of data, along with metadata about the full result set. If
        more than one page, the method will continue to call the API getting the next batch of data until complete.

        Args:
            api_url: EzyVet URL
            params: Query parameters
            headers: Headers for EzyVet authentication.
            endpoint: The name of the endpoint in the format v1/name. Example, v2/appointment.

        Returns:
            A dictionary containing the requested data.
        """
        url = f'{api_url}{endpoint}'
        data = call_api(headers, params, url)

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
                data = call_api(headers, params, url)
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


class EzyVetAPIError(requests.exceptions.HTTPError):
    pass


if __name__ == '__main__':
    e = EzyVetApi()
    e.get(2, None, 'v2/appointment', None, None)
