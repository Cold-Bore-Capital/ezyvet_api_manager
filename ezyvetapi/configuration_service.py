import os
from typing import Union, Any


class ConfigurationService:
    """
    Configuration service for app.

    Handles communications between OS environ variables and requests from app.
    """

    def __init__(self, test_mode: bool = False):
        """
        Init Function.

        Args:
            test_mode: If set to true, config service will return mockup values instead of env variables.
        """
        self._test_mode = test_mode

    '''
    EzyVet API
    
    Env's:
        EZY_VET_API=path_to_the_api 
        SERVER_RETRY_SLEEP_TIME=how long to sleep during a retry. 
        EZYVET_API_FAIL_COUNT=How many times to retry when getting a 429 failure. 
    '''

    @property
    def ezy_vet_api(self):
        return self._check_if_value_exists('EZY_VET_API', None, True, 'localhost')

    @property
    def server_retry_sleep_time(self):
        return int(self._check_if_value_exists('SERVER_RETRY_SLEEP_TIME', None, False, default_value=30))

    @property
    def api_fail_count(self):
        return int(self._check_if_value_exists('EZYVET_API_FAIL_COUNT', None, False, default_value=5))

    '''
    # End Properties
    '''

    def _check_if_value_exists(self,
                               key_name: str,
                               assigned_value: Any = None,
                               error_flag: bool = None,
                               test_response: Any = None,
                               default_value: Any = None,
                               legacy_key_name: str = None) -> Union[None, str]:
        """
        Checks if an env value is set for the key. Optionally raises an error if value is not set.

        Args:
            key_name: The name of the environment variable.
            assigned_value: A value assigned during the __init__ process. This value overrides any env value.
            error_flag: If set to True and the following conditions exist, an error will be raised.
                       Conditions: 1.) The env value was not set, 2.) and the assigned_value is not set.
            test_response: Value to return if in test mode.
            default_value: A value to return if no other values are set. Error flag must be set to False or an error will
                           be raised.
            legacy_key_name: Supports a second legacy key. A warning about the legacy key will be given asking the user
                             to update to the new key.

        Returns:
            The value or None if the value is empty.
        """
        # Check if the value was assigned in the constructor (__init__)
        if assigned_value:
            return assigned_value

        # If in test mode, return the test response. A default value will override this.
        if self._test_mode and not default_value:
            return test_response

        env_value = os.environ.get(key_name)
        if legacy_key_name:
            legacy_env_value = os.environ.get(legacy_key_name)
        else:
            legacy_env_value = None

        if env_value:
            # If the value is set, simply return it.
            return env_value

        elif legacy_env_value:
            print(f'{legacy_key_name} has been deprecated. Please update your env file to use {key_name}')
            return legacy_env_value

        elif default_value:
            if error_flag:
                raise ErrorFlagTrue('The error flag must be set to false if a default value is set.')
            return default_value

        elif error_flag:
            # If the value is not set and error_msg is not None, raise error.
            raise MissingEnviron(key_name)

        # If no error was set, and the value isn't set, return None.
        return None


class MissingEnviron(Exception):
    """Raised when a required environment variable is missing"""

    def __init__(self, env_var_name):
        self.env_var_name = env_var_name
        self.message = f'The required environment variable {self.env_var_name} is missing'
        super().__init__(self.message)


class ErrorFlagTrue(Exception):
    pass
