# EzyVet API Manager 

## Usage

Call the `get_api()` method:

    get_api(location_id=2,
            endpoint='v2/appointment',
            params={'from': '2020-01-01', 'to': '2020-02-01')

The `get_api` method will return a dictionary with the results. If the results span multiple pages, the system will 
consolidate into a single response. 

## Configuration 

The EzyVet API Manager class expects a certain set of environment variables to exist, and a database table containing 
authentication information for each location. 

### Environment variables

The following environment variables must be present. If using a docker container, make sure to import the .env file into 
the container. 

Example of required environment variables. The database variables are the same as required by the cbcdb class.

    # EzyVetApi 
    EZY_VET_API=https://api.ezyvet.com/
    SERVER_RETRY_SLEEP=120

    # DB
    USE_SSH=False
    REMOTE_BIND_PORT=5436
    DB_NAME=test
    DB_USER=test
    DB_PASSWORD=test
    DB_SCHEMA=data
    DB_HOST=localhost

### API Credentials Table

A table in the following structure must exist in the schema specified by `DB_SCHEMA`. In the example below, `data` schema
is used:

    create table data.ezy_vet_credentials
    (
        id                       serial
            constraint ezy_vet_credentials_pk
                primary key,
        name                     varchar(32),
        location_id              int,
        partner_id               char(64),
        client_id                char(32),
        client_secret            char(60),
        access_token             char(64),
        access_token_create_time timestamp
    );