"""
    This routine will poll the runtime and settings information from the associated list of Ecobee
    thermostats for Epiphany Catholic Church, and store the results into a SQLite database.
    Detailed logging is available of the process and can be customized by changing the logging level
    below (default=DEBUG) and destination (default = "ECC ecobee.log"; uncomment line to set to "None"
    to direct to the console instead).

    The core Ecobee routines (pycobee) come from a library originally written by  @sfanous at
    https://github.com/sfanous/Pyecobee;  it is well documented here along with all of the object
    definitions, and Python getter / setter functions.  Unfortunately, it doesn't seem to be
    maintained any longer, which is a problem as new fields are added to the object definitions /
    Ecobee API calls.  There are a number of forks for this that DO seem to be maintained; the
    one I've chosen to use is by Daniel Sullivan (mumblepins), here:  https://github.com/mumblepins/Pyecobee.

    To install the library in Python, I used the following PIP command after downloading the zipped
    archive in the current directory:
                    pip install ./Pyecobee-mumblepins.zip -v        (or whatever the downloaded archive is named)

    The application contains extensive error handling to try to catch and handle the most common errors
    that may occur.  In most cases where an unhandled error DOES slip by, simply re-running the app
    will generally work without causing additional problems with the data.

            Written by DK Fowler ... 1-Oct-2019
"""

from datetime import datetime
from datetime import timedelta
import pytz
import shelve
import json
import os
import sys

from pyecobee import *

import sqlite3
from sqlite3 import Error

# Define version
eccpycobee_version  = "01.00"
eccpycobee_date     = "28-Oct-2019"

# Set up logging...change as appropriate based on implementation location and logging level
log_file_path = 'ECC ecobee.log'
# log_file_path = None  # To direct logging to console instead of file
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s: %(name)s: line: %(lineno)d %(message)s"
)
logger = logging.getLogger('pyecobee')

# Location of database file...change as appropriate based on implementation location
ECCEcobeeDatabase = r"C:\Users\Keith\Downloads\ECC Ecobee\ECCEcobee.db"

# Location of the authorization file w/ tokens
ECCAuthorize = r"C:\Users\Keith\Downloads\ECC Ecobee\ECCEcobee Tkn"

# Location of the JSON revision interval file
json_interval_file = r"C:\Users\Keith\PycharmProjects\ECC Ecobee\Python Ecobee\ecobee_therm_interval.conf"

"""
    The ecobee API is based on extensions to the OAuth 2.0 framework. Authorization for a given API
    call requires several initial steps:
        1)  Defining the new application, done manually by an administrator on the Ecobee portal.
            This results in the issuance of an application key.
        2)  An authorization, providing a scope which defines whether the application "scope" will be
            read, write or both access.  The application key from above is used for the authorization
            request, and if successful, results in a 4-digit PIN (used here, though there are 
            other methods provided).
        3)  An app registration, done manually by the administrator on the Ecobee portal.  The admin provides 
            the PIN from the authorization request previously.  Subsequent calls to the authorization API 
            will not be successful until the validation step is performed.  The PIN has a set duration and 
            will expire after a defined timeframe, so this step is time-sensitive.
        4)  Token issuance.  Valid access tokens are required for all further calls to the Ecobee API.
            Access tokens have a specified life, which means they will expire after a set amount of time.
            Requests for token issuance include an authorization token from the authorization step above.
            If the token issuance request is successful, access and refresh tokens are provided which
            have set expiration timeframes.
        5)  Refreshing tokens.  As noted in the previous step, the access tokens used for all further API
            requests expire after a set time.  If a subsequent API request fails due to token expiration,
            a new set of access/refresh tokens must be requested.  A refresh request must include the
            valid (non-expired) refresh token from the previous token issuance.
            
            The last (valid) set of authorization, access, and refresh tokens are stored by this application
            in a Python Shelve local database.  Though not secure, the scope of this application is R/O,
            so in the unlikely event that the tokens are compromised, the use is limited to reading data
            from the Ecobee thermostats.
                
"""

# The following is a dictionary containing the defined thermostat objects used by the library
thermostat_object_dict = {'settings': 'Settings', 'runtime': 'Runtime',
                          'extended_runtime': 'ExtendedRuntime', 'electricity': 'Electricity',
                          'location': 'Location', 'technician': 'Technician',
                          'utility': 'Utility', 'management': 'Management', 'weather': 'Weather',
                          'program': 'Program', 'house_details': 'HouseDetails',
                          'oem_cfg': 'ThermostatOemCfg', 'notification_settings': 'NotificationSettings',
                          'privacy': 'ThermostatPrivacy', 'version': 'Version',
                          'security_settings': 'SecuritySettings',
                          # Following are list-defined objects
                          'action': 'Action', 'alert': 'Alert', 'climate': 'Climate',
                          'demand_management': 'DemandManagement', 'demand_response': 'DemandResponse',
                          'device': 'Device', 'electricity_device': 'ElectricityDevice',
                          'electricity_tier': 'ElectricityTier', 'equipment_setting': 'EquipmentSetting',
                          'event': 'Event', 'function': 'Function', 'general_setting': 'GeneralSetting',
                          'group': 'Group', 'hierarchy_privilege': 'HierarchyPrivilege',
                          'hierarchy_set': 'HierarchySet', 'hierarchy_user': 'HierarchyUser',
                          'limit_setting': 'LimitSetting', 'output': 'Output', 'page': 'Page',
                          'remote_sensor': 'RemoteSensor', 'remote_sensor_capability': 'RemoteSensorCapability',
                          'runtime_sensor_metadata': 'RuntimeSensorMetadata', 'sensor': 'Sensor',
                          'state': 'State', 'status': 'Status', 'thermostat': 'Thermostat', 'user': 'User',
                          'weather_forecast': 'WeatherForecast'}

# The following is a dictionary containing the SQLite db table names used for storing the snapshot records
# Note the name of the table thermRuntime; this is distinguished from the table runtime, which stores
# the historical 5-minute interval runtime data for each thermostat.  thermRuntime stores the snapshot data
# obtained through the thermostat details API call.
#       Temporary table definitions to limit data retrieval until lists parsing added:
SQLite_table_dict = {'settings': 'thermSettings', 'location': 'thermLocation',
                     'house_details': 'thermHouseDetails', 'version': 'thermVersion'}
'''
SQLite_table_dict = {'settings': 'thermSettings', 'runtime': 'thermRuntime',
                     'extended_runtime': 'thermExtendedRuntime', 'electricity': 'thermElectricity',
                     'location': 'thermLocation', 'technician': 'thermTechnician',
                     'utility': 'thermUtility', 'management': 'thermManagement', 'weather': 'thermWeather',
                     'program': 'thermProgram', 'house_details': 'thermHouseDetails',
                     'oem_cfg': 'thermOemCfg', 'notification_settings': 'thermNotificationSettings',
                     'privacy': 'thermPrivacy', 'version': 'thermVersion',
                     'security_settings': 'thermSecuritySettings'}
'''

def main():
    # Global variables used for informational logging
    global dup_update_cnt_total
    global dup_update_cnt_this_thermostat
    global blank_rec_cnt_total
    global blank_rec_cnt_this_thermostat
    global db_table_recs_written

    dup_update_cnt_total = 0
    dup_update_cnt_this_thermostat = 0
    blank_rec_cnt_total = 0
    blank_rec_cnt_this_thermostat = 0
    snapshot_recs_written_total = 0
    db_table_recs_written = {}  # Initialize a dictionary for storing recs written by table for snapshots

    print(F"*** Initiating Ecobee thermostat data retrieval ***")
    logger.info(F"*** Initiating Ecobee thermostat data retrieval ***")
    print(F"*** ECC Ecobee data retrieval version {eccpycobee_version}, {eccpycobee_date} ***")
    logger.info(F"*** ECC Ecobee data retrieval version {eccpycobee_version}, {eccpycobee_date} ***")

    # Attempt to open the credentials / authorization file and read contents
    try:
        pyecobee_db = shelve.open(ECCAuthorize,  # read the stored authorization file
                                  writeback=True,
                                  protocol=4)  # protocol 4 is the latest pickle version, > Python 3.4
        logger.debug(F"Shelve data structure:  Keys: {len(pyecobee_db)}")
        logger.debug(F"...elements:  {list(pyecobee_db)}")
        auth_list = list(pyecobee_db)
        for pelement in auth_list:
            logger.debug(F"...contents:  {pelement}: {pyecobee_db.get(pelement)}")

        # Test to see if the read credentials are empty; if so, initialize the Ecobee service object.
        # This would typically happen at first run when there are no stored credentials.
        if len(pyecobee_db) == 0:
            app_key = "Vy72kTcU63iWryQ9YG5T4faruTFKxIwz"  # default, if not read from shelve
        else:
            app_key = pyecobee_db['application_key']

        # initialize an Ecobee service object
        ecobee_service = EcobeeService(thermostat_name='',
                                       application_key=app_key,
                                       scope=Scope.SMART_READ)
        logger.info(ecobee_service.pretty_format())

        # If we have a value for the authorization code, access and refresh tokens in the stored credentials,
        # assign these to the appropriate fields in the EcobeeService object
        if 'authorization_token' in pyecobee_db:
            ecobee_service.authorization_token = pyecobee_db['authorization_token']
        if 'access_token' in pyecobee_db:
            ecobee_service.access_token = pyecobee_db['access_token']
        if 'refresh_token' in pyecobee_db:
            ecobee_service.refresh_token = pyecobee_db['refresh_token']

    except KeyError as e:
        logger.error(F"Missing or invalid API key while attempting to initialize Ecobee service object.")
        logger.error(F"Ecobee service return error:  {e}")
        sys.exit(1)  # Not much point in continuing if we don't have a valid application key
    finally:
        pyecobee_db.close()

    # Test for no authorization token present; this would typically happen at first run where no
    # access credentials are stored
    if not ecobee_service.authorization_token:
        logger.info(F"No authorization token found...requesting...")
        authorize(ecobee_service)

    # Test for no access token present; this would typically happen at first run where no access
    # credentials are stored, or where authorization has just occurred
    if not ecobee_service.access_token:
        logger.info(F"No access token found...requesting...")
        request_tokens(ecobee_service)

    # Request the thermostat summary, which contains brief information about each thermostat,
    # including the last-reported revision interval(s) used later for polling runtime data
    try:
        thermostat_summary_response = ecobee_service.request_thermostats_summary(selection=Selection(
            selection_type=SelectionType.REGISTERED.value,
            selection_match='',
            include_equipment_status=True))
    except EcobeeApiException as e:
        # Check for error code 14, which indicates the access token has expired; if so, try to refresh
        if e.status_code == 14:
            logger.error(F"Ecobee access token expired...requesting token refresh")
            try:
                refresh_tokens(ecobee_service)
            except EcobeeApiException as e:
                logger.error(F"Error attempting to refresh Ecobee access token...{e}")

                logger.debug("Refreshed access token:  " + str(ecobee_service.access_token))
                logger.debug("Refreshed refresh token: " + str(ecobee_service.refresh_token))
                sys.exit(1)
            try:
                thermostat_summary_response = ecobee_service.request_thermostats_summary(selection=Selection(
                    selection_type=SelectionType.REGISTERED.value,
                    selection_match='',
                    include_equipment_status=True))
            except EcobeeException as e:
                logger.error(F"Error occurred during second attempt to retrieve thermostat summary...")
                logger.error(F"...Ecobee API error:  {e}")
                sys.exit(1)
    # except (EcobeeRequestsException, EcobeeHttpException, requests.exceptions.ConnectionError,
    #         requests.exceptions.RequestException, urllib3.exceptions.NewConnectionError,
    #        urllib3.exceptions.MaxRetryError, urllib.error.HTTPError, socket.gaierror, OSError) as e:
    # except EcobeeRequestsException as e:
    #    logger.error(F"Ecobee requests exception occurred...{e}")
    # except EcobeeHttpException as e:
    #    logger.error(F"Ecobee HTTP exception occurred...{e}")
    except Exception as e:  # Handle no connection error
        logger.error(F"Request error occurred during attempt to retrieve Ecobee thermostat summary...")
        logger.error(F"...error:  {e}")
        conn_err_msg = "'ConnectionError' object has no attribute 'message'"
        if conn_err_msg in e.__str__():
            logger.error(F"...site not responding, or Internet connection down?")
        sys.exit(1)

    sum_err_cnt = 0
    sum_err_occurred = True  # Falsely assume error here to initiate loop
    while sum_err_occurred and sum_err_cnt <= 3:
        sum_err_occurred = False  # Reset here to assume success
        # Request the thermostat details; this includes the "first connected" date/time for each
        # Only set the include options you need to True.
        # Template for selection types for thermostats:
        selection = Selection(selection_type=SelectionType.REGISTERED.value, selection_match='', include_alerts=True,
                              include_device=True, include_electricity=False, include_equipment_status=True,
                              include_events=False, include_extended_runtime=True, include_house_details=True,
                              include_location=True, include_management=False, include_notification_settings=True,
                              include_oem_cfg=False, include_privacy=False, include_program=True,
                              include_reminders=True,
                              include_runtime=True, include_security_settings=False, include_sensors=True,
                              include_settings=True, include_technician=False, include_utility=False,
                              include_version=True,
                              include_weather=True)

        try:
            thermostat_response = ecobee_service.request_thermostats(selection)
        except EcobeeApiException as e:
            if e.status_code == 14:  # Authentication error occurred
                logger.error(F"Ecobee access token expired while requesting thermostat details..."
                             F"requesting token refresh")
                sum_err_cnt += 1
                sum_err_occurred = True
                logger.error(F"...thermostat details API request, attempt {sum_err_cnt}")
                print(F"...error on thermostat details API request, attempt {sum_err_cnt}")
                try:
                    refresh_tokens(ecobee_service)
                    logger.info(F"Ecobee access tokens refreshed...continuing processing")
                    print(F"Ecobee access tokens refreshed...continuing processing")
                except EcobeeException as e:
                    logger.error(F"...error occurred while requesting token refresh; exiting...")
                    sys.exit(1)
        except EcobeeException as e:  # Some other error occurred
            logger.error(F"Error occurred while requesting thermostat(s) details...")
            logger.error(F"...Ecobee exception:  {e}")
            sys.exit(1)
    else:
        if sum_err_occurred and sum_err_cnt > 3:
            logger.error(F"Exceeded maximum retries while attempting to retrieve Ecobee thermostat details")
            logger.error(F"...aborting (try again later)")
            print(F"...maximum retry attempts exceeded, aborting (try again later)")
            sys.exit(1)

    logger.info(F"Thermostat details retrieved for {len(thermostat_response.thermostat_list)} thermostats.")
    # print(F"Number of thermos found:  {len(thermostat_response)}")

    # Test dumping returns
    # print(F"{thermostat_response.thermostat_list[0].settings.attribute_name_map.keys()}")
    # field_list = [field for field in
    #              thermostat_response.thermostat_list[0].settings.attribute_name_map.keys() if "_" not in field]
    # print(F"Total fields:  {len(field_list)}")
    # for db_name in field_list:
    #    print(F"db name:  {db_name}:: "
    #          F"API name: {thermostat_response.thermostat_list[0].settings.attribute_name_map[db_name]}")

    # for thermostat in thermostat_response.thermostat_list:
    #    print(F"{thermostat.name}: {thermostat.settings.last_service_date}")
    # settings_list = (thermostat.settings).split(", ")
    # print(F"{thermostat.settings.attribute_name_map.keys()}")
    # field_list = [field for field in thermostat.settings.attribute_name_map.keys() if "_" not in field or
    #              thermostat.settings.attribute_name_map[field] == field]
    # field_list = [field for field in thermostat.settings.attribute_name_map.keys() if "_" not in field]
    # print(F"Total fields:  {len(field_list)}")
    # for db_name in field_list:
    #    print(F"db name:  {db_name}:: API name: {thermostat.settings.attribute_name_map[db_name]}")

    for thermostat in thermostat_response.thermostat_list:
        logger.info(F"Thermostats found:  {thermostat.identifier} "
                    F"{thermostat.name} {thermostat.runtime.first_connected} ")
    # Save the first connected information in a dictionary for later iteration
    thermo_cnt = range(0, len(thermostat_response.thermostat_list))
    thermo_connected = {}
    for i in thermo_cnt:
        thermo_connected.update(
            {thermostat_response.thermostat_list[i].name:
                 thermostat_response.thermostat_list[i].runtime.first_connected})
    # logger.debug(F"Thermostat connected date/times: {thermo_connected}")
    # for thermo in thermo_connected.keys():      # or, .values to get dictionary values
    #    logger.info(F"Attempted dump of thermo connected dictionary:  {thermo}")

    # Create a JSON formatted output file with the thermostat summary information, and write it to a file
    # for next iteration
    create_thermostat_summary_JSON(thermostat_summary_response,
                                   json_interval_file)

    # Now as a test, let's try reading the thermostat interval JSON file...
    interval_JSON_file = 'C:\\Users\\Keith\\PycharmProjects\\ECC Ecobee\\Python Ecobee\\ecobee_therm_interval.conf'
    read_interval_JSON = interval_config_from_file(interval_JSON_file)
    logger.info(F"Interval data read from config file:  {read_interval_JSON}")
    # print(F"Test:  {read_interval_JSON['revisionList'][1]['thermostatName']}")
    # Create latest runtime interval dictionary from the JSON data returned
    latest_runtime_intervals_dict = {}
    for thermo in read_interval_JSON['revisionList']:
        logger.info(F"Thermo interval data:  {thermo['thermostatName']} : {thermo['intervalRevision']}")
        latest_runtime_intervals_dict.update({thermo['thermostatName']: thermo['intervalRevision']})

    logger.debug(F"Latest interval data from thermostat summary API call: {latest_runtime_intervals_dict}")

    '''
        Now attempt to open the database.  If successful, read the last record written for each
        thermostat in order to know where to begin in requesting new records.  Also check against
        the previously-stored first-connected information for each to set the very earliest start
        date/time for retrieval (useful if the database is empty, as in the first run).  
    '''
    # Attempt to get a connection to the database table; if the database file does not exist, it will be
    # created, along with the necessary table(s) and indicies.
    conn = connectdb_create_runtime_table()

    # Retrieve the latest revision dates/times written for each thermostat from the db
    logger.debug("Retrieving last written revision dates/times")
    last_rev_dict = {}  # Initialize a dictionary to hold the last revision dates
    for thermo in read_interval_JSON['revisionList']:
        last_db_revision = select_db_last_runtime_interval(conn, thermo['thermostatName'])
        last_rev_dict.update({thermo['thermostatName']: last_db_revision})
        logger.debug(F"Last revision date written in db:  {thermo['thermostatName']}: '{last_db_revision}'")
    # logger.debug(F"Thermo revision dict:  {last_rev_dict}")

    # Now determine the approximate number of days data for each thermostat.  This is in preparation for
    # requesting the runtime reports for the runtime interval data for each, as the call is limited to
    # a maximum of 31 days per call.  Note that the number of retrieval days is for information display
    # only, and is not required for subsequent runtime data retrieval / storage in the SQLite database.
    rev_days_cnt_dict = {}
    rev_days_cnt_dict = calc_revision_days(thermo_connected, last_rev_dict)
    logger.debug(F"Number of days to retrieve:  {rev_days_cnt_dict}")

    # Next, begin to iterate through the thermostats, beginning at the last-revision date, and request
    # runtime data from the Ecobee service.  The request must be broken up into no more than 31 days
    # (30 days used here for safety).
    eastern = pytz.timezone('US/Eastern')
    recs_written_total = 0
    for thermo in read_interval_JSON['revisionList']:
        logger.info(F"Beginning Ecobee data processing for thermostat:  {thermo['thermostatName']}")
        print(F"\nBeginning Ecobee data processing for thermostat:  {thermo['thermostatName']}")
        # Initialize the informational counters for this thermostat
        total_rows_returned_this_thermostat = 0
        recs_written_this_thermostat = 0
        dup_update_cnt_this_thermostat = 0
        blank_rec_cnt_this_thermostat = 0
        # Set the start date for retrieval, either based on the first-connected date (usually for
        # the initial run, where no previous data exists in the runtime db), or, the last revision
        # date/time read from the database.
        rev_date = last_rev_dict.get(thermo['thermostatName'])  # last_rev_dict previously generated from db reads
        if rev_date == "000000000000":  # default for no records currently exist in db
            # Use the first-connected date
            start_datetime = datetime.strptime(thermo_connected.get(thermo['thermostatName']), "%Y-%m-%d %H:%M:%S")
        else:
            start_datetime = datetime.strptime(rev_date, "%y%m%d%H%M%S")
        logger.debug(
            F"Start date for runtime retrieval for thermostat {thermo['thermostatID']} set to {start_datetime}")
        start_datetime_utc = start_datetime.astimezone(pytz.utc)
        start_datetime = eastern.localize(start_datetime, is_dst=True)  # make the time offset aware

        # Set the end date for retrieval, either based on 30 days from the start date (if start+30 days is
        # less than the latest interval date), or, the latest date/time interval retrieved previously from
        # the thermostat summary.  Note that the latest interval data is in UTC, so we must convert it to
        # local time to pass to the API.
        # now_datetime = datetime.now()
        logger.debug(F"Runtime start + 30:  {start_datetime + timedelta(days=30)}")
        interval_datetime_utc = datetime.strptime(
            latest_runtime_intervals_dict.get(thermo['thermostatName']), "%y%m%d%H%M%S")
        interval_datetime_local = pytz.utc.localize(interval_datetime_utc, is_dst=True).astimezone(eastern)
        logger.debug(F"Latest interval date/time:  {interval_datetime_local}")
        if (start_datetime + timedelta(days=30)) >= interval_datetime_local:
            end_datetime = interval_datetime_local
        else:
            end_datetime = start_datetime + timedelta(days=30)
        logger.debug(F"Runtime end datetime initialized at: {end_datetime} local time")

        # The Ecobee runtime API actually uses UTC time for the call, but the library module used
        # here converts local time to UTC for the underlying call; hence, the start/end date/times
        # used here are all in local time.
        while end_datetime <= interval_datetime_local:
            logger.debug(F"Runtime retrieval start/end datetimes:  {start_datetime} :: {end_datetime}")

            # Before calling the runtime report request, check the start date/time (in UTC format) against
            # the latest interval date/time to ensure we're not needlessly calling the report request.
            # This is based on recommendations from the Ecobee API documentation, as the runtime report
            # request is a resource-intensive request, and can return a large amount of data.
            # See references here for further info:
            # https://www.ecobee.com/home/developer/api/documentation/v1/operations/get-runtime-report.shtml
            # https://www.ecobee.com/home/developer/api/documentation/v1/operations/get-thermostat-summary.shtml
            #
            #   Note: we need to convert the start date/time to UTC format as (YYMMDDHHMMSS).
            #   Note: the latest revision interval data was previously requested and stored in
            #         read_interval_JSON['revisionList']
            fmt_start_datetime = datetime.strftime(start_datetime_utc, "%y%m%d%H%M%S")
            logger.debug(F"Converted start date/time for comparison:  {fmt_start_datetime}")

            if fmt_start_datetime <= latest_runtime_intervals_dict.get(thermo['thermostatName']):
                logger.debug(
                    F"Start date of {fmt_start_datetime} prior to latest revision interval date "
                    F"{latest_runtime_intervals_dict.get(thermo['thermostatName'])}")

                runtime_err_cnt = 0
                runtime_err_occurred = True  # falsely set for initial loop iteration
                while runtime_err_occurred and runtime_err_cnt <= 3:
                    runtime_err_occurred = False  # reset to assume success
                    try:
                        runtime_report_response = ecobee_service.request_runtime_reports(
                            selection=Selection(
                                selection_type=SelectionType.THERMOSTATS.value,
                                selection_match=thermo['thermostatID']),
                            start_date_time=start_datetime,
                            end_date_time=end_datetime,
                            columns='auxHeat1,auxHeat2,auxHeat3,compCool1,compCool2,compHeat1,compHeat2,dehumidifier,dmOffset,'
                                    'economizer,fan,humidifier,hvacMode,outdoorHumidity,outdoorTemp,sky,ventilator,wind,'
                                    'zoneAveTemp,zoneCalendarEvent,zoneClimate,zoneCoolTemp,zoneHeatTemp,zoneHumidity,'
                                    'zoneHumidityHigh,zoneHumidityLow,zoneHvacMode,zoneOccupancy',
                            timeout=45)  # timeout for read; longer time required here due to potential large return

                    except EcobeeApiException as e:
                        if e.status_code == 14:  # Authentication error occurred
                            logger.error(F"Ecobee access token expired while requesting thermostat runtime report..."
                                         F"requesting token refresh")
                            runtime_err_cnt += 1
                            runtime_err_occurred = True
                            logger.error(F"...error on thermostat runtime API request, attempt {runtime_err_cnt}")
                            print(F"...error on thermostat runtime API request, attempt {runtime_err_cnt}")
                            try:
                                refresh_tokens(ecobee_service)
                                logger.info(F"Ecobee access tokens refreshed...continuing processing")
                                print(F"Ecobee access tokens refreshed...continuing processing")
                            except EcobeeException as e:
                                logger.error(F"...error occurred while requesting token refresh; exiting...")
                                sys.exit(1)
                    except EcobeeHttpException as e:
                        logger.error(F"HTTP error occurred during Ecobee runtime report API request:  {e}")
                        logger.error(F"...{runtime_report_response.status.code}")
                        print(F"HTTP error occurred...aborting")
                        sys.exit(1)
                    except EcobeeException as e:
                        logger.error(F"Error occurred during Ecobee runtime report API request:  {e}")
                        assert runtime_report_response.status.code == 0, \
                            'Failure while executing request_runtime_reports:\n{0}'.format(
                                runtime_report_response.pretty_format())
                        sys.exit(1)
                    except Exception as e:  # handle HTTP timeout errors
                        logger.error(F"Error occurred during Ecobee runtime report API request...{e}")
                        logger.error(F"...Ecobee status code:  {runtime_report_response.status.code}")
                        timeout_err_msg = "'ReadTimeout' object"
                        if timeout_err_msg in e.__str__():
                            runtime_err_occurred = True
                            runtime_err_cnt += 1
                            logger.error(F"...timeout error on request, attempt {runtime_err_cnt}")
                            print(F"...timeout error on request, attempt {runtime_err_cnt}")
                else:
                    if runtime_err_cnt > 3 and runtime_err_occurred:
                        logger.error(F"Timeout or authentication error occurred during Ecobee runtime report "
                                     F"API request...")
                        logger.error(F"...maximum retry attempts exceeded, aborting (try again later)")
                        print(F"...maximum retry attempts exceeded, aborting (try again later)")
                        sys.exit(1)

                cols = runtime_report_response.columns
                runtime_rows = runtime_report_response.report_list
                logger.debug(F"Columns returned:  {cols}")
                rows_returned = runtime_rows[0].row_count
                total_rows_returned_this_thermostat += rows_returned
                logger.debug(F"Number of rows returned for this query:  {rows_returned}, "
                             F"thermostat: {runtime_rows[0].thermostat_identifier}")
                # logger.debug(F"First row response:  {runtime_rows[0].row_list[0]}")
                for row_response in runtime_rows:
                    for row_cntr in range(0, rows_returned):
                        # logger.debug(F"Row response:  {runtime_rows[0].row_list[row_cntr]}")
                        insert_runtime_rec_status = create_runtime_record(conn,
                                                                          thermo['thermostatName'],
                                                                          runtime_rows[0].thermostat_identifier,
                                                                          runtime_rows[0].row_list[row_cntr])
                        if insert_runtime_rec_status:
                            recs_written_this_thermostat += 1
                            recs_written_total += 1
                            if recs_written_this_thermostat % 100 == 0:
                                print(F"Records written for thermostat {thermo['thermostatName']}:  "
                                      F"{recs_written_this_thermostat}")

                # logger.debug(runtime_report_response.pretty_format())

            else:
                logger.debug(
                    F"Polling start date {fmt_start_datetime} later than last revision interval date {latest_runtime_intervals_dict.get(thermo['thermostatName'])}")

            # Move reporting window to the next 30 days if necessary
            start_datetime = end_datetime
            start_datetime_utc = start_datetime.astimezone(pytz.utc)  # for next check against last rev interval

            if start_datetime == interval_datetime_local:
                break
            elif end_datetime + timedelta(days=30) > interval_datetime_local:
                end_datetime = interval_datetime_local
            else:
                end_datetime += timedelta(days=30)
            # logger.debug(F"New end date/time:  {end_datetime}")

        logger.info(F"Runtime database records written for thermostat {thermo['thermostatName']}:  "
                    F"{recs_written_this_thermostat}")
        print(F"Runtime database records written for thermostat {thermo['thermostatName']}:  "
              F"{recs_written_this_thermostat}")
        logger.info(F"Duplicate runtime database records re-written for thermostat {thermo['thermostatName']}:  "
                    F"{dup_update_cnt_this_thermostat}")
        print(F"Duplicate runtime database records re-written for thermostat {thermo['thermostatName']}:  "
              F"{dup_update_cnt_this_thermostat}")
        logger.info(F"Blank runtime records skipped for thermostat {thermo['thermostatName']}:  "
                    F"{blank_rec_cnt_this_thermostat}")
        print(F"Blank runtime records skipped for thermostat {thermo['thermostatName']}:  {blank_rec_cnt_this_thermostat}")
        logger.info(
            F"Total runtime rows returned for thermostat {thermo['thermostatName']}: {total_rows_returned_this_thermostat}")
        print(
            F"Total runtime rows returned for thermostat {thermo['thermostatName']}: {total_rows_returned_this_thermostat}")

    # Next, store records for the "snapshot" data retrieved previously from the thermostat details.
    # (This includes such data as the thermostat settings, weather, etc., for which historical data is
    # not maintained by the Ecobee service.
    #    selection = Selection(selection_type=SelectionType.REGISTERED.value, selection_match='', include_alerts=True,
    #                          include_device=True, include_electricity=False, include_equipment_status=True,
    #                          include_events=False, include_extended_runtime=True, include_house_details=True,
    #                          include_location=True, include_management=False, include_notification_settings=True,
    #                          include_oem_cfg=False, include_privacy=False, include_program=True,
    #                          include_reminders=True,
    #                          include_runtime=True, include_security_settings=False, include_sensors=True,
    #                          include_settings=True, include_technician=False, include_utility=False,
    #                          include_version=True,
    #                          include_weather=True)

        for thermo_object, db_table in SQLite_table_dict.items():
            logger.debug(F"Beginning processing for snapshot records, table:  {db_table}, "
                         F"thermostat:  {thermo['thermostatName']}")
            print(F"Beginning processing for snapshot records, table:  {db_table}, "
                  F"thermostat:  {thermo['thermostatName']}")
            db_table_recs_written[db_table] = 0
            # print(F"Table:  {db_table}, object:  {thermo_object}, recs written:  {db_table_recs_written[table]}")
            lists_dict = get_snapshot(conn,
                                      db_table,
                                      thermo_object,
                                      thermostat_response.thermostat_list)
            # Logic to follow to handle processing of embedded lists...
            #
            # Check for the lists dictionary not defined to prevent error if no data is available for the thermostat
            # object
            # if 'NoneType' not in str(type(lists_dict)):
            #    if len(lists_dict) != 0:
            #        print(F"...Beginning processing of list data, table:  {db_table} "
            #              F"thermostat {thermo['thermostatName']}")
            # If more data in list, loop to handle...
                # for list_object, list_table in lists_dict.items():
                #    print(F"...object:  {list_object}, ...list table:  {list_table}")

    conn.close()  # Close the db connection
    logger.info(F"Total runtime database records written, all thermostats, this execution:  {recs_written_total}")
    print(F"\nTotal runtime database records written, all thermostats, this execution:  {recs_written_total}")
    logger.info(
        F"Total duplicate runtime database records updated, all thermostats, this execution:  {dup_update_cnt_total}")
    print(F"Total duplicate runtime database records updated, all thermostats, this execution:  {dup_update_cnt_total}")
    logger.info(F"Total runtime blank records skipped, all thermostats, this execution:  {blank_rec_cnt_total}")
    print(F"Total runtime blank records skipped, all thermostats, this execution:  {blank_rec_cnt_total}")

    # Now print a summary of the records written this pass to the "snapshot" tables, those for
    # which Ecobee does not provide a history
    for recs in db_table_recs_written:
        logger.info(F"Total snapshot database records written for table {recs}:  {db_table_recs_written[recs]}")
        print(F"Total snapshot database records written for table {recs}:  {db_table_recs_written[recs]}")
        snapshot_recs_written_total += db_table_recs_written[recs]
    logger.info(F"Total snapshot database records written, all other tables (than runtime):  {snapshot_recs_written_total}")
    print(F"Total snapshot database records written, all other tables (than runtime):  {snapshot_recs_written_total}")


def persist_to_shelf(file_name, ecobee_service):
    pyecobee_db = shelve.open(file_name, protocol=4)
    pyecobee_db['application_key'] = ecobee_service.application_key
    logger.debug(F"Persist access token:  {ecobee_service.access_token}")
    logger.debug(F"Persist refresh token:  {ecobee_service.refresh_token}")
    pyecobee_db['access_token'] = ecobee_service.access_token
    pyecobee_db['refresh_token'] = ecobee_service.refresh_token
    pyecobee_db['authorization_token'] = ecobee_service.authorization_token

    pyecobee_db.close()


def refresh_tokens(ecobee_service):
    try:
        token_response = ecobee_service.refresh_tokens()
        logger.debug(F"Token response returned from refresh tokens request:  \n{token_response.pretty_format()}")
        ecobee_service.access_token = token_response.access_token
        ecobee_service.refresh_token = token_response.refresh_token
        persist_to_shelf(ECCAuthorize, ecobee_service)
    except EcobeeAuthorizationException as e:
        logger.error(F"Error during request to refresh Ecobee access tokens:  {e}")
        if 'The authorization grant, token or credentials are invalid, expired, revoked' in e.error_description:
            logger.error(F"...authorization credentials have expired or invalid")
            logger.error(F"...resetting stored authorization credentials")
            logger.error(F"...you will need to re-authorize the application in the Ecobee portal")
            # Remove the (3) credentials files created by the Shelve module.  (This module uses the
            # dumbdbm module to create the database on Windows, resulting in these files being created
            # by default.)
            auth_file_string_dat = ECCAuthorize + ".dat"
            auth_file_string_bak = ECCAuthorize + ".bak"
            auth_file_string_dir = ECCAuthorize + ".dir"
            try:
                os.remove(auth_file_string_dat)
                os.remove(auth_file_string_bak)
                os.remove(auth_file_string_dir)
                logger.info(F"Ecobee authorization credentials files removed successfully")
            except Exception as e:
                logger.error(F"Error occurred deleting authorization credentials file:  {e}")
                sys.exit(1)
            authorize(ecobee_service)
    except EcobeeException as e:
        logger.error(F"Error during request to refresh Ecobee access tokens:  {e}")
        sys.exit(1)


def request_tokens(ecobee_service):
    try:
        token_response = ecobee_service.request_tokens()
        logger.debug(F"Token response returned from request tokens API call:  \n{token_response.pretty_format()}")
        ecobee_service.access_token = token_response.access_token
        ecobee_service.refresh_token = token_response.refresh_token
        persist_to_shelf(ECCAuthorize, ecobee_service)
    except EcobeeAuthorizationException as e:
        logger.error(F"Authorization error occurred while requesting Ecobee access tokens:  {e}")
        if 'authorization has expired' in e.error_description:
            logger.error(F"...the prior authorization has expired waiting for user to authorize.")
            logger.error(F"...attempting re-authorization")
            try:
                authorize(ecobee_service)
            except EcobeeException as e:
                logger.error(F"...error occurred while attempting to re-authorize Ecobee API, {e}")
                sys.exit(1)
        if 'Waiting for user to authorize' in e.error_description:
            logger.error(F"...waiting for user to authorize application...please log into Ecobee.com "
                         F"and authorize application with PIN as directed, then re-run this application to "
                         F"continue.")
            sys.exit(1)
    except EcobeeException as e:
        logger.error(F"Error during request for Ecobee access tokens:  {e}")
        sys.exit(1)


def authorize(ecobee_service):
    try:
        authorize_response = ecobee_service.authorize()
        logger.debug(F"Authorize response returned from authorize API call:  \n{authorize_response.pretty_format()}")
        persist_to_shelf(ECCAuthorize, ecobee_service)
        logger.info(
            F"...Please go to Ecobee.com, login to the web portal and click on the settings tab. Ensure the My ")
        logger.info(
            F"...Apps widget is enabled. If it is not click on the My Apps option in the menu on the left. In the ")
        logger.info(
            F"...My Apps widget paste '{authorize_response.ecobee_pin}' and in the textbox labeled 'Enter your 4 digit ")
        logger.info(
            F"...pin to install your third party app' and then click 'Install App'.  The next screen will display any ")
        logger.info(F"...permissions the app requires and will ask you to click 'Authorize' to add the application.")
        logger.info(F"...After completing this step please re-run this application to continue.")

        print(F"Application needs to be re-authorized.  Check log for further details.")

        ecobee_service.authorization_token = authorize_response.code
        # Clear the access and refresh tokens, as these are no longer valid with a re-authorization of
        # the app and will need to be requested again on next run
        ecobee_service.access_token = ''
        ecobee_service.refresh_token = ''
        pyecobee_db = shelve.open(ECCAuthorize, protocol=4)  # Save the PIN for future information displays
        pyecobee_db['PIN'] = authorize_response.ecobee_pin
        pyecobee_db.close()

        persist_to_shelf(ECCAuthorize, ecobee_service)
        sys.exit(1)

    except EcobeeApiException as e:
        logger.error(F"Error during request for authorization of Ecobee service:  {e}")
        sys.exit(1)


def create_thermostat_summary_JSON(thermostat_summary_response, thermostat_JSON_interval_file):
    """
        This routine will create a JSON-formatted output file containing the thermostat summary information.
        This data contains the last intervals at which data was reported to the Ecobee service, and is
        used to determine when new data is available and should be written to the local database.
        (See https://www.ecobee.com/home/developer/api/documentation/v1/operations/get-thermostat-summary.shtml
        for recommendations on how thermostat polling should be conducted to not overload the Ecobee service.
            Written by DK Fowler 09-Oct-2019
    :param: thermostat_summary_response     thermostat summary object containing JSON revision interval data
    :param: json_interval_file              JSON output file to which JSON formatted interval data is stored
    :return:
    """
    thermostat_summary_fields = ['thermostatID',
                                 'thermostatName',
                                 'thermostatConnected',
                                 'thermostatRevision',
                                 'alertsRevision',
                                 'runtimeRevision',
                                 'intervalRevision'
                                 ]
    # summaryJSON = {}
    summaryJSONstring = '{ "revisionList" : ['
    recIndex = 0
    for thermostatRevisionRec in thermostat_summary_response.revision_list:
        summaryJSONstring = summaryJSONstring + '{'
        revSplit = thermostatRevisionRec.split(':')
        fieldCntRange = range(7)
        for fieldCnt in fieldCntRange:
            summaryJSONstring = summaryJSONstring \
                                + '"' + thermostat_summary_fields[fieldCnt] \
                                + '" : "' + revSplit[fieldCnt] + '"'
            if fieldCnt != 6:
                summaryJSONstring = summaryJSONstring + ", "
            else:
                if recIndex == len(thermostat_summary_response.revision_list) - 1:
                    summaryJSONstring = summaryJSONstring + "} "
                else:
                    summaryJSONstring = summaryJSONstring + "}, "
        # summaryJSONstring = summaryJSONstring + '\n'
        recIndex += 1

    summaryJSONstring = summaryJSONstring + '] }'

    try:
        logger.debug(F"Summary JSON string follows: \n{summaryJSONstring}")
        JSONrevisionList = json.loads(summaryJSONstring)
        # logger.debug(F"Serialized JSON revision list: {JSONrevisionList}")
    except ValueError as e:
        logger.exception(F"Error occurred while converting interval data to JSON...{e}")
        return False

    logger.debug(F"Final serialized JSON revision list: {json.dumps(JSONrevisionList, indent=4)}")
    # logger.debug(json.dumps(JSONrevisionList, indent=4))

    # Now write the JSON interval information for the thermostats to a file for use in the next polling iteration
    interval_config_from_file(thermostat_JSON_interval_file, JSONrevisionList)


def interval_config_from_file(filename, config=None):
    """
        This routine will read the thermostat interval JSON file and load it into a list for
        further processing, if the passed parameter "config" is set to None.  Otherwise, it will
        write the interval JSON file if the passed parameter "config" contains data.
        It will return True if successful, otherwise, False.
            Written by DK Fowler ... 10-Oct-2019
    :param filename: name of configuration file to open
    :param config: name of list in which to load/read JSON info
    :return: True if successful, otherwise, False

    """
    if config:
        # Passed list contains data; we're writing configuration
        try:
            with open(filename, 'w') as therminterval:
                therminterval.write(json.dumps(config))
            logger.debug(f"Saved {len(config)} records to thermostat interval file.")
        except IOError as error:
            logger.exception(F"Error while attempting to write thermostat interval file: {error}")
            return False
        return True
    else:
        # Passed list is initialized at None; we're reading config
        if os.path.isfile(filename):
            try:
                with open(filename, 'r') as therminterval:
                    return json.loads(therminterval.read())
            except IOError as error:
                logger.exception(F"Error while attempting to read thermostat interval file:  {error}")
                return False
        else:
            return {}


def calc_revision_days(thermo_connect_dict, rev_dict):
    """
        This routine will calculate the approximate number of days that will need to be retrieved from
        the Ecobee service for each thermostat, based on the last date written to the database.  This
        is required for later processing, as the call to retrieve the Ecobee runtime data is limited to
        no more than 31 days at a time.
                Written by DK Fowler ... 12-Oct-2019
    :param thermo_connect_dict:    Dictionary which contains the thermostat name and first connected datetime
    :param rev_dict:               Dictionary which contains the thermostat name and last revision date written
    :return:                       Dictionary which contains the thermostat name and number of days needed to be
                                   retrieved
    """

    retrieve_dict = {}
    for thermo, rev_date in rev_dict.items():
        # Convert the revision date to a datetime value for easier delta processing
        if rev_date == "000000000000":  # default for no records currently exist in db
            # Default datetime if no record exists in the database; if so, use the first-connected date
            # from the passed thermo_connect_dict
            revision_datetime = datetime.strptime(thermo_connect_dict.get(thermo), "%Y-%m-%d %H:%M:%S")
        else:
            revision_datetime = datetime.strptime(rev_date, "%y%m%d%H%M%S")
        now_datetime = datetime.now()
        retrieve_days = (now_datetime - revision_datetime).days
        logger.debug(F"Days to retrieve for thermostat {thermo}:  {retrieve_days}")
        retrieve_dict[thermo] = retrieve_days

    return retrieve_dict


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
        (From SQLite Tutorial at sqlitetutorial.net)
        Modified by DK Fowler ... 09-Oct-2019
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        logger.debug(F"Connected to database:  {db_file}")
    except Error as e:
        logger.error(F"Error occurred while attempting to establish connection to database...")
        logger.error(F"...{e}")

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
        (From SQLite Tutorial at sqlitetutorial.net)
        Modified by DK Fowler ... 09-Oct-2019
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:    True if successful, else False
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        logger.info(F"Successfully created database table")
        c.close()
        return True
    except sqlite3.Error as e:
        logger.error(F"Error occurred while attempting to create database table...")
        logger.error(F"...{e}")
        return False


def connectdb_create_runtime_table():
    """
        This routine will attempt to create the database tables and indicies for the Ecobee data
        storage.
            Written by DK Fowler ... 09-Oct-2019
    :return:
    """
    sql_create_ecobee_runtime_table = """CREATE TABLE IF NOT EXISTS runtime (
                                            record_written_UTC TEXT NOT NULL,
                                            thermostat_name TEXT NOT NULL,
                                            thermostat_id TEXT NOT NULL,
                                            run_date TEXT NOT NULL,
                                            run_time TEXT NOT NULL,
                                            aux_heat1 NUMERIC,
                                            aux_heat2 NUMERIC,
                                            aux_heat3 NUMERIC,
                                            comp_cool1 NUMERIC,
                                            comp_cool2 NUMERIC,
                                            comp_heat_1 NUMERIC,
                                            comp_heat_2 NUMERIC,
                                            dehumidifier NUMERIC,
                                            dmoffset NUMERIC,
                                            economizer NUMERIC,
                                            fan NUMERIC,
                                            humidifier NUMERIC,
                                            hvac_mode TEXT,
                                            outdoor_humidity NUMERIC,
                                            outdoor_temp NUMERIC,
                                            sky NUMERIC,
                                            ventilator NUMERIC,
                                            wind NUMERIC,
                                            zone_ave_temp NUMERIC,
                                            zone_calendar_event TEXT,
                                            zone_climate TEXT,
                                            zone_cool_temp NUMERIC,
                                            zone_heat_temp NUMERIC,
                                            zone_humidity NUMERIC,
                                            zone_humidity_high NUMERIC,
                                            zone_humidity_low NUMERIC,
                                            zone_hvac_mode TEXT,
                                            zone_occupancy TEXT,
                                            PRIMARY KEY (thermostat_id, run_date, run_time)
                                       ); """
    sql_create_ecobee_runtime_index1 = """CREATE INDEX thermostat_name_idx
                                            ON runtime(thermostat_name);"""
    sql_create_ecobee_runtime_index2 = """CREATE INDEX thermostat_id_idx
                                            ON runtime(thermostat_id);"""

    # create a database connection
    conn = create_connection(ECCEcobeeDatabase)

    # create tables
    if conn is not None:
        # create runtime table
        # First, check if the table exists:
        db_table = "runtime"
        table_exists = check_if_table_exists(conn, db_table, ECCEcobeeDatabase)
        if not table_exists:
            table_success = create_table(conn, sql_create_ecobee_runtime_table)
            # Create secondary indicies
            try:
                indices_success = conn.execute(sql_create_ecobee_runtime_index1)
                indices_success = conn.execute(sql_create_ecobee_runtime_index2)
            except sqlite3.Error as e:
                logger.error(F"Error creating secondary indicies for table {db_table}, {e}")
        else:
            logger.debug(F"Database table {db_table} exists...opening connection")
    else:
        logger.error("Error...cannot create the Ecobee database connection.")

    return conn


def check_if_table_exists(conn, ecobeeTable, ECCEcobeeDatabase):
    """
        This routine will check for the existence of the table passed in ecobeeTable in the
        database file ECCEcobeeDatabase.  If it exists, the function will return true; otherwise
        it will return false.
            Written by DK Fowler ... 09-Oct-2019
    :param conn:                database connection (may exist already, or be set to None
    :param ecobeeTable:         table name to check for existence
    :param ECCEcobeeDatabase:   database file to check for table existence
    :return:                    True if table exists; otherwise, False
    """
    if conn is None:
        try:
            conn = create_connection(ECCEcobeeDatabase)
        except Error:
            logger.error(
                F"Error connecting to database {ECCEcobeeDatabase} to check for table {ecobeeTable} existence.")
            return False

    # Check the SQLite master table for the table name
    c = conn.cursor()
    find_table_query = "SELECT count(name) FROM sqlite_master WHERE type='table' AND name=? "
    logger.debug(F"Find table query string:  {find_table_query}")
    c.execute(find_table_query, (ecobeeTable,))
    # c.execute(find_table_query)

    # Now check the number returned; if not 1, then table doesn't exist
    foundFlag = False
    if c.fetchone()[0] == 1:
        logger.info(f"Table '{ecobeeTable}' found...")
        foundFlag = True
    else:
        logger.info(f"Table '{ecobeeTable}' not found...")

    # commit the changes (none here)
    conn.commit()
    # close the connection
    # conn.close()

    return foundFlag


def create_table_from_string(conn, db_table, create_table_sql_str):
    """
        This routine will attempt to create a database table given the passed database connection,
        table name, and SQL creation string.  If successful, it will also create secondary
        indicies for the thermostat name and ID.
            Written by DK Fowler ... 24-Oct-2019
    :param conn:                database connection (may exist already, or be set to None)
    :param db_table:            table name to create
    :param create_table_sql_str string containing SQL statement to create table
    :return:                    True if table created successfully; otherwise, False
    """

    table_success = False  # Assume failure
    indicies_success = False

    # Check to make sure we have a database connection
    if conn is not None:
        # SQL strings to create secondary indicies.  (All tables use the same secondary keys.)
        sql_create_ecobee_sec_index1 = "CREATE INDEX " + db_table + "_thermostat_name_idx ON " + \
                                       db_table + "(thermostat_name);"
        sql_create_ecobee_sec_index2 = "CREATE INDEX " + db_table + "_thermostat_id_idx ON " + \
                                       db_table + "(thermostat_id);"
        table_exists = check_if_table_exists(conn, db_table, ECCEcobeeDatabase)
        if not table_exists:
            table_success = create_table(conn, create_table_sql_str)
            if not table_success:
                logger.debug(F"Error occurred attempting to create table {db_table}...aborting")
                sys.exit(1)
            # Create secondary indicies
            try:
                indicies_success = conn.execute(sql_create_ecobee_sec_index1)
                indicies_success = conn.execute(sql_create_ecobee_sec_index2)
            except sqlite3.Error as e:
                logger.error(F"Error creating secondary indicies for table {db_table}, {e}")
        else:
            logger.debug(F"Database table {db_table} exists...")
            table_success = True  # Set the success flags, as table and indicies already exist
            indicies_success = True
    else:
        logger.error("Error...the Ecobee database connection did not exist when attempting to create "
                     F"table {db_table}")
        logger.error(F"...aborting")
        sys.exit(1)

    if table_success and indicies_success:
        return True
    else:
        return False


def create_database_record(conn, db_table, thermo_name, thermo_id, db_fields, db_values):
    """
        This routine will attempt to write a record to the passed table with the passed
        thermostat name, ID, and list of record names / values.  If successful, the routine
        will return True, else False.
            Written by DK Fowler ... 24-Oct-2019
    :param conn:            database connection (may exist already, or be set to None)
    :param db_table:        table name to which a record write is attempted
    :param thermo_name      name of thermostat
    :param thermo_id        id of thermostat
    :param db_fields        dictionary of field names for the record
    :param db_values        list of values for the record
    :return:                True if table created successfully; otherwise, False
    """

    # First create a SQL string for INSERTing the record into the passed table
    sql_insert = construct_insert_table_sql(db_table, db_fields)

    # Create a dictionary of the field values
    values_dict = create_values_dict(db_fields, db_values)

    # Get the current date/time in UTC for the record INSERT
    record_written_UTC = datetime.utcnow()

    cur = conn.cursor()  # Get a cursor for the db connection
    # Make sure we have a database connection
    if conn is not None:
        logger.debug(F"Attempting record insert...table:  {db_table}")
        try:
            val_list = [val for key, val in values_dict.items()]
            date_written_str = datetime.strftime(record_written_UTC, "%Y-%m-%d %H:%M:%S.%f")
            val_list.insert(0, date_written_str)  # insert common key values in the beginning of the value list
            val_list.insert(1, thermo_name)
            val_list.insert(2, thermo_id)
            # Special case next...for the 'thermDevices' table, the thermostat name and id are already included
            # in the data.  Delete these duplicate keys.
            if db_table == 'thermDevices':
                del val_list[3:5]  # id and name are in positions 3, 4 of the list
            # print(F"Values used in INSERT:  {val_list}")
            logger.debug(F"...field values {db_values}")
            cur.execute(sql_insert, val_list)
            logger.debug(F"Record written for thermostat {thermo_name}, date: {record_written_UTC}, "
                         F"table: {db_table}")
        except sqlite3.Error as e:
            cur.close()
            logger.error(F"Error writing to database table {db_table}, {e}")
            err_string = e.__str__()  # returns string with error message
            # define text for duplicate primary key error; (would be more effective to check extended error code,
            # but Python doesn't seem to support this yet??
            unique_err_str = "UNIQUE constraint failed: runtime.thermostat_id, runtime.run_date, runtime.run_time"
            # logger.debug(F"Error:  {e.__str__()}")
            # logger.debug(F"Exception class is: {e.__class__}")
            # logger.debug(F"Exception is  {e.args}")
            # if e.__class__ == sqlite3.IntegrityError:
            #    logger.debug(F"Integrity error; duplicate key detected...record already exists in db")

            # The following error should NEVER occur, as the date written is part of the key and is
            # created in this routine.
            if unique_err_str in err_string:
                logger.error(F"Duplicate key detected...record already exists in database...")
            return False
        except ValueError as e:  # Catches malformed INSERT in some instances...
            logger.error(F"Value error occurred during attempt to INSERT record in table {db_table}, error:  {e}")
            logger.error(F"...SQL INSERT statement:  {sql_insert}")
            logger.error(F"...Values used in INSERT:  {val_list}")
            cur.close()
            return False
        except Exception as e:
            logger.error(F"Exception occurred during INSERT:  {e}")
            logger.error(F"...SQL INSERT statement:  {sql_insert}")
            logger.error(F"...Values used in INSERT:  {val_list}")
            cur.close()
            return False
    else:
        logger.error(F"No database connection detected while attempting to write new record, table {db_table}")
        sys.exit(1)

    conn.commit()
    cur.close()
    return True


def create_values_dict(db_fields, db_values):
    """
        This routine will construct a dictionary with the key being the field name and the value being
        the field value.
            Written by DK Fowler ... 24-Oct-2019
    :param db_fields:    list of fields for this table
    :param db_values     list of field values for this table
    :return:             dictionary containing [key: value] as [field name: field value]
    """

    field_vals_dict = {}
    # Filter out list fields and defined thermostat objects for special processing
    non_list_fields = [non_list_field for non_list_field in db_fields if 'List' not in db_fields[non_list_field] and
                       non_list_field not in thermostat_object_dict]
    for field_value in non_list_fields:
        val_str = 'db_values.' + field_value  # construct string reference for the value
        e_val = eval(repr(val_str))
        field_vals_dict[field_value] = eval(e_val)  # yeah, it's like magic I think; not sure why the double 'eval'?
        # print(F"Value is:  {eval(e_val)}")

    # print(F"Values dictionary:  {field_vals_dict}")
    logger.debug(F"Values dictionary:  {field_vals_dict}")

    return field_vals_dict


def construct_insert_table_sql(db_table, db_fields):
    """
        This routine will construct a SQLite INSERT statement to be used in an attempt to
        write a new record to the table passed.
            Written by DK Fowler ... 24-Oct-2019
    :param db_table:        table name to which a record INSERT will be attempted
    :param db_fields        Dictionary of field names for the record
    :return:                SQLite string containing the INSERT statement
    """

    # logger.debug(F"DB fields:  {db_fields}")
    # Begin construction with the field names common to all records, also used for the PRIMARY KEY
    db_insert_sql_str = '''INSERT INTO ''' + db_table + '''(record_written_UTC, thermostat_name, thermostat_id, '''

    field_cnt = 0
    # logger.debug(F"Length of passed field list:  {len(db_fields)}")
    # Filter out fields that are lists and defined thermostat objects; special processing is needed for these
    # (lists and objects will be written to a separate table)
    non_list_fields = [non_list for non_list in db_fields if 'List' not in db_fields[non_list] and
                       non_list not in thermostat_object_dict]
    for field in non_list_fields:
        field_cnt += 1
        # special case for table 'thermDevices', since thermostat name and id are already part of the
        # standard record key structure, skip them here
        if not (db_table == 'thermDevices' and (field == 'name' or field == 'identifier')):
            db_insert_sql_str += field
            # logger.debug(F"...field count:  {field_cnt}, field:  {field}")
            if field_cnt != len(non_list_fields):  # don't add the comma for the last field
                db_insert_sql_str += ', '
    db_insert_sql_str += ''') VALUES(?,?,?,'''  # first 3 values are for the common fields, used for the key

    # special case for table 'thermDevices', remove place holder '?' for fields name, identifier
    if db_table == 'thermDevices':
        ph_range = range(0, len(non_list_fields) - 3)
    else:
        ph_range = range(0, len(non_list_fields) - 1)
    for field_ph in ph_range:
        db_insert_sql_str += '''?,'''

    db_insert_sql_str += '''?)'''
    logger.debug(F"SQL INSERT statement:  '{db_insert_sql_str}'")

    return db_insert_sql_str


def select_db_last_runtime_interval(conn, thermostatName):
    """
    Query last runtime interval for the specified thermostat.
    (Based on SQLite Tutorial at sqlitetutorial.net)

    This routine will return the last runtime interval written to the thermostat runtime table, based
    on previous calls to the Ecobee request runtime API.  The intent is to provide the last
    runtime interval written for this thermostat to prevent redundant attempts to record data already
    written.
        Written by DK Fowler ... 8-Oct-2019

    :param conn: the Connection object
    :param thermostatName: the name of the thermostat
    :return: the last runtime interval recorded, or '00000000' if none
    """

    runtime_fields = ['record_written_UTC',
                      'thermostat_name',
                      'thermostat_id',
                      'run_date',
                      'run_time',
                      'aux_heat1',
                      'aux_heat2',
                      'aux_heat3',
                      'comp_cool1',
                      'comp_cool2',
                      'comp_heat_1',
                      'comp_heat_2',
                      'dehumidifier',
                      'dmoffset',
                      'economizer',
                      'fan',
                      'humidifier',
                      'hvac_mode',
                      'outdoor_humidity',
                      'outdoor_temp',
                      'sky',
                      'ventilator',
                      'wind',
                      'zone_ave_temp',
                      'zone_calendar_event',
                      'zone_climate',
                      'zone_cool_temp',
                      'zone_heat_temp',
                      'zone_humidity',
                      'zone_humidity_high',
                      'zone_humidity_low',
                      'zone_hvac_mode',
                      'zone_occupancy']

    cur = conn.cursor()
    # Build SQL query string, including exclusion of "blank" records...
    rev_str = "SELECT * FROM runtime WHERE thermostat_name=? AND NOT ("
    for field_idx in range(5, len(runtime_fields)):
        rev_str += runtime_fields[field_idx] + "= ''"
        if field_idx != len(runtime_fields) - 1:
            rev_str += " AND "
        else:
            rev_str += ") ORDER BY run_date DESC, run_time DESC LIMIT 1"
    logger.debug(F"SQL for last revision interval:  ")
    logger.debug(F"{rev_str}")
    cur.execute(rev_str, (thermostatName,))

    rows = cur.fetchall()

    row_cnt = 0
    for row in rows:
        row_cnt = row_cnt + 1
        logger.debug(F"Last runtime interval record for thermostat: {thermostatName}")
        logger.debug(row)
    """
        If no rows are returned for this thermostat, then there were previously no entries written in
        the thermostat summary table.  This would typically happen during first run of the routine, or
        when a new thermostat is added.  In this case, return a default value for the last runtime
        interval written; otherwise, return the last runtime interval logged.
    """
    if row_cnt == 0:
        last_runtime_interval = "000000000000"
        logger.debug(F"No records found for thermostat {thermostatName} while retrieving last revision written")
    else:
        # Calculate the last run date/time from the record retrieved; the 3rd and 4th fields contain this
        # data (indexed from 0)
        str_last_run = rows[0][3] + " " + rows[0][4]
        logger.debug(F"Revision interval date/time string retrieved from db:  {str_last_run}")
        # Convert the text string to a datetime datatype
        last_run = datetime.strptime(str_last_run, "%Y-%m-%d %H:%M:%S")
        # Format it to match the revision date/time reported by Ecobee
        last_runtime_interval = last_run.strftime("%y%m%d%H%M%S")

    cur.close()
    return last_runtime_interval


def create_runtime_record(conn, thermostat_name, thermostat_id, runtime_row):
    """
        This routine will attempt to add a new record to the SQLite runtime table.
            Written by DK Fowler ... 16-Oct-2019
    :param conn:            Connection object for database
    :param thermostat_name  Thermostat name
    :param thermostat_id    Thermostat ID
    :param runtime_row:     CSV list of field data
    :return:                True, if successful write, else False
    """
    runtime_fields = ['record_written_UTC',
                      'thermostat_name',
                      'thermostat_id',
                      'run_date',
                      'run_time',
                      'aux_heat1',
                      'aux_heat2',
                      'aux_heat3',
                      'comp_cool1',
                      'comp_cool2',
                      'comp_heat_1',
                      'comp_heat_2',
                      'dehumidifier',
                      'dmoffset',
                      'economizer',
                      'fan',
                      'humidifier',
                      'hvac_mode',
                      'outdoor_humidity',
                      'outdoor_temp',
                      'sky',
                      'ventilator',
                      'wind',
                      'zone_ave_temp',
                      'zone_calendar_event',
                      'zone_climate',
                      'zone_cool_temp',
                      'zone_heat_temp',
                      'zone_humidity',
                      'zone_humidity_high',
                      'zone_humidity_low',
                      'zone_hvac_mode',
                      'zone_occupancy']

    # Add additional identifying fields to the runtime row data passed to the routine:
    # (record written datetime, thermostat name and id)
    record_written_UTC = datetime.utcnow()
    # runtime_row = "'" + datetime.strftime(record_written_UTC, "%Y-%m-%d %H:%M:%S") + "','" + \
    #    thermostat_name + "','" + thermostat_id + "','" + runtime_row
    runtime_row = datetime.strftime(record_written_UTC, "%Y-%m-%d %H:%M:%S") + "," + \
                  thermostat_name + "," + thermostat_id + "," + runtime_row
    # Data includes a trailing comma, remove it
    runtime_row = runtime_row[:-1]
    # Split the row data by the comma delimiter
    runtime_row_split = runtime_row.split(",")
    # logger.debug(F"Runtime row data:  {runtime_row_split}")

    # Build sql insert statement with list of fields
    sql_insert = ''' INSERT INTO runtime ('''
    for field in range(0, len(runtime_fields)):
        sql_insert += runtime_fields[field]
        if field != len(runtime_fields) - 1:
            sql_insert += ', '
    sql_insert += ''') VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    # logger.debug(F"SQL insert statement:  '{sql_insert}'")
    cur = conn.cursor()
    # Ensure the record we're getting ready to write isn't blank; if so, skip it.
    if not check_for_empty_runtime_record(runtime_row_split):
        try:
            cur.execute(sql_insert, runtime_row_split)
            logger.debug(F"Record written for thermostat {thermostat_name}, date: {runtime_row_split[3]}, "
                         F"time: {runtime_row_split[4]}")
        except sqlite3.Error as e:
            cur.close()
            logger.error(F"Error writing to database, {e}")
            err_string = e.__str__()  # returns string with error message
            # define text for duplicate primary key error; (would be more effective to check extended error code,
            # but Python doesn't seem to support this yet??
            unique_err_str = "UNIQUE constraint failed: runtime.thermostat_id, runtime.run_date, runtime.run_time"
            # logger.debug(F"Error:  {e.__str__()}")
            # logger.debug(F"Exception class is: {e.__class__}")
            # logger.debug(F"Exception is  {e.args}")
            # if e.__class__ == sqlite3.IntegrityError:
            #    logger.debug(F"Integrity error; duplicate key detected...record already exists in db")
            if unique_err_str in err_string:
                logger.error(F"Duplicate key detected...record already exists in database...")
                logger.error(F"...thermostat: {thermostat_name}, date: {runtime_row_split[3]}, "
                             F"time: {runtime_row_split[4]}")
                # Attempt to update the duplicate record in the database
                rewrite_status = check_and_rewrite_duplicate_record(conn, runtime_row_split)
                if not rewrite_status:
                    logger.debug(F"Database record NOT updated")

            return False
        finally:
            conn.commit()
            cur.close()
            return True
    else:
        logger.info(F"Empty record detected for thermostat: {thermostat_name}, "
                    F"date: {runtime_row_split[3]}, time: {runtime_row_split[4]}")
        logger.info(F"...skipped writing record to database")
        logger.info(F"{runtime_row_split}")


def check_for_empty_runtime_record(db_record):
    """
        This routine will scan the passed database record contents to determine if it contains
        "blank" data.  This appears to be a quirk in the way the Ecobee thermostats report
        their runtime data; many times the latest 8-10 records contain nothing other than the
        date/time for the 5-minute timeslot, with all other fields empty.  These need to be
        filtered out at runtime, and if a later record for the same timeslot is reported, it
        should overwrite the original one.
            Written by DK Fowler ... 17-Oct-2019
    :param db_record:       List containing record data
    :return:                True, if record contents are blank, else False
    """

    global blank_rec_cnt_this_thermostat
    global blank_rec_cnt_total

    empty = True
    # Start checks with the 5th field in the record; first 5 fields contain date written,
    # thermostat name, id, runtime date, and runtime time fields.  (Indexed from 0 of course.)
    # logger.debug(F"Check for empty record:  {db_record}")
    # logger.debug(F"...number of fields:  {len(db_record)}")
    for field_idx in range(5, len(db_record)):
        # logger.debug(F"Field value:  '{db_record[field_idx]}', index: {field_idx}")
        if db_record[field_idx] != '':
            empty = False
            # logger.debug(F"Field value not blank...returning 'False'")
            break
    if empty:
        blank_rec_cnt_this_thermostat += 1
        blank_rec_cnt_total += 1

    return empty


def check_and_rewrite_duplicate_record(conn, db_record):
    """
        This routine will locate a duplicate record in the Ecobee runtime database, based on the primary
        key information from the passed db_record list.  It will then compare the database record
        against the passed record information to determine which has the most detailed information, and
        store it back to the database for this timeslot.

        This process is a workaround for the quirky way in which the Ecobee thermostats report their
        runtime data to the Ecobee service.  For the most recent timeslots, data is frequently reported
        incomplete.  Though this app will filter out completely blank records and not store them, if
        a partial data return is made, the record will be recorded potentially during an earlier run
        of the app.  This routine will attempt to resolve this by recording any new information for the
        given timeslot, rather than just fail on a store attempt for a duplicate record.
                Written by DK Fowler ... 18-Oct-2019
    :param conn:            Runtime database connection object
    :param db_record:       List containing current record data to be stored for a given timeslot
    :return:                True, if record contents are updated with the passed new record data, else False
    """
    runtime_fields = ['record_written_UTC',
                      'thermostat_name',
                      'thermostat_id',
                      'run_date',
                      'run_time',
                      'aux_heat1',
                      'aux_heat2',
                      'aux_heat3',
                      'comp_cool1',
                      'comp_cool2',
                      'comp_heat_1',
                      'comp_heat_2',
                      'dehumidifier',
                      'dmoffset',
                      'economizer',
                      'fan',
                      'humidifier',
                      'hvac_mode',
                      'outdoor_humidity',
                      'outdoor_temp',
                      'sky',
                      'ventilator',
                      'wind',
                      'zone_ave_temp',
                      'zone_calendar_event',
                      'zone_climate',
                      'zone_cool_temp',
                      'zone_heat_temp',
                      'zone_humidity',
                      'zone_humidity_high',
                      'zone_humidity_low',
                      'zone_hvac_mode',
                      'zone_occupancy']

    global dup_update_cnt_total
    global dup_update_cnt_this_thermostat

    # Build a SQL query string to find the original record for this timeslot in the database

    cur = conn.cursor()
    # Build SQL query string, including exclusion of "blank" records...
    db_str = "SELECT * FROM runtime WHERE thermostat_id=? AND run_date =? AND run_time=? "
    db_str += "ORDER BY run_date DESC, run_time DESC LIMIT 1"
    logger.debug(F"SQL for duplicate record query:  ")
    logger.debug(F"...{db_str}")

    try:
        cur.execute(db_str, (db_record[2], db_record[3], db_record[4],))
    except sqlite3.Error as e:
        logger.error(F"Error occurred while attempting to find original duplicate record for this timeslot "
                     F"in runtime database:  {e}")
        logger.error(F"...record search key:  thermostat: {db_record[2]}  "
                     F"run_date: {db_record[3]}  run_time: {db_record[4]}")
        cur.close()
        return False

    rows = cur.fetchall()

    # We have the original record; now compare its data against the passed new record data to see
    # which (if either) contains the most non-blank data
    db_record_data_cnt = 0
    org_record_data_cnt = 0
    for row in rows:
        for field_idx in range(5, len(db_record)):  # loop through the fields to compare; skip the index info
            if db_record[field_idx] != '':
                db_record_data_cnt += 1
            if row[field_idx] != '':
                org_record_data_cnt += 1

    logger.debug(F"Duplicate record data comparision:  original record from db count: {org_record_data_cnt}, "
                 F"new record data count: {db_record_data_cnt}")
    logger.debug(F"Database record: {rows[0]}")
    logger.debug(F"New record:      {db_record}")

    # If the database record contains the most data values, return without updating it.  Otherwise, update
    # the record with the new data.
    if org_record_data_cnt > db_record_data_cnt:  # Original database record most up-to-date
        cur.close()
        return False
    else:  # New record is more up-to-date
        record_written_UTC = datetime.utcnow()  # Get current UTC time for updating record
        record_written_UTC_str = datetime.strftime(record_written_UTC, "%Y-%m-%d %H:%M:%S")

        # Construct SQL update string
        db_update_str = "UPDATE runtime SET "
        # First field to update is the record-written date/time in UTC
        db_update_str += runtime_fields[0] + " = '" + record_written_UTC_str + "', "
        # Loop through data fields
        for field_idx in range(5, len(db_record)):
            db_update_str += runtime_fields[field_idx] + " = '"
            db_update_str += db_record[field_idx]
            if field_idx != len(db_record) - 1:
                db_update_str += "', "
            else:
                db_update_str += "'"
        db_update_str += " WHERE " + runtime_fields[2] + " = '" + db_record[2] + "'"
        db_update_str += " AND " + runtime_fields[3] + " = '" + db_record[3] + "'"
        db_update_str += " AND " + runtime_fields[4] + " = '" + db_record[4] + "'"

        logger.debug(F"...SQL update string for duplicate key: {db_update_str}")

        # Now try to execute the SQL update with the new data
        try:
            cur.execute(db_update_str)
            dup_update_cnt_total += 1  # global variable used for informational summary
            dup_update_cnt_this_thermostat += 1  # global variable used for informational summary
            logger.info(F"Record updated for key:  thermostat: {db_record[2]}  "
                        F"run_date: {db_record[3]}  run_time: {db_record[4]}")
        except sqlite3.Error as e:
            logger.error(F"Error occurred while attempting to update existing duplicate record in runtime database:  "
                         F"{e}")
            logger.error(F"...record search key:  thermostat: {db_record[2]}  "
                         F"run_date: {db_record[3]}  run_time: {db_record[4]}")
            logger.error(F"...new record:  {db_record}")
            logger.error(F"...orig record:  {rows[0]}")
            cur.close()
            return False

        cur.commit()
        return True


def construct_create_table_sql(table_name, attribute_name_map, attribute_type_map):
    """
        This routine will construct a SQLite CREATE TABLE SQL statement from the passed parameters.
        This can be used in subsequent calls to the SQLite Python API to create a new database
        table for the passed table name.

        The attribute names and datatypes come from the object definitions in the supplied Pyecobee
        library.

        In addition to returning a string containing the SQL statement to create the table, it will
        also return a dictionary containing field name and datatype for subsequent use in the
        application.
                Written by DK Fowler ... 23-Oct-2019
    :param table_name:          Database table to create (if it doesn't exist)
    :param attribute_name_map:  Dictionary containing field names for the database table
    :param attribute_type_map:  Dictionary containing data types for the fields
    :return datatype_dict:      Dictionary containing field name as key with datatype returned
    :return: SQL_create_str:    SQL string to create the table
    """

    # attrib_range = range(0, len(attribute_name_map.keys()), 2)

    attribute_name_list = [field for field in
                           attribute_name_map.keys() if ("_" in field or
                                                         attribute_name_map[field] == field) and
                           field not in thermostat_object_dict]
    # Create a SQLite datatype dictionary, with the key being field name from the attribute_name_map passed
    # and the value being the datatype (translated from the passed Python datatype in the attribute_type_map)
    datatype_dict = create_sqlite_datatype_dict(attribute_name_list, attribute_type_map)
    # print(F"Datatype dictionary:  {datatype_dict}")
    # Now scan the created datatype dictionary looking for lists.  These require further (recursive)
    # processing.
    #       Futures:  list processing...
    lists_dict = {}
    '''
    lists_dict = {list_item: list_type for (list_item, list_type) in
                  datatype_dict}
    # datatype_dict if 'List' in datatype_dict)

    # key: value for (key, value) in iterable
    for list_item, list_type in lists_dict:
        print(F"...list dictionary item:  {list_item}: {list_type}")
    '''
    logger.debug(F"SQLite datatype dictionary created:  {datatype_dict}")

    # Construct SQL table create string
    SQL_create_str = "CREATE TABLE IF NOT EXISTS " + table_name + " ("

    # First fields form the primary key, and are the same for all the supplemental tables
    SQL_create_str += "record_written_UTC TEXT NOT NULL, "
    SQL_create_str += "thermostat_name TEXT NOT NULL, "
    SQL_create_str += "thermostat_id TEXT NOT NULL, "

    # Loop through data fields
    for field_idx in range(0, len(datatype_dict)):
        # don't include fields with datatype of 'List', objects defined by the API, and exclude
        # fields for 'name' and 'identifier' if the table is 'thermDevices', as these are in the
        # standard key for all tables
        if ('List' not in datatype_dict[attribute_name_list[field_idx]]) and \
                (attribute_name_list[field_idx] not in thermostat_object_dict) and \
                not (table_name == 'thermDevices' and (attribute_name_list[field_idx] == 'name') or
                     (attribute_name_list[field_idx] == 'identifier')):
            SQL_create_str += attribute_name_list[field_idx] + " "
            SQL_create_str += datatype_dict[attribute_name_list[field_idx]]
            SQL_create_str += ", "
        # else:
            # print(F"Found list element in datatype, will require further processing...")
            # Add additional logic here

    SQL_create_str += "PRIMARY KEY (thermostat_id, record_written_UTC) ); "

    logger.debug(F"SQL {table_name} table create string: ")
    logger.debug(F"...{SQL_create_str}")

    return SQL_create_str, datatype_dict, lists_dict


def create_sqlite_datatype_dict(db_fields, db_datatypes_dict):
    """
        This routine will construct a dictionary containing a key for a field name and a
        value for the SQLite datatype.  The passed datatype list will be converted as
        appropriate from a Python datatype to a SQLite one.
                Written by DK Fowler ... 23-Oct-2019
    :param db_fields:           List containing the field name for the dictionary
    :param db_datatypes_dict:   Dictionary containing the Python datatypes for the fields
    :return:                    Dictionary with key of field name and value of SQLite datatype
    """
    """
        Translate the pass Python datatypes to appropriate SQLite ones...e.g., boolean is
        stored as INTEGER; six.text_type as TEXT; and int as INTEGER.
    """
    datatype_dict = {}
    for db_field in db_fields:
        datatype_pyth = db_datatypes_dict[db_field]
        # translate the Python datatype to one for SQLite
        if 'List' in datatype_pyth:  # Special recursive processing required for lists
            # This check must be done first, as list elements will have a datatype associated as well,
            # triggering the checks following.  We want to set the datatype as 'List' for now
            datatype_sql = datatype_pyth
        elif 'bool' in datatype_pyth:
            datatype_sql = 'INTEGER'
        elif 'int' in datatype_pyth:
            datatype_sql = 'INTEGER'
        elif 'six.text_type' in datatype_pyth:
            datatype_sql = 'TEXT'
        else:
            datatype_sql = 'TEXT'  # default datatype is TEXT if unidentified
        logger.debug(F"Translated datatype for {db_field}: {datatype_pyth} --> {datatype_sql}")
        datatype_dict.update({db_field: datatype_sql})

    logger.debug(F"Count of fields/datatypes:  {len(datatype_dict)}")

    return datatype_dict


def get_snapshot(conn,
                 db_table,
                 thermo_object,
                 thermostat_response_object):
    """
        This routine will retrieve a snapshot of records for each thermostat and attempt to
        store these in a SQLite database.  The data being retrieved is specified by the calling
        routine and passed through the parameters.
                Written by DK Fowler ... 25-Oct-2019
    :param conn                         The SQLite database connection
    :param db_table:                    The SQLite database table to which records are to be written
    :param thermo_object:               The thermostat response sub-class from which the values are obtained
    :param thermostat_response_object:  Thermostat response object from API call with thermostat details
    :return lists_dict:                 Dictionary returned containing more list fields requiring processing, if detected
"""

    global db_table_recs_written  # global dictionary used for storing recs written counters

    try:
        if thermo_object != "":
            thermo_subclass_top = eval("thermostat_response_object[0]." + thermo_object)
        else:
            thermo_subclass_top = thermostat_response_object[0]

    except AttributeError as e:
        logger.error(F"Incorrect attribute specified for Thermostat object:  {thermo_object}...{e}")
        sys.exit(1)

    # print(F"Thermostat subclass top:  {thermo_subclass_top}")
    # print(F"Attrib name map:  {thermostat_response_object[0].device.attribute_name_map}")

    # Create a SQL statement to create the table; also return a dictionary with field names, datatypes,
    # and a dictionary with list data requiring further processing
    try:
        create_table_sql_str, table_fields_dict, lists_dict = \
            construct_create_table_sql(db_table,
                                       thermo_subclass_top.attribute_name_map,
                                       thermo_subclass_top.attribute_type_map)
        # print(F"Lists dictionary returned to get_snapshot:  {lists_dict}")
    except AttributeError as e:  # catch no data available for this attribute
        logger.debug(F"No data available for table:  {db_table}, object:  {thermo_object}...{e}")
        print(F"No data available for table:  {db_table}, object:  {thermo_object}...{e}")
        return

    # Loop through the thermostats, creating records for this table
    # for thermo in thermostat_response.thermostat_list:
    for thermo in thermostat_response_object:
        #   Create the settings table if it doesn't already exist
        if conn is not None:
            # First, check if the table exists:
            # Create SQL string for creating the table
            create_table_success = create_table_from_string(conn, db_table, create_table_sql_str)
            if create_table_success:
                create_record_status = False  # assume failure
                if thermo_object != "":
                    thermo_subclass_complete = eval("thermo." + thermo_object)
                else:
                    thermo_subclass_complete = thermo
                # print(F"Thermo subclass complete:  {thermo_subclass_complete}")
                # Table successfully created or already exists.  Now write records.
                create_record_status = create_database_record(conn,
                                                              db_table,
                                                              thermo.name,
                                                              thermo.identifier,
                                                              table_fields_dict,
                                                              thermo_subclass_complete)
                if create_record_status:
                    logger.debug(F"...{db_table} record written successfully for {thermo.name}")
                    db_table_recs_written[db_table] += 1
            else:
                # Error creating table...
                logger.error(F"Error occurred creating table {db_table}...aborting")
                conn.close()  # close the db connection
                sys.exit(1)

    return lists_dict  # Dictionary with more data to process in lists


if __name__ == '__main__':
    main()
