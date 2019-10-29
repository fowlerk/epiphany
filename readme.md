# ECC Ecobee
## Epiphany Catholic Church - Ecobee Thermostat Data Retrieval

## Overview
This application will poll information for the Ecobee thermostats for Epiphany Catholic Church and write the resultant data to a SQLite database.

The data retrieved is extensive -- the Ecobee service provides detailed data covering over two dozen record types (even more for utility customers).  Some of this data is maintained on a historical basis, for our purposes here, primarily historical runtime information.  This data is reported from the thermostats every 15 minutes or so (more frequently on equipment status changes), and is maintained on intervals of every 5 minutes.  This historical data is quite large, with 288 records maintained for each thermostat for each day (12 5-min. increments * 24 hours).  So, retrieving 90 days of information will result in almost 26,000 records for one thermostat.

Additional thermostat details are available as a snapshot in time for most of the remaining record types.  This includes point-in-time information on items such as current settings, notifications, programs, events, firmware version, etc.

The application will first poll the historical runtime information for each thermostat.  The timeframe for each iteration is determined by examining the existing data from the SQLite database to determine the last revision interval (5-minute time slice) that was reported and written.  It will then poll the Ecobee service to determine the latest interval that was reported to the service.  If newer information is available, it will then be requested.  This process follows the recommendations given on the Ecobee site to avoid unnecessary polling requests (see [Thermostat Summary Polling](https://www.ecobee.com/home/developer/api/documentation/v1/operations/get-thermostat-summary.shtml) for more details).

If no runtime information is currently available in the database, the application will default to using the first-reported date retrieved for this thermostat from the Ecobee service (essentially the in-service date).  Data requests for the runtime information only support up to 31 days retrieval per request; therefore, larger requests must be broken up into smaller ones not to exceed this limit.  The application automatically determines the retrieval window and will adjust the polling requests to accomodate this restriction.  This is generally only an issue during the first-run, when several months of data is to be retrieved for each thermostat, beginning with the in-service date.

Following the polling of the runtime information, data is retrieved for the point-in-time or snapshot data.  One record is written for each data-type retrieved for each thermostat; so, the number of records written for each data-type is determined by the frequency by which the application is run.  That is, if it is run hourly, one record will be retrieved per thermostat, per data-type, for each hour.  Or, one per day, per thermostat, per data-type if run daily.
## Quick Start
In order to perform the initial setup for running the application:
1. Install the Pyecobee library from the included archive file:  'pip install ./Pyecobee-mumblepins.zip -v'
This installs the library into the local Python environment from the provided archive.  (Note:  for other than Windows platforms, it may be necessary to download the newest archive from here:  [Pyecobee - mumblepins](https://github.com/mumblepins/Pyecobee)
2. Edit the application to configure various settings (ECCPycobee v01.00.py)
   * log file path
   * logging level
   * database location
   * authorization tokens file location
   * thermostat revision interval file location
3. Schedule the application to run on a recurring basis.  Suggested scheduling is to run once each hour in order to collect snapshot data hourly.

## SQLite Data Structure
All data associated with the Ecobee thermostats is written to one database with multiple tables.  The application will determine if the database exists, and if not, will create it automatically.  This is also true for the tables within the database.  As described above, there are many different record types associated with the details of a thermostat, and tables are created for each record type.
The runtime historical data is keyed based on the combination of thermostat id, runtime date, and runtime time as reported from the thermostat.  As the data is reported and maintained by the service in local thermostat time, there likely will be gaps and / or overwrite situations that occur as a result of daylight savings time changes.  As of the initial version of this application, no allowances are made for this limitation, as it appears to be a restriction in the implementation by Ecobee.
All snapshot data tables are keyed based on a combination of thermostat id and record-written date/time in UTC.  Given the timestamp when the records are written, there should be no issues with gaps or duplicates as with the historical runtime data.
### Blank (empty) and duplicate records
The data returned from the Ecobee service for the historical runtime records sometimes contains only the time slot date/time stamp and thermostat identification -- all other data fields are set to '0'.  This seems to occur due to two conditions. The first is for situations where the thermostat was not connected to the network for a sufficiently-long timeframe, resulting in a loss of some of the 5-minute time slots.  The second situation occurs when the API request window includes the most recent data available from the service for the requested thermostat.  In this case, it is common for the last 8-10 records returned to include only partial or all-zero data.  Subsequent requests for the same window will then return valid or more complete data for the same time slots.

The application handles all-zero record returns by ignoring these and not writing them to the database.  These are counted and reported in the statistics for each run however.  (There may be some argument that these should be recorded, as they would indicate times when the thermostat was not connected; however, this can be inferred from the missing time slots in the database.)  On successive iterations of the application, if a duplicate key is detected for a given time slot and thermostat, the existing data in the database is compared against the latest data returned from the service.  The record with the most non-zero data will be recorded in the database.

## Logging
The application makes use of the Python logging service to provide information regarding each run iteration.  The log-level may be set to various levels to indicate the desired level of detail to log; initially this is set to DEBUG, the highest level of detail.  This is recommended for the first month or so of running new versions to ensure any bugs are logged appropriately.  After that time period, the log level can be set lower (such as INFO) to limit the size of the log file.

## Pyecobee Library
The application makes use of a library that wraps the Ecobee API's into a simple but powerful set of API calls that return native Python objects.  This library was originally written by [@sfanous](https://github.com/sfanous/Pyecobee);  it is well documented here along with all of the object definitions, and Python getter / setter functions.  Unfortunately, it doesn't seem to be maintained any longer, which is a problem as new fields are added to the object definitions / Ecobee API calls.  There are a number of forks for this that DO seem to be maintained; the one I've chosen to use is by Daniel Sullivan(mumblepins), here:  [Pyecobee mumblepins](https://github.com/mumblepins/Pyecobee).  This library must first be installed into the local Python environment in order to use the ECC Ecobee application.

## Ecobee API and Date / Time
The underlying Ecobee API expects some API requests to be in thermostat time (local time), while others expect it to be in UTC form.  The library methods used here that accept a datetime object as an argument expects the argument to be passed in thermostat time.  The datetime object passed must be a timezone aware object.  The method will then either use the passed in datetime object as is, or convert it to its UTC time equivalent depending on the requirements of the ecobee API request being executed.  This is another advantage (consistency at least!) in the use of the library
vs. the core Ecobee service routines.

## Authorization and Access to the Ecobee Service
The ecobee API is based on extensions to the OAuth 2.0 framework.  See here: [Authorization and Access](https://www.ecobee.com/home/developer/api/documentation/v1/auth/auth-intro.shtml) for details on how this is implemented.  The application makes the necessary calls to ensure updated access tokens are produced and / or refreshed as they expire.  If all else fails and these somehow become corrupted, detailed steps will be logged in order to re-authorize the application through the Ecobee portal.

## Error handling
I have spent quite a bit of effort in identifying and handling the most common errors that can occur.  In most cases, if a severe error occurs, re-running the application at a later time should result in success without causing issues with data integrity, etc.

