# File name: ecobee_client.py
# Author: Abel Alhussainawi, Cody Gividen, Zach Ballard
# Created: 2/29/2025
# License: BSD licenses
#
# Description: This file contains functions to authenticate with Ecobee
# and schedule mode changes

import os
import pytz
import json
import logging
import requests

from datetime import datetime
from six.moves import input
from pyecobee import *

# This is one day of the schedule
_daily_schedule_pattern = ["sleep"] * 12 + ["home"] * 24 + ["sleep"] * 6

# Common settings to all climates
_base_climate_settings = {
    "coolFan": "auto",
    "heatFan": "auto",
    "vent": "off",
    "ventilatorMinOnTime": 20,
    "owner": "system",
    "type": "program",
}

_schedule_payload = {
  "selection": {
    "selectionType":"thermostats",
    "selectionMatch":""
  },
  "thermostat": {
      "program": {
        "schedule": [
            # Create 7 distinct copies of the daily pattern for the
            # schedule
            list(_daily_schedule_pattern) for _ in range(7)
       ],
       "climates": [
          {
              # Unpack the common climate settings
              **_base_climate_settings,
              "name": "Unoccupied",
              "climateRef": "away",
              "isOccupied": False,
              "isOptimized": True,
              "colour": 9021815,
              "coolTemp": 821,
              "heatTemp": 601,
          },
          {
              # Unpack the common climate settings
              **_base_climate_settings,
              "name": "Occupied",
              "climateRef": "home",
              "isOccupied": True,
              "isOptimized": False,
              "colour": 13560055,
              "coolTemp": 720,
              "heatTemp": 700,
          },
          {
              # Unpack the common climate settings
              **_base_climate_settings,
              "name": "Overnight",
              "climateRef": "sleep",
              "isOccupied": True,
              "isOptimized": False,
              "colour": 2179683,
              "coolTemp": 781,
              "heatTemp": 661,
          },
        ]
     }
  }
}

class EcobeeClient:
    def __init__(self, thermostat_name="Test1", credentials_file=None, config=None):
        """
        Initializes the EcobeeClient, sets up the file paths and starts authentication.
        """
        self.credentials_file = credentials_file
        self.schedule_payload = _schedule_payload
        self.config = config
        self.thermostat_name = thermostat_name
        self.ecobee_service = None
        self.ECOBEE_THERMOSTAT_URL = "https://api.ecobee.com/1/thermostat"
        self.authenticate()

    def authenticate(self):
        """Authenticates the client with Ecobee, using either stored tokens,
        environment variables, or interactive authorization"""
        logging.info("Starting Ecobee authentication process...")

        logging.info(f"Using credentials from {self.credentials_file}.")
        with open(self.credentials_file, 'r') as f:
            creds_data = json.load(f)
            application_key = creds_data.get('application_key')
            access_token = creds_data.get('access_token')
            refresh_token = creds_data.get('refresh_token')

            if not all([application_key, access_token, refresh_token]):
                error_msg = "Missing required fields in credentials.json: 'application_key', 'access_token', 'refresh_token'"
                logging.error(error_msg)
                exit(1)

        self.ecobee_service = EcobeeService(
            thermostat_name=self.thermostat_name,
            application_key=application_key,
            access_token=access_token,
            refresh_token=refresh_token
        )

        try:
            self.refresh_tokens()
            logging.info("Initial token refresh successful.")
        except EcobeeApiException as e:
            logging.warning(f"Refresh failed: {e}. Attempting to reauthorize...")
            self.authorize()
            self.request_tokens()

    def authorize(self):
        """Perform interactive authorization by generating a PIN."""
        logging.info("Starting authorization process...")
        print("Starting authorization process...")
        authorize_response = self.ecobee_service.authorize()
        logging.debug(f"AuthorizeResponse: {authorize_response.pretty_format()}")
        pin = authorize_response.ecobee_pin
        logging.info(
            f"""Please go to https://ecobee.com, login to the web
portal and click on the "Profile" tab.  Click on the "My Apps" option
in the menu on the right. In the "My Apps" widget, select "Add
Application".  Paste "{pin}" into the textbox labeled "Enter PIN code"
and then click "Validate".  The next screen will display any
permissions the app requires and will ask you to click "Authorize" to
add the application.

After completing this step, press Enter to continue."""
        )
        input()

    def request_tokens(self):
        """Request initial tokens after authorization (if you're interactive)."""
        logging.info("Requesting tokens...")
        try:
            token_response = self.ecobee_service.request_tokens()
            logging.debug(f"TokenResponse: {token_response.pretty_format()}")
            print(f"New Access Token: {self.ecobee_service.access_token}")
            print(f"New Refresh Token: {self.ecobee_service.refresh_token}")
        except EcobeeApiException as e:
            logging.error(f"Error requesting tokens: {e}")
            raise

    def refresh_tokens(self):
        """Refresh the access and refresh tokens."""
        logging.info("Refreshing tokens...")
        try:
            token_response = self.ecobee_service.refresh_tokens()
            logging.debug("TokenResponse received.")
            logging.info("Tokens refreshed successfully.")
        except EcobeeApiException as e:
            logging.error(f"Error refreshing tokens: {e}")
            raise

    def refresh_tokens_if_needed(self):
        """Refresh tokens if the access token has expired."""
        now_utc = datetime.now(pytz.utc)
        logging.debug(f"Checking token expiration: now={now_utc}, expires_on={self.ecobee_service.access_token_expires_on}")
        if (self.ecobee_service.access_token_expires_on is not None and
            now_utc > self.ecobee_service.access_token_expires_on):
            self.refresh_tokens()

    def schedule_mode_change(self, dataframe, target_ecobee):
        """
        Applies a weekly schedule from a dataframe to the target Ecobee thermostat.
        Args:
        dataframe (pd.DataFrame): Schedule for each day of the week.
        target_ecobee (str): Thermostat name to update.

        """
        self.refresh_tokens_if_needed()

        ordered_days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        masked_schedule = [dataframe[day].tolist() for day in ordered_days]
        self.schedule_payload["thermostat"]["program"]["schedule"] = masked_schedule

        climate_ref_to_config_name = {
            "home": "Occupied",
            "away": "Unoccupied",
            "sleep": "Overnight"
        }

        for climate in self.schedule_payload["thermostat"]["program"].get("climates", []):
            mode_ref = climate.get("climateRef", "").lower()
            config_key = climate_ref_to_config_name.get(mode_ref)

            if config_key and config_key in self.config["modes"]:
                mode_config = self.config["modes"][config_key]
                climate["coolTemp"] = mode_config["max temperature"] * 10
                climate["heatTemp"] = mode_config["min temperature"] * 10
                logging.info(
                    f"Updated climate '{mode_ref}' (mapped to '{config_key}'): coolTemp={climate['coolTemp']}, heatTemp={climate['heatTemp']}")

        thermostat_id = self.get_thermostat_id_by_name(target_ecobee)
        if not thermostat_id:
            logging.error(f"Thermostat {target_ecobee} not found.")
            raise ValueError(f"Thermostat {target_ecobee} not found.")

        self.schedule_payload['selection']['selectionMatch'] = thermostat_id

        response = requests.post(
            self.ECOBEE_THERMOSTAT_URL,
            data=json.dumps(self.schedule_payload, default=str),
            headers={'Authorization': 'Bearer ' + self.ecobee_service.access_token}
        )

        if response.ok:
            logging.info(f'Thermostat {target_ecobee} updated successfully.')
            print(f'Thermostat {target_ecobee} updated.')
            print(response.text)
        else:
            logging.error(f'Failed to update thermostat {target_ecobee}: {response.status_code}')
            print('Failure sending request to thermostat')
            print(response.text)
            raise Exception(f"Thermostat update failed: {response.text}")

    def get_thermostat_id_by_name(self, ecobee_name):
        """
        Given a thermostat name, return its identifier.
        """
        self.refresh_tokens_if_needed()
        selection = Selection(selection_type=SelectionType.REGISTERED.value, selection_match='')

        try:
            thermostat_response = self.ecobee_service.request_thermostats(selection)
            for tstat in thermostat_response.thermostat_list:
                if tstat.name.lower() == ecobee_name.lower():
                    logging.debug(f"Found thermostat {ecobee_name} with ID {tstat.identifier}")
                    return tstat.identifier
            logging.warning(f"Thermostat {ecobee_name} not found in registered thermostats.")
            return None
        except EcobeeApiException as e:
            logging.error(f"Failed to fetch thermostats: {e}")
            raise

    def format_dt_str(self, dt_val):
        """
        Convert a datetime object or ISO datetime (or date) string to a friendly, readable format.
        """
        return dt_val.strftime("%b %d, %Y %I:%M %p %Z")
