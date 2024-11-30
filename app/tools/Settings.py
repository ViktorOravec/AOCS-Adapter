import json
import os
from dotenv import load_dotenv
from typing import Optional
import logging

logger = logging.getLogger(__name__)

load_dotenv()

SETTINGS_FILE: Optional[str] = os.getenv('SETTINGS_FILE')

# Singleton class to connect to an InfluxDB service
class Settings:
    _instance: Optional['Settings'] = None
    _settings: dict = {}

    def __new__(cls, *args, **kwargs) -> 'Settings':
        if not cls._instance:
            cls._instance = super(Settings, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        self.initialized = True
        self._load_settings_file()

    def _load_settings_file(self) -> None:
        # check if SETTINGS_FILE is set
        if not SETTINGS_FILE:
            logger.error("SETTINGS_FILE is not set")
            exit(1)
        # check if file exists
        if not os.path.exists(SETTINGS_FILE):
            logger.error(f"SETTINGS_FILE {SETTINGS_FILE} does not exist")
            exit(1)
        # load data points from file and store them in a list self.enabled_data_points
        with open(SETTINGS_FILE) as f:
            try:
                # load settings string, convert to dict and store in self._settings
                settings = f.read()
                json_settings = json.loads(settings)
                self._settings = json_settings
            except Exception as e:
                logger.error(f"Error loading settings file: {e}")
                exit(1)

    def get_enabled_data_points(self) -> list:
        if(self._settings):
            return self._settings['enabled_data_points']
    def get_disabled_data_points(self) -> list:
        if(self._settings):
            return self._settings['disabled_data_points']
    def get_items(self) -> dict:
        return self._settings["items"]

