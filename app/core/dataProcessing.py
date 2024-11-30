
import csv
import io
import logging

import pytz
from datetime import datetime
from app.tools.Settings import Settings
from influxdb_client import Point

logger = logging.getLogger(__name__)

from app.microservicies.ObjectStorageConnector import ObjectStorageConnector
from app.microservicies.InfluxConnector import InfluxConnector

influxconnector = InfluxConnector()
s3connector = ObjectStorageConnector()
settings = Settings()


# list unprocessed csv files from s3, process them (push to influxdb), mark as processed in s3 (tag)
def dataProcessing():
    try:
        logger.info("Processing data")
        # List unprocessed files from S3
        unprocessed_files = s3connector.list_unprocessed_files()
        logger.info(f"There are {len(unprocessed_files)} unprocessed files")
        enabled_data_points = settings.get_enabled_data_points()
        disabled_data_points = settings.get_disabled_data_points()
        this_run_discovered_data_points = []
        for file in unprocessed_files:
            logger.info(f"Processing file: {file}")
            # Process file
            data = s3connector.get_file(file)
            if not data:
                logger.error(f"Failed to get file {file}")
                return
            # parse csv file
            #convert binary to string
            # set delimiter to
            dataString = data.getvalue().decode('utf-8').splitlines()
            measurements = csv.DictReader(dataString, delimiter=';')
            data_points = []

            for m in measurements:
                object_code=m['KOD_OBJEKTU']
                meter_code=m['KOD_MERACA']
                value=float(m['POCITADLO'])
                meter_name= m['NAZOV_MERACA']
                energia=m['ENERGIA']
                timestamp_csv=m['PM_TIME']
                local_timezone = pytz.timezone("Europe/Bratislava")  
                timestamp_local = datetime.strptime(timestamp_csv, "%d.%m.%Y %H:%M")
                timestamp_utc = local_timezone.localize(timestamp_local).astimezone(pytz.utc)
                id = object_code + "_" + meter_code

                # check if id is enabled
                d = enabled_data_points.get(id)
                if d is None:
                    if id not in disabled_data_points and id not in this_run_discovered_data_points:
                        logger.info(f"Data point {meter_name} [{id}] is not enabled, skipping")
                        this_run_discovered_data_points.append(id)
                        pass
                    continue

                # build influx format
                point = (
                    Point("koor_processed_data") 
                    .tag("energy", energia)   
                    .tag("meter_name", meter_name)
                    .tag("object_code", object_code)
                    .field(id, value)
                    .time(timestamp_utc)          
                )
                # push to influx queue
                data_points.append(point)            

            # Write all data points to InfluxDB
            influxconnector.write_multiple_data(data_points)
            # Mark file as processed
            logger.info(f"Marking file as processed: {file}")
            s3connector.mark_file_as_processed(file)
        logger.info("Data processing complete")
    except Exception as e:
        logger.error(f"Error processing data: {e}")