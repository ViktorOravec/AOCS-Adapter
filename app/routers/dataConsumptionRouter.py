import json
import logging
import time
from fastapi import APIRouter, Request, Response, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from app.microservicies.InfluxConnector import InfluxConnector
from app.tools.Settings import Settings
from app.tools.logger import CustomLogger


# app/routers/dataConsumption.py


security = HTTPBasic()
logger = logging.getLogger(__name__)
influxconnector = InfluxConnector()


router = APIRouter()

# define router getProperty, no credentials adapterId in query and propertyName in query
@router.get("/{adapterId}/property/{pid}",
            summary="Get Property",
            description="Get the value of a property",
            responses={
                200: {"description": "Property value returned"},
                400: {"description": "Bad Request. Invalid parameters"}
            }
    )
async def get_property(request: Request, adapterId: str = "", pid: str = "", startTimestamp: str = "", stopTimestamp: str = ""):
    """
    Endpoint to get the value of a property.
    """
    try:
        # start time for measuring how long the function takes
        starttime = time.time()
        logger.info(f"Getting property {pid} for adapter {adapterId} from {startTimestamp} to {stopTimestamp}")
        # call influxdb to get the value of the property
        if pid == "getAll":
            # we have adapterId -> we need to find object id and all monitored properties
            # it is in settings.json
            items = Settings().get_items()
            enabled_data_points = Settings().get_enabled_data_points()
            item = None
            for i in items:
                if i.get('adapterid') == adapterId:
                    item = i
                    break
            if item is None:
                logger.error(f"Adapter {adapterId} not found in settings")
                # respond with error json 
                raise HTTPException(status_code=404, detail="Adapter: "+adapterId+" not found")
            properties_ids = item.get('properties')
            data = {}
            middle_time = time.time()
            logger.info(f"Middle time for getting properties for adapter {adapterId} took {middle_time-starttime} seconds")
            import concurrent.futures

            def fetch_property_data(pid):
                title = enabled_data_points[pid].get('title')
                return title, influxconnector.getData(pid, startTimestamp, stopTimestamp)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_pid = {executor.submit(fetch_property_data, pid): pid for pid in properties_ids}
                for future in concurrent.futures.as_completed(future_to_pid):
                    pid = future_to_pid[future]
                    try:
                        title, property_data = future.result()
                        data[title] = property_data
                    except Exception as exc:
                        logger.error(f"Error fetching data for property {pid}: {exc}")
            # end time for measuring how long the function takes
            endtime = time.time()
            logger.info(f"Getting all properties for adapter {adapterId} took {endtime-middle_time} seconds")
            return data
        else:
            data = influxconnector.getData(pid, startTimestamp, stopTimestamp)
            endtime = time.time()
            logger.info(f"Getting property {pid} for adapter {adapterId} took {endtime-starttime} seconds")
            return data
    except HTTPException as e:
        raise
    except Exception as e:
        logger.error(f"Error getting property {pid} for adapter {adapterId} from {startTimestamp} to {stopTimestamp}: {e}")
        raise HTTPException(status_code=404, detail="Property: "+pid+" not found")