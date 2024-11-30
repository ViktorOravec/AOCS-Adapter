# app/routers/csvProcessing.py

import csv
import io
import logging
from typing import Annotated
from fastapi import APIRouter, Body, Request, Response, HTTPException
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from datetime import datetime
from typing import Optional
from app.tools.logger import CustomLogger
from fastapi.security import HTTPBasic, HTTPBasicCredentials  
from app.core.Scheduler import Scheduler

security = HTTPBasic()
logger = logging.getLogger(__name__)



from app.microservicies.ObjectStorageConnector import ObjectStorageConnector
from app.microservicies.InfluxConnector import InfluxConnector

router = APIRouter()
s3connector = ObjectStorageConnector()
influxconnector = InfluxConnector()

logger = CustomLogger(name='CSVProcessingRouter')

@router.get("/health",
            summary="Health Check",
            description="Check the health of the CSV processing service",
            # return json {"s3": True, "influx": True}
            responses={
                200: {"description": "Service is healthy", "content": {"application/json": {"example": {"s3": True, "influx": True}}}},
                503: {"description": "Service is unhealthy"}
            }
    )
async def get_health():
    """
    Endpoint to check the health of the service.
    """
    try:
        logger.debug("Checking health of the service")
        # Check if the service is healthy
        # return json {"s3": True, "influx": True}
        s3status = s3connector.is_healthy()
        influxstatus = influxconnector.is_healthy()
        return {"s3": s3status, "influx": influxstatus}
    except Exception as e:
        logger.error(f"Error checking health of the service: {e}")
        return Response(status_code=503)

@router.post("/notify",
            summary="Notify",
            description="Notify the service that a new file has been uploaded",
            responses={
                200: {"description": "Notification received"},
                400: {"description": "Bad Request. Invalid notification format"}
            }
    )
async def put_notify(request: Request, credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    """
    Endpoint to notify the service that a new file has been uploaded.
    """
    try:
        logger.info("Received a notification")
        Scheduler().process_on_demand()
        # Check if the Content-Type is 'application/json'
        return Response(status_code=200)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error processing notification: {e}")
        raise HTTPException(status_code=400, detail="Invalid notification format")


@router.post("/upload_csv",
             summary="Upload CSV file",
             description="Upload a CSV file for processing",
             responses={
                200: {"description": "CSV file accepted for processing","content": None},
                415: {"description": "Unsupported Media Type. Only .csv files are allowed.", "content": None},
                400: {"description": "Bad Request. Invalid file format or other issues."}
                },
    )
async def post_csv(request: Request, credentials: Annotated[HTTPBasicCredentials, Depends(security)], file: bytes = Body(..., media_type="text/csv") , file_name: Optional[str] = None):
    """
    Endpoint to accept a CSV file for processing.
    """
    try:
        logger.debug("Received a CSV file for processing")
        # Check if the Content-Type is 'text/csv'
        content_type = request.headers.get("Content-Type")
        if content_type != "text/csv":
            raise HTTPException(status_code=415, detail="Only 'text/csv' files are allowed.")
        
        # Generate file name - take whatever is in query parameter and append timestamp (remove.csv if present)
        if file_name is None:
            file_name = f"file_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        else:
            file_name = file_name.split(".")[0] + f"_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        
        
        # Log file size
        file_size = len(file)
        fileStr = ""
        try:
            # try utf-8
            fileStr = file.decode("utf-8")
        except UnicodeDecodeError:
            # try Windows-1252
            fileStr = file.decode("Windows-1252")
        file_stream = io.BytesIO(fileStr.encode("utf-8"))
        # parse csv
        csv_reader = csv.reader(io.StringIO(fileStr))
        # number of rows
        num_rows = len(list(csv_reader))
        logger.debug(f"Uploaded file  {file_name} with size {file_size} bytes and {num_rows} rows")
        # Push to object storage
        s3connector.push_to_storage(file_stream, file_name)
        Scheduler().process_on_demand()
        return Response(status_code=200, content=None)
        # Return a 200 OK response without any message
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        raise HTTPException(status_code=400, detail="Invalid file format or other issues.")
