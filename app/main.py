import signal
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.openapi.utils import get_openapi
from app.core.registrationHandler import initRegistrationCheck
from app.routers.dataProcessingRouter import router as data_processing_router
from app.routers.dataConsumptionRouter import router as data_consumption_router
# from app.tools.logger import CustomLogger
from app.core.Scheduler import Scheduler
import logging


logger = logging.getLogger(__name__)


app = FastAPI(
    title="CSV Processing API",
    description="API to process CSV files",
    version="0.0.1",
)

# Include the CSV router
app.include_router(data_processing_router, prefix="/processing", tags=["CSV Processing"])
app.include_router(data_consumption_router, prefix="/consumption", tags=["Data Consumption"])

@app.get("/", include_in_schema=False)
def read_root():
    return


@app.exception_handler(404)
async def not_found_handler(request, exc):
    response = {"message": "Not Found", "statusCode": 404}
    return JSONResponse(status_code=404, content=response)

@app.get("/openapi.json", include_in_schema=False)
def get_openapi_endpoint():
    """
    Retrieve the generated OpenAPI schema.
    """
    return JSONResponse(content=get_openapi(
        title="CSV Processing API",
        version="0.0.1",
        description="",
        routes=app.routes,
    )())

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down the service")
    scheduler.stop()

initRegistrationCheck()


# plan scheduler in 10 seconds
scheduler = Scheduler()
scheduler.start()

