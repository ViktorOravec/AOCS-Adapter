import os
from dotenv import load_dotenv
from typing import Optional
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

logger = logging.getLogger(__name__)


load_dotenv()

influx_host: Optional[str] = os.getenv('INFLUX_HOST')
influx_port: Optional[int] = int(os.getenv('INFLUX_PORT', 8086))
influx_protocol: Optional[str] = os.getenv('INFLUX_PROTOCOL', 'http')
influx_token: Optional[str] = os.getenv('INFLUX_TOKEN')
influx_organization: Optional[str] = os.getenv('INFLUX_ORGANIZATION')
influx_bucket: Optional[str] = os.getenv('INFLUX_BUCKET')

# Singleton class to connect to an InfluxDB service
class InfluxConnector:
    _instance: Optional['InfluxConnector'] = None

    def __new__(cls, *args, **kwargs) -> 'InfluxConnector':
        if not cls._instance:
            cls._instance = super(InfluxConnector, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, 'initialized'):
            url = f"{influx_protocol}://{influx_host}:{influx_port}"
            logger.info("Initializing InfluxDB client: "+url)
            self.client = influxdb_client.InfluxDBClient(
                url=url,
                token=influx_token,
                org=influx_organization
            )
            self.writeApi = self.client.write_api(write_options=SYNCHRONOUS)
            self.initialized = True

    def is_healthy(self) -> bool:
        try:
            # check if the client can connect to the InfluxDB server
            self.client.ready()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")
            return False

    def write_multiple_data(self, points: list[influxdb_client.Point]) -> None:
        logger.debug("Writing data to InfluxDB")
        try:
            self.writeApi.write(bucket=influx_bucket, record=points)
        except Exception as e:
            logger.error(f"Failed to write data to InfluxDB: {e}")
            raise e
    def write_single_data(self, point: influxdb_client.Point) -> None:
        logger.debug("Writing data to InfluxDB")
        try:
            self.writeApi.write(bucket=influx_bucket, record=point)
        except Exception as e:
            logger.error(f"Failed to write data to InfluxDB: {e}")
            raise e
        
    def getData(self, pid: str, startTimestamp: str, stopTimestamp: str):
        try:
            if(startTimestamp == ""):
                startTimestamp = "-7d"
            if(stopTimestamp == ""):
                stopTimestamp = "now()"
            query = f'from(bucket: "{influx_bucket}") |> range(start: {startTimestamp}, stop: {stopTimestamp}) \
            |> filter(fn: (r) => r["_field"] == "{pid}") \
            |> sort(columns: ["_time"], desc: true)'
            # |> window(every: 1h)'
            result = self.client.query_api().query(query=query)
            # convert result to format {timestamp: value}
            processed = []
            for table in result:
                for record in table.records:
                    processed.append({
                        "timestamp": record.get_time(),
                        "value": record.get_value(),
                        "id": record.get_field()
                    })
            return processed
        except Exception as e:
            logger.error(f"Failed to get data from InfluxDB: {e}")
            raise e

   
