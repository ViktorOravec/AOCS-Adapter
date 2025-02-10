import json
import os
from dotenv import load_dotenv
from typing import Optional
import requests
import logging


logger = logging.getLogger(__name__)

load_dotenv()

auroral_node_sb: Optional[str] = os.getenv('AURORAL_NODE_SB')
auroral_node_username: Optional[int] = os.getenv('AURORAL_NODE_USERNAME')
auroral_node_password: Optional[str] = os.getenv('AURORAL_NODE_PASSWORD')

# Singleton class to connect to an Auroral service
class AuroralNode:
    _instance: Optional['AuroralNode'] = None

    def __new__(cls, *args, **kwargs) -> 'AuroralNode':
        if not cls._instance:
            cls._instance = super(AuroralNode, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, 'initialized'):
            self.host = auroral_node_sb
            self.initialized = True

    def is_healthy(self) -> bool:
        try:
            path = "api/agent/healthcheck"
            # call healthcheck endpoint - if it returns 200, the service is healthy
            response = requests.get(f"{self.host}/{path}", auth=(auroral_node_username, auroral_node_password))
            logger.debug(f"Healthcheck response: {response.text}")
            if response.status_code != 200:
                logger.error(f"Failed to connect to Auroral: {response.text}")
                return False
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Auroral: {e}")
            return False
    
    def getRegistartionOidByAdapterid(self, adapterId: str)-> str :
        try:
            path = "api/registration/oid/" + adapterId
            response = requests.get(f"{self.host}/{path}", auth=(auroral_node_username, auroral_node_password))
            if response.status_code != 200:
                logger.error(f"Failed to connect to Auroral Node: {response.text}")
                return None
            # extract body
            body = response.json()
            return body['message']
        except Exception as e:
            logger.error(f"Failed to connect to Auroral Node: {e}")
            return None
    # td is in json format
    def registerItem(self, td: json) -> None:
        try:
            logger.info(f"Registering item")
            path = "api/registration"
            response = requests.post(f"{self.host}/{path}", auth=(auroral_node_username, auroral_node_password), json=td)
            if response.status_code != 201:
                logger.error(f"Registration response: {response.text}")
                raise Exception(f"Failed to register in node: {response.text}")                
        except Exception as e:
            logger.error(f"Failed to register in node: {e}")
    def updateItem(self, td: json) -> None:
        try:
            logger.info(f"Updating item")
            path = "api/registration"
            response = requests.put(f"{self.host}/{path}", auth=(auroral_node_username, auroral_node_password), json=td)
            if response.status_code != 200:
                # logger.error(f"TD: {td}")
                logger.error(f"Update response: {response.text}")
                raise Exception(f"Failed to update in node: {response.text}")                
            else:
                logger.info(f"Item updated")
        except Exception as e:
            logger.error(f"Failed to update in node: {e}")
        

