
import json
import logging
import os
from app.microservicies.AuroralNode import AuroralNode
from app.tools.Settings import Settings

logger = logging.getLogger(__name__)

settings = Settings()
itemsSettings = settings.get_items()
enabled_data_points = settings.get_enabled_data_points()
auroralNode = AuroralNode()

adapter_host: str = os.getenv('ADAPTER_HOST')

def initRegistrationCheck():
    for i in itemsSettings:
        adapterid = i.get('adapterid')
        # check if item is already registered
        # logger.info(f"Checking registration for item {adapterid}")
        oid = auroralNode.getRegistartionOidByAdapterid(adapterid)
        if(oid):
            logger.info(f"Item {adapterid} is already registered with OID {oid}")
            updateItem(i, oid)
            continue
        else:
            logger.info(f"Item {adapterid} is not registered")
            # register item
            registerItem(i)
            # logger.info(f"Item {adapterid} registered")

def buildTd(itemSettings: dict)-> json:
    adapterid = itemSettings.get('adapterid')
    # load td from template file
    tdJson = {}
    try:
        tdString = ""
        with open('app/templates/td.json') as f:
            tdString = f.read()
            tdJson = json.loads(tdString)
    except Exception as e:
        logger.error(f"Error loading td file: {e}")
        return
    # fill in the template
    tdJson['adapterId'] = adapterid
    tdJson['title'] = itemSettings.get('title')
    tdJson['description'] = itemSettings.get('description')
    tdJson['location'] = itemSettings.get('location')
    # prepare properties
    properties = {}
    for p in itemSettings.get('properties'):
        details = enabled_data_points.get(p)
        if not details:
            logger.error("Inconsistency in settings file: property " + p + " not found in enabled_data_points")
            return
        properties[details.get("title")] = {
            "@type": details.get('@type'),
            "title": details.get('title'),
            "description": details.get('NAZOV_MERACA'),
            "unit": details.get('unit'),
            "type": "number",
            "forms": [
                {
                    "op": ["readproperty"],
                    "href": adapter_host + "consumption/" + adapterid + "/property/" + p
                }
            ], 
            "readOnly": True
        }
    properties["getAll"] = {
        "@type": "",
        "title": "getAll",
        "description": "Get all properties",
        "type": "object",
        "forms": [
            {
                "op": ["readproperty"],
                "href": adapter_host + "consumption/" + adapterid + "/property/getAll"
            }
        ],
        "readOnly": True
    }
    tdJson['properties'] = properties
    return tdJson

def registerItem(itemSettings: dict):
    adapterid = itemSettings.get('adapterid')
    # build td
    tdJson = buildTd(itemSettings)
    # register item
    auroralNode.registerItem({'td': tdJson})
    logger.info(f"Item {adapterid} registered")
    
def updateItem(itemSettings: dict, oid: str = None):
    # build td
    tdJson = buildTd(itemSettings)
    tdJson['oid'] = oid
    tdJson['id'] = oid
    # update item
    auroralNode.updateItem({'td': tdJson})
    # logger.info(f"Item {itemSettings.get('adapterid')} updated")