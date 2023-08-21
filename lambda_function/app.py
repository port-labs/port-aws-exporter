import logging

from aws.resources.handler import ResourcesHandler
from config import get_config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Load config")
    config = get_config(event, context)
    logger.info("Handling resources")
    resources_handler = ResourcesHandler(config, context)
    resources_handler.handle()
    logger.info("Exiting...")
