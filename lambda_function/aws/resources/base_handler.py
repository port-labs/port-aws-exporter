import copy
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

import consts
from port.entities import create_entities_json

logger = logging.getLogger(__name__)


class BaseHandler:
    def __init__(self, resource_config, port_client, lambda_context, default_region):
        self.resource_config = copy.deepcopy(resource_config)
        self.port_client = port_client
        self.lambda_context = lambda_context
        self.kind = self.resource_config.get('kind', '')
        selector = self.resource_config.get('selector', {})
        self.selector_query = selector.get('query')
        self.selector_aws = selector.get('aws', {})
        self.regions = self.selector_aws.get('regions', [default_region])
        self.regions_config = self.selector_aws.get('regions_config', {})
        self.next_token = self.selector_aws.get('next_token', '')
        self.mappings = self.resource_config.get('port', {}).get('entity', {}).get('mappings', [])
        self.aws_entities = set()
        self.skip_delete = False

    def handle(self):
        raise NotImplementedError("Subclasses should implement 'handle' function")

    def handle_single_resource_item(self, region, resource_id, action_type='upsert'):
        raise NotImplementedError("Subclasses should implement 'handle_single_resource_item' function")

    def _cleanup_regions(self, region):
        self.regions.remove(region)
        self.regions_config.pop(region, None)
        self.selector_aws['regions'] = self.regions
        self.selector_aws['regions_config'] = self.regions_config
