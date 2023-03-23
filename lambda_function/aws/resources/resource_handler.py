import copy
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

import consts
from port.entities import create_entities_json

logger = logging.getLogger(__name__)


class ResourceHandler:
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
        for region in list(self.regions):
            aws_cloudcontrol_client = boto3.client('cloudcontrol', region_name=region)
            resources_models = self.regions_config.get(region, {}).get('resources_models', ["{}"])
            for resource_model in list(resources_models):
                logger.info(f"List kind: {self.kind}, region: {region}, resource_model: {resource_model}")
                self.next_token = '' if self.next_token is None else self.next_token
                while self.next_token is not None:
                    list_resources_params = {'TypeName': self.kind, 'ResourceModel': resource_model}
                    if self.next_token:
                        list_resources_params['NextToken'] = self.next_token

                    try:
                        response = aws_cloudcontrol_client.list_resources(**list_resources_params)
                    except Exception as e:
                        logger.error(
                            f"Failed list kind: {self.kind}, region: {region}, resource_model: {resource_model}; {e}")
                        self.skip_delete = True
                        self.next_token = None
                        break

                    self._handle_list_response(aws_cloudcontrol_client, response)

                    self.next_token = response.get('NextToken')
                    if self.lambda_context.get_remaining_time_in_millis() < consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
                        # Lambda timeout is too close, should return checkpoint for next run
                        return self._handle_close_to_timeout(resources_models, resource_model, region)

                self._cleanup_resources_models(resources_models, resource_model, region)

            self._cleanup_regions(region)

        return {'aws_entities': self.aws_entities, 'next_resource_config': None, 'skip_delete': self.skip_delete}

    def _handle_list_response(self, aws_cloudcontrol_client, list_response):
        resource_descriptions = list_response.get('ResourceDescriptions', [])
        with ThreadPoolExecutor(max_workers=consts.MAX_UPSERT_WORKERS) as executor:
            futures = [executor.submit(self.handle_single_resource_item, aws_cloudcontrol_client,
                                       resource_desc.get('Identifier', '')) for resource_desc in resource_descriptions]
            for completed_future in as_completed(futures):
                result = completed_future.result()
                self.aws_entities.update(result.get('aws_entities', set()))
                self.skip_delete = result.get('skip_delete', False) if not self.skip_delete else self.skip_delete

    def handle_single_resource_item(self, aws_cloudcontrol_client, resource_id, action_type='upsert'):
        entities = []
        skip_delete = False
        try:
            resource_obj = {}
            if action_type == 'upsert':
                logger.info(f"Get resource for kind: {self.kind}, resource id: {resource_id}")
                resource_obj = json.loads(aws_cloudcontrol_client.get_resource(TypeName=self.kind,
                                                                               Identifier=resource_id).get(
                    'ResourceDescription').get('Properties'))
            elif action_type == 'delete':
                resource_obj = {"identifier": resource_id}  # Entity identifier to delete

            entities = create_entities_json(resource_obj, self.selector_query, self.mappings, action_type)
        except Exception as e:
            logger.error(f"Failed to extract or transform resource id: {resource_id}, kind: {self.kind}, error: {e}")
            skip_delete = True

        aws_entities = self._handle_entities(entities, action_type)

        return {'aws_entities': aws_entities, 'skip_delete': skip_delete}

    def _handle_entities(self, entities, action_type='upsert'):
        aws_entities = set()
        for entity in entities:
            blueprint_id = entity.get('blueprint')
            entity_id = entity.get('identifier')

            aws_entities.add(f"{blueprint_id};{entity_id}")

            try:
                if action_type == 'upsert':
                    self.port_client.upsert_entity(entity)
                elif action_type == 'delete':
                    self.port_client.delete_entity(entity)
            except Exception as e:
                logger.error(
                    f"Failed to handle entity: {entity_id} of blueprint: {blueprint_id}, action: {action_type}; {e}")

        return aws_entities

    def _handle_close_to_timeout(self, resources_models, current_resource_model, region):
        if self.next_token:
            self.selector_aws['next_token'] = self.next_token
        else:
            self.selector_aws.pop('next_token', None)
            resources_models = self._cleanup_resources_models(resources_models, current_resource_model, region)
            if not resources_models:
                self._cleanup_regions(region)
                if not self.regions:  # Nothing left to sync
                    return {'aws_entities': self.aws_entities, 'next_resource_config': None,
                            'skip_delete': self.skip_delete}

        if 'selector' not in self.resource_config:
            self.resource_config['selector'] = {}
        self.resource_config['selector']['aws'] = self.selector_aws

        return {'aws_entities': self.aws_entities, 'next_resource_config': self.resource_config,
                'skip_delete': self.skip_delete}

    def _cleanup_resources_models(self, resources_models, current_resource_model, region):
        resources_models.remove(current_resource_model)
        if region not in self.regions_config:
            self.regions_config[region] = {}
        self.regions_config[region]['resources_models'] = resources_models
        self.selector_aws['regions_config'] = self.regions_config

        return resources_models

    def _cleanup_regions(self, region):
        self.regions.remove(region)
        self.regions_config.pop(region, None)
        self.selector_aws['regions'] = self.regions
        self.selector_aws['regions_config'] = self.regions_config
