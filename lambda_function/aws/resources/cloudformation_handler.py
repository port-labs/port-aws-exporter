import copy
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode
import boto3
import yaml
import ruamel.yaml

import consts
from port.entities import create_entities_json

logger = logging.getLogger(__name__)


class CloudFormationHandler:
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
            aws_cloudformation_client = boto3.client('cloudformation', region_name=region)
            stack_status_filter = self.regions_config.get(region, {}).get('stack_status_filter', [])
            logger.info(f"List CloudFormation Stack, region: {region}, Stack statuses filter: {stack_status_filter}")
            self.next_token = '' if self.next_token is None else self.next_token
            while self.next_token is not None:
                list_stacks_params = {'StackStatusFilter': stack_status_filter}
                if self.next_token:
                    list_stacks_params['NextToken'] = self.next_token
                try:
                    response = aws_cloudformation_client.list_stacks(**list_stacks_params)
                except Exception as e:
                    logger.error(
                        f"Failed list CloudFormation Stack, region: {region}, Stack statuses filter: {stack_status_filter}; {e}")
                    self.skip_delete = True
                    self.next_token = None
                    break

                self._handle_list_response(aws_cloudformation_client, response, region)

                self.next_token = response.get('NextToken')
                if self.lambda_context.get_remaining_time_in_millis() < consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
                    # Lambda timeout is too close, should return checkpoint for next run
                    return self._handle_close_to_timeout(region)

            self._cleanup_regions(region)

        return {'aws_entities': self.aws_entities, 'next_resource_config': None, 'skip_delete': self.skip_delete}

    def _handle_list_response(self, aws_cloudformation_client, list_response, region):
        stacks = list_response.get('StackSummaries', [])
        with ThreadPoolExecutor(max_workers=consts.MAX_UPSERT_WORKERS) as executor:
            futures = [executor.submit(self.handle_single_resource_item, aws_cloudformation_client,
                                       stack.get("StackName"), region) for stack in stacks]
            for completed_future in as_completed(futures):
                result = completed_future.result()
                self.aws_entities.update(result.get('aws_entities', set()))
                self.skip_delete = result.get('skip_delete', False) if not self.skip_delete else self.skip_delete

    def handle_single_resource_item(self, aws_cloudformation_client, stack_name, region, action_type='upsert'):
        entities = []
        skip_delete = False
        try:
            stack_obj = {}
            if action_type == 'upsert':
                logger.info(f"Get CloudFormation Stack, id: {stack_name}")
                stack_obj = aws_cloudformation_client.describe_stacks(StackName=stack_name).get("Stacks")[0]
                stack_obj['StackResources'] = aws_cloudformation_client.describe_stack_resources(StackName=stack_name) \
                    .get('StackResources')
                stack_obj['Url'] = self._get_stack_console_url(stack_obj['StackId'])

                stack_obj['TemplateBody'] = aws_cloudformation_client.get_template(StackName=stack_name,
                                                                                   TemplateStage='Original').get(
                    'TemplateBody')

                stack_obj = json.loads(json.dumps(stack_obj, default=str))
            elif action_type == 'delete':
                stack_obj = {"identifier": stack_name}  # Entity identifier to delete

            entities = create_entities_json(stack_obj, self.selector_query, self.mappings, action_type)
        except Exception as e:
            logger.error(f"Failed to extract or transform CloudFormation Stack with id: {stack_name}, error: {e}")
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

    def _handle_close_to_timeout(self, region):
        if self.next_token:
            self.selector_aws['next_token'] = self.next_token
        else:
            self.selector_aws.pop('next_token', None)

            self._cleanup_regions(region)
            if not self.regions:  # Nothing left to sync
                return {'aws_entities': self.aws_entities, 'next_resource_config': None,
                        'skip_delete': self.skip_delete}

        if 'selector' not in self.resource_config:
            self.resource_config['selector'] = {}
        self.resource_config['selector']['aws'] = self.selector_aws

        return {'aws_entities': self.aws_entities, 'next_resource_config': self.resource_config,
                'skip_delete': self.skip_delete}

    def _cleanup_regions(self, region):
        self.regions.remove(region)
        self.regions_config.pop(region, None)
        self.selector_aws['regions'] = self.regions
        self.selector_aws['regions_config'] = self.regions_config

    def _get_stack_console_url(self, stack_id):
        base_url = "https://console.aws.amazon.com/go/view?arn="

        url = f"{base_url}{stack_id}"
        return url
