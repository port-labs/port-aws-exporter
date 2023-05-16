import json
import logging
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import consts
import yaml
from aws.resources.base_handler import BaseHandler
from port.entities import create_entities_json, handle_entities

logger = logging.getLogger(__name__)


class CloudFormationHandler(BaseHandler):
    def handle(self):
        for region in list(self.regions):
            aws_cloudformation_client = boto3.client('cloudformation', region_name=region)
            logger.info(f"List CloudFormation Stack, region: {region}")
            self.next_token = '' if self.next_token is None else self.next_token
            while self.next_token is not None:
                list_stacks_params = self.selector_aws.get('list_parameters', {})
                if self.next_token:
                    list_stacks_params['NextToken'] = self.next_token
                try:
                    response = aws_cloudformation_client.list_stacks(**list_stacks_params)
                except Exception as e:
                    logger.error(
                        f"Failed list CloudFormation Stack, region: {region},"
                        f" Parameters: {list_stacks_params}; {e}")
                    self.skip_delete = True
                    self.next_token = None
                    break

                self._handle_list_response(response, region)

                self.next_token = response.get('NextToken')
                if self.lambda_context.get_remaining_time_in_millis() < consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
                    # Lambda timeout is too close, should return checkpoint for next run
                    return self._handle_close_to_timeout(region)

            self._cleanup_regions(region)

        return {'aws_entities': self.aws_entities, 'next_resource_config': None, 'skip_delete': self.skip_delete}

    def _handle_list_response(self, list_response, region):
        stacks = list_response.get('StackSummaries', [])
        with ThreadPoolExecutor(max_workers=consts.MAX_UPSERT_WORKERS) as executor:
            futures = [executor.submit(self.handle_single_resource_item, region, stack.get("StackId")) for stack in
                       stacks if stack['StackStatus'] != 'DELETE_COMPLETE']
            for completed_future in as_completed(futures):
                result = completed_future.result()
                self.aws_entities.update(result.get('aws_entities', set()))
                self.skip_delete = result.get('skip_delete', False) if not self.skip_delete else self.skip_delete

    def handle_single_resource_item(self, region, stack_id, action_type='upsert'):
        entities = []
        skip_delete = False
        try:
            stack_obj = {}
            if action_type == 'upsert':
                logger.info(f"Get CloudFormation Stack, id: {stack_id}")

                aws_cloudformation_client = boto3.client("cloudformation", region_name=region)
                stack_obj = aws_cloudformation_client.describe_stacks(StackName=stack_id).get("Stacks")[0]
                stack_obj['StackResources'] = aws_cloudformation_client.describe_stack_resources(
                    StackName=stack_id).get('StackResources')
                template = aws_cloudformation_client.get_template(StackName=stack_id).get(
                    'TemplateBody')

                # Some templates return as nested OrderedDict, so we need to convert them
                # to regular dicts using the json library and then to yaml strings for a clear yaml
                if isinstance(template, OrderedDict):
                    template = yaml.dump(json.loads(json.dumps(template)))

                stack_obj['TemplateBody'] = template

                # Handles unserializable date properties in the JSON by turning them into a string
                stack_obj = json.loads(json.dumps(stack_obj, default=str))

            elif action_type == 'delete':
                stack_obj = {"identifier": stack_id}  # Entity identifier to delete

            entities = create_entities_json(stack_obj, self.selector_query, self.mappings, action_type)

        except Exception as e:
            logger.error(f"Failed to extract or transform CloudFormation Stack with id: {stack_id}, error: {e}")
            skip_delete = True

        aws_entities = handle_entities(entities, self.port_client, action_type)

        return {'aws_entities': aws_entities, 'skip_delete': skip_delete}

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
