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


class EC2Handler(BaseHandler):
    def handle(self):
        for region in list(self.regions):
            aws_ec2_client = boto3.resource('ec2', region_name=region)
            logger.info(f"List EC2 Instance, region: {region}")
            self.next_token = '' if self.next_token is None else self.next_token
            while self.next_token is not None:
                list_instances_params = self.selector_aws.get('list_parameters', {})
                if self.next_token:
                    list_instances_params['NextToken'] = self.next_token
                try:
                    response = aws_ec2_client.instances.all()
                    instance_list = list()
                    for instance in response:
                        instance_list.append({"id": instance.id})

                except Exception as e:
                    logger.error(
                        f"Failed list EC2 Instance, region: {region},"
                        f" Parameters: {list_instances_params}; {e}")
                    self.skip_delete = True
                    self.next_token = None
                    break

                self._handle_list_response(instance_list, region)

                #self.next_token = instance_list.get('NextToken')
                if self.lambda_context.get_remaining_time_in_millis() < consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
                    # Lambda timeout is too close, should return checkpoint for next run
                    return self._handle_close_to_timeout(region)

            self._cleanup_regions(region)

        return {'aws_entities': self.aws_entities, 'next_resource_config': None, 'skip_delete': self.skip_delete}

    def _handle_list_response(self, list_response, region):
        with ThreadPoolExecutor(max_workers=consts.MAX_UPSERT_WORKERS) as executor:
            futures = [executor.submit(self.handle_single_resource_item, region, instance.get("id")) for instance in list_response]
            for completed_future in as_completed(futures):
                result = completed_future.result()
                self.aws_entities.update(result.get('aws_entities', set()))
                self.skip_delete = result.get('skip_delete', False) if not self.skip_delete else self.skip_delete

    def handle_single_resource_item(self, region, instance_id, action_type='upsert'):
        entities = []
        skip_delete = False
        try:
            instance_obj = {}
            if action_type == 'upsert':
                logger.info(f"Describe EC2 Instance with ID: {instance_id}")

                aws_ec2_client = boto3.client("ec2", region_name=region)
                instance_response = aws_ec2_client.describe_instances(InstanceIds=[instance_id])
                instance_obj = instance_response["Reservations"][0]["Instances"][0]

                # Handles unserializable date properties in the JSON by turning them into a string
                instance_obj = json.loads(json.dumps(instance_obj, default=str))

            elif action_type == 'delete':
                instance_obj = {"identifier": instance_id}  # Entity identifier to delete

            entities = create_entities_json(instance_obj, self.selector_query, self.mappings, action_type)

        except Exception as e:
            logger.error(f"Failed to extract or transform EC2 Instance with id: {instance_id}, error: {e}")
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
