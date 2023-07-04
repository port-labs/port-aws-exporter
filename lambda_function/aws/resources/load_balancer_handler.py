import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import consts
from aws.resources.base_handler import BaseHandler
from port.entities import create_entities_json, handle_entities

logger = logging.getLogger(__name__)


class LoadBalancerHandler(BaseHandler):
    def handle(self):
        for region in list(self.regions):
            aws_elbv2_client = boto3.client("elbv2", region_name=region)
            logger.info(f"List Load Balancers, region: {region}")
            self.next_token = "" if self.next_token is None else self.next_token
            while self.next_token is not None:
                filter_parameters = self.selector_aws.get("list_parameters", {})
                if self.next_token:
                    filter_parameters["Marker"] = self.next_token
                try:
                    response = aws_elbv2_client.describe_load_balancers(**filter_parameters)
                except Exception as e:
                    logger.error(
                        f"Failed list Load Balancers, region: {region},"
                        f" Parameters: {filter_parameters}; {e}")
                    self.skip_delete = True
                    self.next_token = None
                    break

                self._handle_list_response(response, region)

                self.next_token = response.get("NextMarker")
                if self.lambda_context.get_remaining_time_in_millis()< consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
                    # Lambda timeout is too close, should return checkpoint for next run
                    return self._handle_close_to_timeout(region)

            self._cleanup_regions(region)

        return {"aws_entities": self.aws_entities, "next_resource_config": None, "skip_delete": self.skip_delete}

    def _handle_list_response(self, list_response, region):
        load_balancers = list_response.get("LoadBalancers", [])
        with ThreadPoolExecutor(max_workers=consts.MAX_UPSERT_WORKERS) as executor:
            futures = [executor.submit(self.handle_single_resource_item, region, load_balancer.get("LoadBalancerName")) for load_balancer in load_balancers]
            for completed_future in as_completed(futures):
                result = completed_future.result()
                self.aws_entities.update(result.get("aws_entities", set()))
                self.skip_delete = result.get("skip_delete", False) if not self.skip_delete else self.skip_delete

    def handle_single_resource_item(self, region, elb_name, action_type="upsert"):
        entities = []
        skip_delete = False
        try:
            if action_type == "upsert":
                logger.info(f"Get Load Balancer, Name: {elb_name} in {region}")

                # Create a Boto3 client for the Elastic Load Balancing service
                aws_elbv2_client = boto3.client('elbv2')
                response = aws_elbv2_client.describe_load_balancers(Names=[elb_name])

                # Extract load balancer details from the response
                load_balancer_obj = response['LoadBalancers'][0]

                # Handles unserializable date properties in the JSON by turning them into a string
                load_balancer_obj = json.loads(json.dumps(load_balancer_obj, default=str))

            elif action_type == "delete":
                load_balancer_obj = {"identifier": elb_name}  # Entity identifier to delete

            entities = create_entities_json(load_balancer_obj, self.selector_query, self.mappings, action_type)

        except Exception as e:
            logger.error(f"Failed to extract or transform Load Balancer with name: {elb_name}, error: {e}")
            skip_delete = True

        aws_entities = handle_entities(entities, self.port_client, action_type)

        return {"aws_entities": aws_entities, "skip_delete": skip_delete}

    def _handle_close_to_timeout(self, region):
        if self.next_token:
            self.selector_aws["next_token"] = self.next_token
        else:
            self.selector_aws.pop("next_token", None)
            self._cleanup_regions(region)
            if not self.regions:  # Nothing left to sync
                return {"aws_entities": self.aws_entities, "next_resource_config": None, "skip_delete": self.skip_delete}

        if "selector" not in self.resource_config:
            self.resource_config["selector"] = {}
        self.resource_config["selector"]["aws"] = self.selector_aws

        return {"aws_entities": self.aws_entities, "next_resource_config": self.resource_config, "skip_delete": self.skip_delete}