import json
import logging
from concurrent.futures import ThreadPoolExecutor

import boto3

import consts
import jq
from aws.resources.handler_creator import create_resource_handler
from port.client import PortClient

logger = logging.getLogger(__name__)


class ResourcesHandler:
    def __init__(self, config, lambda_context):
        self.config = config
        self.lambda_context = lambda_context
        split_arn = lambda_context.invoked_function_arn.split(":")
        self.region = split_arn[3]
        account_id = split_arn[4]
        self.user_id = f"accountid/{account_id} region/{self.region}"
        port_client_id = (
            self.config.get("port_client_id")
            if self.config.get("keep_cred")
            else self.config.pop("port_client_id")
        )
        port_client_secret = (
            self.config.get("port_client_secret")
            if self.config.get("keep_cred")
            else self.config.pop("port_client_secret")
        )
        self.port_client = PortClient(
            port_client_id,
            port_client_secret,
            user_agent=f"{consts.PORT_AWS_EXPORTER_NAME}/0.1 ({self.user_id})",
            api_url=self.config.get("port_api_url", consts.PORT_API_URL),
        )
        self.event = self.config.get("event")
        self.bucket_name = self.config["bucket_name"]
        self.next_config_file_key = self.config.get("next_config_file_key")
        self.aws_entities = set(self.config.get("aws_entities", []))
        self.resources_config = self.config["resources"]
        self.skip_delete = self.config.get("skip_delete", False)
        self.require_reinvoke = False

    def handle(self):
        if self.event and self.event.get("Records"):  # Single events from SQS
            logger.info("Handle events from sqs")
            for record in self.event.get("Records"):
                try:
                    resource = json.loads(record["body"])
                    self._handle_event_resource(resource)
                except Exception as e:
                    logger.error(f"Failed to handle event: {self.event}, error: {e}")
            return

        logger.info("Starting upsert of AWS resources to Port")

        self._upsert_resources()

        if self.require_reinvoke:
            return self._reinvoke_lambda()

        logger.info("Done upsert of AWS resources to Port")

        if not self.skip_delete:
            logger.info("Starting delete process of stale resources from Port")
            self._delete_stale_resources()
            logger.info("Done deleting stale resources from Port")

        logger.info("Done handling your resources")

    def _handle_event_resource(self, resource):
        assert "identifier" in resource, "Event must include 'identifier'"
        assert "region" in resource, "Event must include 'region'"
        region = jq.first(resource["region"], resource)
        identifier = jq.first(resource["identifier"], resource)

        action_type = str(
            jq.first(resource.get("action", '"upsert"'), resource)
        ).lower()
        assert action_type in [
            "upsert",
            "delete",
        ], f"Action should be one of 'upsert', 'delete'"

        resource_configs = [
            resource_config
            for resource_config in self.resources_config
            if resource_config["kind"] == resource["resource_type"]
        ]
        assert (
            resource_configs
        ), f"Resource config not found for kind: {resource['resource_type']}"

        for resource_config in resource_configs:
            resource_handler = create_resource_handler(
                resource_config, self.port_client, self.lambda_context, region
            )
            resource_handler.handle_single_resource_item(
                region, identifier, action_type
            )

    def _upsert_resources(self):
        for resource_index, resource in enumerate(list(self.resources_config)):
            resource_handler = create_resource_handler(
                resource, self.port_client, self.lambda_context, self.region
            )
            result = resource_handler.handle()
            self.aws_entities.update(result.get("aws_entities", set()))
            next_resource_config = result.get("next_resource_config")
            self.skip_delete = (
                result.get("skip_delete", False)
                if not self.skip_delete
                else self.skip_delete
            )
            self.resources_config[resource_index] = next_resource_config
            if (
                self.lambda_context.get_remaining_time_in_millis()
                < consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD
            ):
                self._handle_close_to_timeout()
                break

    def _handle_close_to_timeout(self):
        self.config["resources"] = [
            res_config for res_config in self.resources_config if res_config
        ]
        if self.config["resources"]:
            logger.info(
                "Lambda will be timed out soon, a new Lambda will be invoked to continue the sync process."
            )
            self.config["aws_entities"] = list(self.aws_entities)
            self.config["skip_delete"] = self.skip_delete
            self.require_reinvoke = True

    def _delete_stale_resources(self):
        query = {
            "combinator": "and",
            "rules": [
                {
                    "property": "$datasource",
                    "operator": "contains",
                    "value": consts.PORT_AWS_EXPORTER_NAME,
                },
                {
                    "property": "$datasource",
                    "operator": "contains",
                    "value": self.user_id,
                },
            ],
        }
        port_entities = self.port_client.search_entities(query)

        with ThreadPoolExecutor(max_workers=consts.MAX_DELETE_WORKERS) as executor:
            executor.map(
                self.port_client.delete_entity,
                [
                    entity
                    for entity in port_entities
                    if f"{entity.get('blueprint')};{entity.get('identifier')}"
                    not in self.aws_entities
                ],
            )

    def _reinvoke_lambda(self):
        self._save_config_state()
        payload = {"next_config_file_key": self.next_config_file_key}

        aws_lambda_client = boto3.client("lambda")
        return aws_lambda_client.invoke(
            FunctionName=self.lambda_context.function_name,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )

        # self.__init__(self.config, self.lambda_context)  # return self.handle()

    def _save_config_state(self):
        aws_s3_client = boto3.client("s3")
        try:
            aws_s3_client.put_object(
                Body=json.dumps(self.config),
                Bucket=self.bucket_name,
                Key=self.next_config_file_key,
            )
        except Exception as e:
            logger.warning(
                f"Failed to save lambda state, bucket: {self.bucket_name}, key: {self.next_config_file_key}; {e}"
            )
            self.skip_delete = True
