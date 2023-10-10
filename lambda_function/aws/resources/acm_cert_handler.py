import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import consts
from aws.resources.base_handler import BaseHandler
from port.entities import create_entities_json, handle_entities

logger = logging.getLogger(__name__)


class ACMHandler(BaseHandler):
    def handle(self):
        for region in list(self.regions):
            aws_acm_client = boto3.client("acm", region_name=region)
            try:
                paginator = aws_acm_client.get_paginator('list_certificates')
                for page in paginator.paginate():
                    certificate_summary_list = page.get('CertificateSummaryList', [])
                    self._handle_list_response(certificate_summary_list, region)

                    if self.lambda_context.get_remaining_time_in_millis() < consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
                        # Lambda timeout is too close, should return checkpoint for next run
                        return self._handle_close_to_timeout(region)
            except Exception as e:
                logger.error(f"Failed to list ACM certificates in region: {region}; {e}")
                self.skip_delete = True

        return {
            "aws_entities": self.aws_entities,
            "next_resource_config": None,
            "skip_delete": self.skip_delete,
        }

    def _handle_list_response(self, certificate_summary_list, region):
        with ThreadPoolExecutor(max_workers=consts.MAX_CC_WORKERS) as executor:
            futures = [
                executor.submit(self.handle_single_resource_item, region, cert.get("CertificateArn", ""))
                for cert in certificate_summary_list
            ]
            for completed_future in as_completed(futures):
                result = completed_future.result()
                self.aws_entities.update(result.get("aws_entities", set()))
                self.skip_delete = result.get("skip_delete", False) if not self.skip_delete else self.skip_delete

    def _handle_close_to_timeout(self, region):
        if "selector" not in self.resource_config:
            self.resource_config["selector"] = {}
        self.resource_config["selector"]["aws"] = self.selector_aws

        return {
            "aws_entities": self.aws_entities,
            "next_resource_config": self.resource_config,
            "skip_delete": self.skip_delete,
        }

    def handle_single_resource_item(self, region, certificate_arn, action_type="upsert"):
        entities = []
        skip_delete = False
        try:
            resource_obj = {}
            if action_type == "upsert":
                logger.info(f"Get ACM certificate details for ARN: {certificate_arn}")
                aws_acm_client = boto3.client("acm", region_name=region)
                response = aws_acm_client.describe_certificate(CertificateArn=certificate_arn)
                resource_obj = response.get("Certificate", {})

                # Handles unserializable date properties in the JSON by turning them into a string
                resource_obj = json.loads(json.dumps(resource_obj, default=str))
            elif action_type == "delete":
                resource_obj = {"identifier": certificate_arn}  # Entity identifier to delete
            entities = create_entities_json(resource_obj, self.selector_query, self.mappings, action_type)
        except Exception as e:
            logger.error(f"Failed to extract or transform certificate ARN: {certificate_arn}, error: {e}")
            skip_delete = True

        aws_entities = handle_entities(entities, self.port_client, action_type)

        return {"aws_entities": aws_entities, "skip_delete": skip_delete}
