import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import consts
from aws.resources.base_handler import BaseHandler
from port.entities import create_entities_json, handle_entities

logger = logging.getLogger(__name__)


class ElasticacheClusterHandler(BaseHandler):
    def handle(self):
        for region in list(self.regions):
            aws_elasticache_client = boto3.client("elasticache", region_name=region)
            logger.info(f"Listing Elasticache Clusters in region: {region}")
            self.next_token = "" if self.next_token is None else self.next_token
            while self.next_token is not None:
                filter_parameters = self.selector_aws.get("list_parameters", {})
                if self.next_token:
                    filter_parameters["Marker"] = self.next_token
                try:
                    response = aws_elasticache_client.describe_cache_clusters(**filter_parameters)
                except Exception as e:
                    logger.error(
                        f"Failed to list Elasticache Cluster in region: {region},"
                        f" Parameters: {filter_parameters}; {e}")
                    self.skip_delete = True
                    self.next_token = None
                    break

                self._handle_list_response(response, region)

                self.next_token = response.get("Marker")
                if self.lambda_context.get_remaining_time_in_millis()< consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
                    # Lambda timeout is too close, should return checkpoint for next run
                    return self._handle_close_to_timeout(region)

            self._cleanup_regions(region)

        return {"aws_entities": self.aws_entities, "next_resource_config": None, "skip_delete": self.skip_delete}

    def _handle_list_response(self, list_response, region):
        cache_clusters = list_response.get("CacheClusters", [])
        with ThreadPoolExecutor(max_workers=consts.MAX_DEFAULT_AWS_WORKERS) as executor:
            futures = [executor.submit(self.handle_single_resource_item, region, cache_cluster.get("CacheClusterId")) for cache_cluster in cache_clusters]
            for completed_future in as_completed(futures):
                result = completed_future.result()
                self.aws_entities.update(result.get("aws_entities", set()))
                self.skip_delete = result.get("skip_delete", False) if not self.skip_delete else self.skip_delete

    def handle_single_resource_item(self, region, cache_cluster_id, action_type="upsert"):
        entities = []
        skip_delete = False
        try:
            if action_type == "upsert":
                logger.info(f"Get Cache Cluster, ID: {cache_cluster_id} in {region}")

                # Create a Boto3 client for the Elasticache cluster service
                aws_elasticache_client = boto3.client('elasticache')
                response = aws_elasticache_client.describe_cache_clusters(CacheClusterId=cache_cluster_id)
                # Extract cache cluster details from the response
                cache_cluster_obj = response['CacheClusters'][0]
                cache_cluster_arn = cache_cluster_obj["ARN"]

                ## Fetch the tags attached to this cache
                cache_tags_response = aws_elasticache_client.list_tags_for_resource(ResourceName=cache_cluster_arn)
                cache_cluster_obj["Tags"] = cache_tags_response["TagList"]

                # Handles unserializable date properties in the JSON by turning them into a string
                cache_cluster_obj = json.loads(json.dumps(cache_cluster_obj, default=str))

            elif action_type == "delete":
                cache_cluster_obj = {"identifier": cache_cluster_id}  # Entity identifier to delete

            entities = create_entities_json(cache_cluster_obj, self.selector_query, self.mappings, action_type)

        except Exception as e:
            logger.error(f"Failed to extract or transform Cache Cluster with name: {cache_cluster_id}, error: {e}")
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