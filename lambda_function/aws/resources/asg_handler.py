import logging
import boto3
from aws.resources.extended_resource_handler import ExtendedResourceHandler

logger = logging.getLogger(__name__)

class ASGHandler(ExtendedResourceHandler):
    def __init__(self, resource_config, port_client, lambda_context, default_region):
        super().__init__(resource_config, port_client, lambda_context, default_region)
        self.asg_client = boto3.client('autoscaling', region_name=self.region)

    def fetch_resources(self):
        response = self.asg_client.describe_auto_scaling_groups()
        logger.debug("Autoscaling group response: %s" % response)
        return response.get('AutoScalingGroups', [])
