import logging
import boto3
from aws.resources.extended_resource_handler import ExtendedResourceHandler

logger = logging.getLogger(__name__)

class SecurityGroupHandler(ExtendedResourceHandler):
    def __init__(self, resource_config, port_client, lambda_context, default_region):
        super().__init__(resource_config, port_client, lambda_context, default_region)
        self.ec2_client = boto3.client('ec2', region_name=self.region)

    def fetch_resources(self):
        response = self.ec2_client.describe_security_groups()
        logger.debug("security group response: %s" % response)
        return response.get('SecurityGroups', [])



