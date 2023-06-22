from typing import Dict, Type

from aws.resources.base_handler import BaseHandler
from aws.resources.cloudcontrol_handler import CloudControlHandler
from aws.resources.cloudformation_handler import CloudFormationHandler
from aws.resources.elastic_cloud_compute_handler import EC2InstanceHandler

SPECIAL_AWS_HANDLERS: Dict[str, Type[BaseHandler]] = {"AWS::CloudFormation::Stack": CloudFormationHandler, "AWS::EC2::Instance": EC2InstanceHandler}


def create_resource_handler(resource_config, port_client, lambda_context, default_region):
    handler = SPECIAL_AWS_HANDLERS.get(resource_config['kind'], CloudControlHandler)
    return handler(resource_config, port_client, lambda_context, default_region)
