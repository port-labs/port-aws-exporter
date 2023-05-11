from aws.resources.cloudcontrol_handler import CloudControlHandler
from aws.resources.cloudformation_handler import CloudFormationHandler
from aws.resources.base_handler import BaseHandler
from typing import Dict, Type

SPECIAL_AWS_HANDLERS: Dict[str, Type[BaseHandler]] = {"AWS::CloudFormation::Stack": CloudFormationHandler}


def create_resource_handler(resource_config, port_client, lambda_context, default_region):
    if resource_config['kind'] in SPECIAL_AWS_HANDLERS:
        return SPECIAL_AWS_HANDLERS[resource_config['kind']](resource_config, port_client, lambda_context,
                                                             default_region)

    return CloudControlHandler(resource_config, port_client, lambda_context, default_region)
