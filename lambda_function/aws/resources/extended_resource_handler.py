import logging
import json, jq
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from aws.resources.base_handler import BaseHandler
from port.entities import handle_entities
import consts


class ExtendedResourceHandler(BaseHandler):
    def __init__(self, resource_config, port_client, lambda_context, default_region):
        super().__init__(resource_config, port_client, lambda_context, default_region)
        self.region = default_region
        self.logger = logging.getLogger(__name__)
        self.mappings = resource_config['port']['entity']['mappings'][0]

    def _process_resource(self, resource, mappings):
        # serialize datetime
        resource_json = json.dumps(resource, default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else x)

        def process_expression(expr):
            if expr.startswith('.'):
                try:
                    complied_json = jq.compile(expr).input(text=resource_json).all()
                    return complied_json[0] if len(complied_json) == 1 else complied_json
                except Exception as e:
                    self.logger.error(f"Error processing jq expression {expr}: {e}")
                    return expr
            else:
                return expr

        entity = {
            "identifier": process_expression(mappings['identifier']),
            "title": process_expression(mappings['title']),
            "blueprint": mappings['blueprint'],
            "properties": {k: process_expression(v) for k, v in mappings.get('properties', {}).items()},
            "relations": {k: process_expression(v) for k, v in mappings.get('relations', {}).items()},
            "mirrorProperties": {k: process_expression(v) for k, v in mappings.get('mirrorProperties', {}).items()},
            "calculationProperties": {k: process_expression(v) for k, v in mappings.get('calculationProperties', {}).items()}
        }

        self.logger.debug(f"resource {resource} entity: {entity}")
        return entity

    def _handle_resource(self, resource, mappings):
        entities = []
        skip_delete = False

        try:
            entity = self._process_resource(resource, mappings)
            entities.append(entity)
            aws_entities = handle_entities(entities, self.port_client)
        except Exception as e:
            self.logger.error(f"Failed to process resource: {resource}, error: {e}")
            skip_delete = True
            aws_entities = set()

        self.logger.debug(f"AWS entities: {aws_entities}")
        return {'aws_entities': aws_entities, 'skip_delete': skip_delete}

    def handle(self):
        try:
            self.logger.info("Fetching resources...")
            resources = self.fetch_resources()
            self.logger.info(f"Received {len(resources)} resources")

            with ThreadPoolExecutor(max_workers=consts.MAX_UPSERT_WORKERS) as executor:
                futures = [executor.submit(self._handle_resource, resource, self.mappings) for resource in resources]
                for future in as_completed(futures):
                    result = future.result()
                    self.aws_entities.update(result.get('aws_entities', set()))
                    self.skip_delete = result.get('skip_delete', False) if not self.skip_delete else self.skip_delete

            self.cleanup()

            self.logger.info("Finished processing resources")
            return self._handle_close_to_timeout()

        except Exception as e:
            self.logger.error(f"An error occurred while processing resources: {e}")
            self.cleanup()
            return self._handle_close_to_timeout()

    def fetch_resources(self):
        # Implement the logic to fetch resources from AWS
        raise NotImplementedError("Subclasses must implement fetch_resources() method")

    def cleanup(self):
        # Perform cleanup logic
        pass

    def _handle_close_to_timeout(self):
        if self.lambda_context.get_remaining_time_in_millis() < consts.REMAINING_TIME_TO_REINVOKE_THRESHOLD:
            if self.next_token:
                self.resource_config['next_token'] = self.next_token
            else:
                self.resource_config.pop('next_token', None)
            return {'aws_entities': self.aws_entities, 'next_resource_config': self.resource_config,
                    'skip_delete': self.skip_delete}
        return {'aws_entities': self.aws_entities, 'next_resource_config': None, 'skip_delete': self.skip_delete}
