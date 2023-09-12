import copy
import logging
import urllib.parse

import requests

logger = logging.getLogger(__name__)


class PortClient:
    def __init__(self, client_id, client_secret, user_agent, api_url):
        self.api_url = api_url
        self.access_token = self.get_token(client_id, client_secret)
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "User-Agent": user_agent,
        }

    def get_token(self, client_id, client_secret):
        credentials = {"clientId": client_id, "clientSecret": client_secret}
        token_response = requests.post(
            f"{self.api_url}/auth/access_token", json=credentials
        )
        token_response.raise_for_status()
        return token_response.json()["accessToken"]

    def upsert_entity(self, entity):
        entity_to_upsert = copy.deepcopy(entity)
        blueprint_id = entity_to_upsert.pop("blueprint")
        logger.info(
            f"Upsert entity: {entity_to_upsert.get('identifier')} of blueprint: {blueprint_id}"
        )
        requests.post(
            f'{self.api_url}/blueprints/{urllib.parse.quote(blueprint_id, safe="")}/entities',
            json=entity_to_upsert,
            headers=self.headers,
            params={"upsert": "true", "merge": "true"},
        ).raise_for_status()

    def delete_entity(self, entity):
        entity_to_delete = copy.deepcopy(entity)
        blueprint_id = entity_to_delete.pop("blueprint")
        entity_id = entity_to_delete.pop("identifier")
        logger.info(f"Delete entity: {entity_id} of blueprint: {blueprint_id}")
        requests.delete(
            f'{self.api_url}/blueprints/{urllib.parse.quote(blueprint_id, safe="")}/entities/{urllib.parse.quote(entity_id, safe="")}',
            headers=self.headers,
            params={"delete_dependents": "true"},
        ).raise_for_status()

    def search_entities(self, query):
        search_req = requests.post(
            f"{self.api_url}/entities/search",
            json=query,
            headers=self.headers,
            params={
                "exclude_calculated_properties": "true",
                "include": ["blueprint", "identifier"],
            },
        )
        search_req.raise_for_status()
        return search_req.json()["entities"]

    def upsert_integration(self, integration):
        logger.info(
            f"Upsert integration: {integration.get('installationId')}"
        )
        requests.post(f'{self.api_url}/integration',
                      json=integration,
                      headers=self.headers,
                      params={"upsert": "true"},
                      ).raise_for_status()
