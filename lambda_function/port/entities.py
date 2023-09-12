import logging
from concurrent.futures import ThreadPoolExecutor

import jq

import consts

logger = logging.getLogger(__name__)


def handle_entities(entities, port_client, action_type="upsert"):
    def handle_concurrently(entities_to_handle):
        if entities_to_handle:
            with ThreadPoolExecutor(
                    max_workers=min(len(entities_to_handle), consts.MAX_PORT_WORKERS)) as executor:
                executor.map(lambda p: handle_entity(*p),
                             [(e, port_client, action_type) for e in entities_to_handle])

    if entities:
        not_dependent_entities = []
        not_dependent_entity_ids = set()
        for entity in entities:
            if not any(target_id in not_dependent_entity_ids
                       for target in entity.get('relations', {}).values()
                       for target_id in (target if isinstance(target, list) else [target])):
                not_dependent_entities.append(entity)
                not_dependent_entity_ids.add(entity.get('identifier'))
            else:
                handle_concurrently(not_dependent_entities)
                handle_entity(entity, port_client, action_type)
                not_dependent_entities = []
                not_dependent_entity_ids = set()

        handle_concurrently(not_dependent_entities)

    return {f"{entity.get('blueprint')};{entity.get('identifier')}" for entity in entities}


def handle_entity(entity, port_client, action_type="upsert"):
    try:
        if action_type == "upsert":
            port_client.upsert_entity(entity)
        elif action_type == "delete":
            port_client.delete_entity(entity)
    except Exception as e:
        logger.error(
            f"Failed to handle entity: {entity.get('identifier')} of blueprint: {entity.get('blueprint')}, action: {action_type}; {e}"
        )


def create_entities_json(
        resource_object, selector_jq_query, jq_mappings, action_type="upsert"
):
    def run_jq_query(jq_query):
        return jq.first(jq_query, resource_object)

    def dedup_list(lst):
        return [dict(tup) for tup in {tuple(obj.items()) for obj in lst}]

    if action_type == "delete":
        return dedup_list(
            [
                {
                    "identifier": resource_object["identifier"],
                    "blueprint": mapping.get("blueprint", "").strip('"') or raise_missing_exception("blueprint",
                                                                                                    mapping),
                }
                for mapping in jq_mappings
            ]
        )

    if selector_jq_query and not run_jq_query(selector_jq_query):
        return []

    entities = []
    for mapping in jq_mappings:
        items_to_parse = mapping.get('itemsToParse')
        if items_to_parse:
            items = run_jq_query(items_to_parse)
            items = items if isinstance(items, list) else []
            for item in items:
                entities.append(create_upsert_entity_json(mapping, resource_object | {'item': item}))
        else:
            entities.append(create_upsert_entity_json(mapping, resource_object))

    return entities


def create_upsert_entity_json(mapping, resource_object):
    def run_jq_query(jq_query):
        return jq.first(jq_query, resource_object)

    return {
        k: v
        for k, v in {
            "identifier": run_jq_query(mapping.get("identifier", "null"))
                          or raise_missing_exception("identifier", mapping),
            "title": run_jq_query(mapping.get("title", "null")) if mapping.get("title") else None,
            "blueprint": mapping.get("blueprint", "").strip('"') or raise_missing_exception("blueprint", mapping),
            "icon": run_jq_query(mapping.get("icon", "null")) if mapping.get("icon") else None,
            "team": run_jq_query(mapping.get("team", "null")) if mapping.get("team") else None,
            "properties": {
                prop_key: run_jq_query(prop_val)
                for prop_key, prop_val in mapping.get("properties", {}).items()
            },
            "relations": {
                             rel_key: run_jq_query(rel_val)
                             for rel_key, rel_val in mapping.get("relations", {}).items()
                         }
                         or None,
        }.items()
        if v is not None
    }


def raise_missing_exception(missing_field, mapping):
    raise Exception(
        f"Missing required field value for entity, field: {missing_field}, mapping: {mapping.get(missing_field)}"
    )
