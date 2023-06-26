import json
import logging
import os

import boto3

logger = logging.getLogger(__name__)

aws_secretsmanager_client = boto3.client("secretsmanager")
aws_s3_client = boto3.client("s3")


def get_config(event, lambda_context):
    logger.info("Load resources config from s3")
    resources_config = _get_resources_config(event, lambda_context)
    logger.info("Load port credentials from secrets manager")
    port_creds = _get_port_credentials(event)
    return {**{"event": event}, **resources_config, **port_creds}


def _get_resources_config(event, lambda_context):
    bucket_name = os.getenv("BUCKET_NAME")
    original_config_file_key = os.getenv("CONFIG_JSON_FILE_KEY")
    next_config_file_key = event.get("next_config_file_key")

    # Not supposed to happen. Just make sure to not accept the original config as next config, so it won't get deleted
    assert next_config_file_key != original_config_file_key, "next_config_file_key must not equal CONFIG_JSON_FILE_KEY"

    config_json_file_key = next_config_file_key or original_config_file_key
    config_from_s3 = json.loads(aws_s3_client.get_object(Bucket=bucket_name, Key=config_json_file_key)["Body"].read())

    assert "resources" in config_from_s3, "resources key is missing from config file json"

    if next_config_file_key:  # In case it's a re-invoked lambda
        # Clean config state from s3 after reading it
        try:
            aws_s3_client.delete_object(Bucket=bucket_name, Key=next_config_file_key)
        except Exception as e:
            logger.warning(f"Failed to clean config state, bucket: {bucket_name}, key: {next_config_file_key}; {e}")
    else:
        next_config_file_key = os.path.join(os.path.dirname(original_config_file_key), lambda_context.aws_request_id, "config.json")

    s3_config = {"bucket_name": bucket_name, "next_config_file_key": next_config_file_key}

    return {**config_from_s3, **s3_config}


def _get_port_credentials(event):
    if event.get("port_client_id"):
        return {
            **{
                key: event.get(key)
                for key in ["port_client_id", "port_client_secret", "port_api_url"]
            },
            **{"keep_cred": True},
        }

    secret_arn = os.getenv("PORT_CREDS_SECRET_ARN")
    port_creds = json.loads(
        aws_secretsmanager_client.get_secret_value(SecretId=secret_arn).get(
            "SecretString", "{}"
        )
    )
    return {
        "port_client_id": port_creds["id"],
        "port_client_secret": port_creds["clientSecret"],
    }
