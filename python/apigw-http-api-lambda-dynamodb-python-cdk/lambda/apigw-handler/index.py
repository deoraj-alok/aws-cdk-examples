# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    request_id = context.request_id
    table = os.environ.get("TABLE_NAME")
    
    # Extract request context
    request_context = event.get("requestContext", {})
    identity = request_context.get("identity", {})
    
    # Structured log with context
    logger.info(json.dumps({
        "message": "Processing request",
        "request_id": request_id,
        "table_name": table,
        "source_ip": identity.get("sourceIp"),
        "user_agent": identity.get("userAgent"),
    }))
    
    try:
        if event.get("body"):
            item = json.loads(event["body"])
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            
            # Log without sensitive data
            logger.info(json.dumps({
                "message": "Inserting item",
                "request_id": request_id,
                "item_id": id,
            }))
            
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            logger.info(json.dumps({
                "message": "No payload received, using default",
                "request_id": request_id,
            }))
            
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": str(uuid.uuid4())},
                },
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        logger.error(json.dumps({
            "message": "Error processing request",
            "request_id": request_id,
            "error": str(e),
            "error_type": type(e).__name__,
        }))
        raise
