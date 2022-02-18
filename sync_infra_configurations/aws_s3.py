import copy

import boto3

import sync_infra_configurations.common_action as common_action

def execute_buckets(action, is_new, src_data, session):
    s3_client = session.client("s3")
    return common_action.execute_elem_items(action, src_data, lambda: list_buckets(s3_client), common_action.null_item_executor)

def list_buckets(s3_client):
    result = []
    res = s3_client.list_buckets()
    for elem in res['Buckets']:
        name = elem["Name"]
        result.append(name)
    return result

