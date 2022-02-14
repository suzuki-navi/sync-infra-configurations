import copy

import boto3

import sync_infra_configurations.lib as sic_lib

def execute_buckets(action, src_data, session):
    s3_client = session.client("s3")
    return sic_lib.execute_elem_items(action, src_data, lambda: list_buckets(s3_client), sic_lib.null_item_executor)

def list_buckets(s3_client):
    result = []
    res = s3_client.list_buckets()
    for elem in res['Buckets']:
        name = elem["Name"]
        result.append(name)
    return result

