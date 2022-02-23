import copy
import re
import sys

import boto3
import botocore.exceptions

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.common_action as common_action
import sync_infra_configurations.aws_s3               as sic_aws_s3
import sync_infra_configurations.aws_glue_datacatalog as aws_glue_datacatalog
import sync_infra_configurations.aws_glue_crawler     as aws_glue_crawler
import sync_infra_configurations.aws_glue_job         as aws_glue_job
import sync_infra_configurations.aws_stepfunctions    as aws_stepfunctions

def get_message_prefix(data):
    if "profile" in data:
        profile = data["profile"]
    else:
        profile = "default"
    ret = f"aws(proifle={profile}"
    if "region" in data:
        region = data["region"]
        ret = ret + ", region={region}"
    ret = ret + ")"
    return ret

def do_action(action, src_data):
    session = create_aws_session(src_data)
    res_data = copy.copy(src_data)
    if "resources" in src_data:
        res_data["resources"] = execute_elem_resources(action, src_data["resources"], session)
    return res_data

def create_aws_session(data):
    if "profile" in data:
        profile = data["profile"]
    else:
        profile = "default"
    if "region" in data:
        region = data["region"]
    else:
        region = None
    session = boto3.session.Session(profile_name = profile, region_name = region)
    return session

def execute_elem_resources(action, src_data, session):
    return common_action.execute_elem_properties(action, src_data,
        common_action.null_describe_fetcher,
        common_action.null_updator,
        {
            "S3Buckets":     lambda action, src_data: aws_s3.execute_buckets(action, src_data, session),
            "DataCatalog":   lambda action, src_data: aws_glue_datacatalog.execute_datacatalog(action, src_data, session),
            "GlueCrawlers":  lambda action, src_data: aws_glue_crawler.execute_crawlers(action, src_data, session),
            "GlueJob":       lambda action, src_data: aws_glue_job.execute_gluejob(action, src_data, session),
            "StepFunctions": lambda action, src_data: aws_stepfunctions.execute_stepfunctions(action, src_data, session),
        },
    )

def fetch_account_id(session):
    account_id = session.client("sts").get_caller_identity()["Account"]
    return account_id

def fetch_s3_object(s3_path: str, session):
    s3_client = session.client("s3")
    m = re.compile("\As3://([^/]+)/(.*)\Z").search(s3_path)
    if not m:
        return None
    s3_bucket = m.group(1)
    s3_key = m.group(2)
    try:
        res = s3_client.get_object(Bucket = s3_bucket, Key = s3_key)
        body = res['Body'].read()
        body_str = body.decode('utf-8')
        return body_str
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return ""
        else:
            raise

def put_s3_object(s3_path: str, body: str, session):
    s3_client = session.client("s3")
    m = re.compile("\As3://([^/]+)/(.*)\Z").search(s3_path)
    if not m:
        return None
    s3_bucket = m.group(1)
    s3_key = m.group(2)
    sic_main.add_update_message(f"s3_client.put_object(Bucket = {s3_bucket}, Key = {s3_key}, ...)")
    if sic_main.put_confirmation_flag:
        res = s3_client.put_object(Bucket = s3_bucket, Key = s3_key, Body = body.encode('utf-8'))

