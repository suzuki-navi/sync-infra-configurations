import copy
import re
import sys

import boto3

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.aws_s3 as sic_aws_s3
import sync_infra_configurations.aws_glue_datacatalog as sic_aws_glue_datacatalog
import sync_infra_configurations.aws_glue_crawler as sic_aws_glue_crawler
import sync_infra_configurations.aws_glue_job as sic_aws_glue_job

def do_action(action, src_data):
    session = create_aws_session(src_data)
    res_data1 = copy.deepcopy(src_data)
    res_data2 = copy.deepcopy(src_data)
    if "resources" in src_data:
        res_data1["resources"], res_data2["resources"] = execute_elem_resources(action, False, src_data["resources"], session)
    return (res_data1, res_data2)

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

def execute_elem_resources(action, is_new, src_data, session):
    return sic_lib.execute_elem_properties(action, False, src_data,
        sic_lib.null_describe_fetcher,
        sic_lib.null_updator,
        {
            "S3Buckets": lambda action, is_new, src_data: sic_aws_s3.execute_buckets(action, is_new, src_data, session),
            "DataCatalog": lambda action, is_new, src_data: sic_aws_glue_datacatalog.execute_datacatalog(action, is_new, src_data, session),
            "GlueCrawlers": lambda action, is_new, src_data: sic_aws_glue_crawler.execute_crawlers(action, is_new, src_data, session),
            "GlueJob": lambda action, is_new, src_data: sic_aws_glue_job.execute_gluejob(action, is_new, src_data, session),
        },
    )

def fetch_s3_object(s3_path: str, session):
    s3_client = session.client("s3")
    m = re.compile("\As3://([^/]+)/(.*)\Z").search(s3_path)
    if not m:
        return None
    s3_bucket = m.group(1)
    s3_key = m.group(2)
    res = s3_client.get_object(Bucket = s3_bucket, Key = s3_key)
    body = res['Body'].read()
    body_str = body.decode('utf-8')
    return body_str

def put_s3_object(s3_path: str, body: str, is_preview: bool, session):
    s3_client = session.client("s3")
    m = re.compile("\As3://([^/]+)/(.*)\Z").search(s3_path)
    if not m:
        return None
    s3_bucket = m.group(1)
    s3_key = m.group(2)
    print(f"s3_client.put_object(Bucket = {s3_bucket}, Key = {s3_key}, ...)", file = sys.stderr)
    if not is_preview:
        if not sic_main.put_confirmation_flag:
            raise Exception(f"put_confirmation_flag = False")
        res = s3_client.put_object(Bucket = s3_bucket, Key = s3_key, Body = body.encode('utf-8'))

