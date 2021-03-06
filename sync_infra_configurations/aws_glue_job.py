import copy

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.common_action as common_action
import sync_infra_configurations.aws as sic_aws

####################################################################################################
# GlueJob
####################################################################################################

def execute_gluejob(action, src_data, session):
    glue_client = session.client("glue")
    return common_action.execute_elem_properties(action, src_data,
        executor_map = {
            "Jobs": lambda action, src_data: execute_jobs(action, src_data, session, glue_client),
        },
    )

####################################################################################################
# GlueJob -> Jobs
####################################################################################################

def execute_jobs(action, src_data, session, glue_client):
    return common_action.execute_elem_items(action, src_data,
        list_fetcher = lambda: list_jobs(glue_client),
        item_executor = lambda action, name, src_data: execute_job(action, name, src_data, session, glue_client))

def list_jobs(glue_client):
    result = []
    res = glue_client.get_jobs()
    while True:
        for elem in res['Jobs']:
            name = elem["Name"]
            result.append(name)
        if not "NextToken" in res:
            break
        res = glue_client.get_jobs(NextToken = res["NextToken"])
    return result

####################################################################################################
# DataCatalog -> Databases -> <database_name>
####################################################################################################

def execute_job(action, name, src_data, session, glue_client):
    return common_action.execute_elem_properties(action, src_data,
        describer = lambda: describe_job(name, session, glue_client),
        updator = lambda src_data, curr_data: update_job(name, src_data, curr_data, session, glue_client),
        executor_map = {
            "ScriptSource": lambda action, src_data: execute_scriptsource(action, name, src_data, session, glue_client),
        },
        help_generator = help_job,
    )

def help_job():
    return {
        "Description": "a description of the job",
        "Role": "the name or ARN of the IAM role associated with this job",
        "ExecutionProperty": "the maximum number of concurrent runs allowed for this job",
        "Command": "the JobCommand that runs this job",
        "DefaultArguments": "the default arguments for this job, specified as name-value pairs",
        "NonOverridableArguments": "non-overridable arguments for this job, specified as name-value pairs",
        "Connections": "the connections used for this job",
        "MaxRetries": "the maximum number of times to retry this job after a JobRun fails",
        "Timeout": "he job timeout in minutes",
        "AllocatedCapacity": "this field is deprecated. Use MaxCapacity instead",
        "MaxCapacity": "For Glue version 1.0 or earlier jobs, using the standard worker type, the number of Glue data processing units (DPUs) that can be allocated when this job runs.",
        "WorkerType": "The type of predefined worker that is allocated when a job runs. Accepts a value of Standard, G.1X, or G.2X.",
        "NumberOfWorkers": "he number of workers of a defined workerType that are allocated when a job runs",
        "SecurityConfiguration": "the name of the SecurityConfiguration structure to be used with this job",
        "NotificationProperty": "specifies configuration properties of a job notification",
        "GlueVersion": "Glue version determines the versions of Apache Spark and Python that Glue supports.",
        "ScriptSource": "Script in S3",
    }

def describe_job(name, session, glue_client):
    res = glue_client.get_job(JobName = name)
    info = copy.copy(res["Job"])
    sic_lib.removeKey(info, "Name")
    sic_lib.removeKey(info, "CreatedOn")
    sic_lib.removeKey(info, "LastModifiedOn")
    return info

def update_job(name, src_data, curr_data, session, glue_client):
    if curr_data == None:
        # ????????????
        sic_main.add_update_message(f"glue_client.create_job(Name = {name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = modify_data_for_put(src_data)
            update_data["Name"] = name
            glue_client.create_job(**update_data)

    elif src_data == None:
        # ??????
        raise Exception("TODO")

    else:
        # ??????
        sic_main.add_update_message(f"glue_client.update_job(JobName = {name} ...)")
        if sic_main.put_confirmation_flag:
            update_data = modify_data_for_put(src_data)
            glue_client.update_job(JobName = name, JobUpdate = update_data)

def modify_data_for_put(update_data):
    update_data = copy.copy(update_data)
    if update_data["WorkerType"] == "Standard":
        # MaxCapacity ???????????? AllocatedCapacity ??????????????????
        sic_lib.removeKey(update_data, "AllocatedCapacity")
    elif "NumberOfWorkers" in update_data:
        sic_lib.removeKey(update_data, "AllocatedCapacity")
        sic_lib.removeKey(update_data, "MaxCapacity")
    else:
        sic_lib.removeKey(update_data, "AllocatedCapacity")
    return update_data

####################################################################################################
# DataCatalog -> Databases -> <database_name> -> ScriptSource
####################################################################################################

def execute_scriptsource(action, name, src_data, session, glue_client):
    return common_action.execute_elem_properties(action, src_data,
        describer = lambda: describe_scriptsource(name, session, glue_client),
        updator = lambda src_data, curr_data: update_scriptsource(name, src_data, curr_data, session, glue_client),
    )

def describe_scriptsource(name, session, glue_client):
    info = describe_job(name, session, glue_client)
    script_s3_path = info["Command"]["ScriptLocation"]
    script_source = fetch_script_source(script_s3_path, session)
    info["ScriptSource"] = script_source
    return script_source

def update_scriptsource(name, src_data, curr_data, session, glue_client):
    info = describe_job(name, session, glue_client)
    script_s3_path = info["Command"]["ScriptLocation"]

    if curr_data == None:
        # ????????????
        put_script_source(src_data, script_s3_path, session)

    elif src_data == None:
        # ??????
        raise Exception("TODO")

    else:
        # ??????
        put_script_source(src_data, script_s3_path, session)

def fetch_script_source(script_s3_path, session):
    script_source = sic_aws.fetch_s3_object(script_s3_path, session)
    if script_source == None:
        return ""
    lines = []
    for line in script_source.split("\n"):
        lines.append(line.rstrip(" \t\r"))
    while len(lines) > 0 and lines[0] == "":
        lines = lines[1:]
    while len(lines) > 0 and lines[len(lines) - 1] == "":
        lines = lines[0 : len(lines) - 1]
    return "\n".join(lines) + "\n"

def put_script_source(script_source, script_s3_path, session):
    sic_aws.put_s3_object(script_s3_path, script_source, session)

####################################################################################################
