import copy
import sys

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.common_action as common_action
import sync_infra_configurations.aws as sic_aws

####################################################################################################
# GlueJob
####################################################################################################

def execute_gluejob(action, is_new, src_data, session):
    glue_client = session.client("glue")
    return common_action.execute_elem_properties(action, is_new, src_data,
        common_action.null_describe_fetcher,
        common_action.null_updator,
        {
            "Jobs": lambda action, is_new, src_data: execute_jobs(action, is_new, src_data, session, glue_client),
        },
    )

####################################################################################################
# GlueJob -> Jobs
####################################################################################################

def execute_jobs(action, is_new, src_data, session, glue_client):
    return common_action.execute_elem_items(action, src_data,
        lambda: list_jobs(glue_client),
        lambda action, is_new, name, src_data: execute_job(action, is_new, name, src_data, session, glue_client))

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

def execute_job(action, is_new, name, src_data, session, glue_client):
    return common_action.execute_elem_properties(action, is_new, src_data,
        lambda: describe_job(name, session, glue_client),
        lambda src_data, is_new: update_job(name, src_data, is_new, session, glue_client),
        {},
    )

def describe_job(name, session, glue_client):
    res = glue_client.get_job(JobName = name)
    info = copy.copy(res["Job"])
    sic_lib.removeKey(info, "Name")
    sic_lib.removeKey(info, "CreatedOn")
    sic_lib.removeKey(info, "LastModifiedOn")
    script_s3_path = info["Command"]["ScriptLocation"]
    script_source = fetch_script_source(script_s3_path, session)
    info["ScriptSource"] = script_source
    return info

def fetch_script_source(script_s3_path, session):
    script_source = sic_aws.fetch_s3_object(script_s3_path, session)
    lines = []
    for line in script_source.split("\n"):
        lines.append(line.rstrip(" \t\r"))
    while len(lines) > 0 and lines[0] == "":
        lines = lines[1:]
    while len(lines) > 0 and lines[len(lines) - 1] == "":
        lines = lines[0 : len(lines) - 1]
    return "\n".join(lines) + "\n"

def update_job(name, src_data, is_new, session, glue_client):
    if is_new:
        sic_main.add_update_message(f"glue_client.create_job(Name = {name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = modify_data_for_put(src_data)
            update_data["Name"] = name
            del update_data["ScriptSource"]
            glue_client.create_job(**update_data)

            script_s3_path = src_data["Command"]["ScriptLocation"]
            put_script_source(src_data["ScriptSource"], script_s3_path, session)

        return None

    elif src_data == None:
        # 削除
        raise Exception("TODO")

    else:
        curr_data = describe_job(name, session, glue_client)
        if src_data == curr_data:
            return curr_data

        src_data2 = copy.copy(src_data)
        curr_data2 = copy.copy(curr_data)
        del src_data2["ScriptSource"]
        del curr_data2["ScriptSource"]
        if src_data2 != curr_data2:
            sic_main.add_update_message(f"glue_client.update_job(JobName = {name} ...)")
            if sic_main.put_confirmation_flag:
                update_data = modify_data_for_put(src_data2)
                glue_client.update_job(JobName = name, JobUpdate = update_data)

        if src_data["ScriptSource"] != curr_data["ScriptSource"]:
            script_s3_path = src_data["Command"]["ScriptLocation"]
            put_script_source(src_data["ScriptSource"], script_s3_path, session)

        return curr_data

def modify_data_for_put(update_data):
    update_data = copy.copy(update_data)
    if update_data["WorkerType"] == "Standard":
        # MaxCapacity が必須で AllocatedCapacity の指定は不可
        sic_lib.removeKey(update_data, "AllocatedCapacity")
    elif "NumberOfWorkers" in update_data:
        sic_lib.removeKey(update_data, "AllocatedCapacity")
        sic_lib.removeKey(update_data, "MaxCapacity")
    else:
        sic_lib.removeKey(update_data, "AllocatedCapacity")
    return update_data

def put_script_source(script_source, script_s3_path, session):
    sic_aws.put_s3_object(script_s3_path, script_source, session)

####################################################################################################
