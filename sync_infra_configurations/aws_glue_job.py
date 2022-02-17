import copy
import sys

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.aws as sic_aws

####################################################################################################
# GlueJob
####################################################################################################

def execute_gluejob(action, is_new, src_data, session):
    glue_client = session.client("glue")
    return sic_lib.execute_elem_properties(action, is_new, src_data,
        sic_lib.null_describe_fetcher,
        sic_lib.null_updator,
        {
            "Jobs": lambda action, is_new, src_data: execute_jobs(action, is_new, src_data, session, glue_client),
        },
    )

####################################################################################################
# GlueJob -> Jobs
####################################################################################################

def execute_jobs(action, is_new, src_data, session, glue_client):
    return sic_lib.execute_elem_items(action, src_data,
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
    return sic_lib.execute_elem_properties(action, is_new, src_data,
        lambda: describe_job(name, session, glue_client),
        lambda src_data, is_new, is_preview: update_job(name, src_data, is_new, is_preview, session, glue_client),
        {},
    )

def describe_job(name, session, glue_client):
    res = glue_client.get_job(JobName = name)
    info = copy.deepcopy(res["Job"])
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
    return "\n".join(lines)

def update_job(name, src_data, is_new, is_preview, session, glue_client):
    if is_new:
        raise Exception("TODO")

    elif src_data == None:
        # 削除
        raise Exception("TODO")

    else:
        curr_data = describe_job(name, session, glue_client)
        if src_data == curr_data:
            return (src_data, curr_data)
        res_data = copy.deepcopy(src_data)

        src_data2 = copy.deepcopy(src_data)
        curr_data2 = copy.deepcopy(curr_data)
        del src_data2["ScriptSource"]
        del curr_data2["ScriptSource"]
        if src_data2 != curr_data2:
            cmd = f"glue_client.update_job(JobName = {name} ...)"
            print(cmd, file = sys.stderr)
            if not is_preview:
                if not sic_main.put_confirmation_flag:
                    raise Exception(f"put_confirmation_flag = False")
                update_data = copy.deepcopy(src_data2)
                glue_client.update_job(JobName = name, JobUpdate = update_data)
            #res_data["#command"].append(cmd)

        if src_data["ScriptSource"] != curr_data["ScriptSource"]:
            script_s3_path = src_data["Command"]["ScriptLocation"]
            put_script_source(src_data["ScriptSource"], script_s3_path, is_preview, session)
            #res_data["#command"].append(cmd)

        return (res_data, curr_data)

def put_script_source(script_source, script_s3_path, is_preview, session):
    sic_aws.put_s3_object(script_s3_path, script_source, is_preview, session)

####################################################################################################
