import copy
import sys

import boto3

import sync_infra_configurations.lib as sic_lib

####################################################################################################
# DataCatalog
####################################################################################################

def execute_datacatalog(action, src_data, session):
    glue_client = session.client("glue")
    return sic_lib.execute_elem_properties(action, src_data,
        sic_lib.null_describe_fetcher,
        sic_lib.null_updator,
        {
            "Databases": lambda action, src_data: execute_databases(action, src_data, glue_client),
        },
    )

####################################################################################################
# DataCatalog -> Databases
####################################################################################################

def execute_databases(action, src_data, glue_client):
    return sic_lib.execute_elem_items(action, src_data,
        lambda: list_databases(glue_client),
        lambda action, name, src_data: execute_database(action, name, src_data, glue_client))

def list_databases(glue_client):
    result = []
    res = glue_client.get_databases()
    while True:
        for elem in res['DatabaseList']:
            name = elem["Name"]
            result.append(name)
        if not "NextToken" in res:
            break
        res = glue_client.get_databases(NextToken = res["NextToken"])
    return result

####################################################################################################
# DataCatalog -> Databases -> <database_name>
####################################################################################################

def execute_database(action, name, src_data, glue_client):
    return sic_lib.execute_elem_properties(action, src_data,
        lambda: describe_database(name, glue_client),
        sic_lib.null_updator,
        {
            "Tables": lambda action, src_data: execute_tables(action, name, src_data, glue_client),
        },
    )

def describe_database(name, glue_client):
    res = glue_client.get_database(Name = name)
    info = copy.deepcopy(res["Database"])
    del info["Name"]
    del info["CreateTime"]
    return info

####################################################################################################
# DataCatalog -> Databases -> <database_name> -> Tables
####################################################################################################

def execute_tables(action, database_name, src_data, glue_client):
    return sic_lib.execute_elem_items(action, src_data,
        lambda: list_tables(database_name, glue_client),
        lambda action, name, src_data: execute_table(action, database_name, name, src_data, glue_client))

def list_tables(database_name, glue_client):
    result = []
    res = glue_client.get_tables(DatabaseName = database_name)
    while True:
        for elem in res['TableList']:
            name = elem["Name"]
            result.append(name)
        if not "NextToken" in res:
            break
        res = glue_client.get_databases(NextToken = res["NextToken"])
    return result

####################################################################################################
# DataCatalog -> Databases -> <database_name> -> Tables -> <table_name>
####################################################################################################

def execute_table(action, database_name, table_name, src_data, glue_client):
    return sic_lib.execute_elem_properties(action, src_data,
        lambda: describe_table(database_name, table_name, glue_client),
        lambda src_data, is_preview: update_table(database_name, table_name, src_data, is_preview, glue_client),
        {},
    )

def describe_table(database_name, table_name, glue_client):
    res = glue_client.get_table(DatabaseName = database_name, Name = table_name)
    info = copy.deepcopy(res["Table"])
    del info["DatabaseName"]
    del info["Name"]
    del info["CreateTime"]
    del info["UpdateTime"]
    del info["CreatedBy"]
    del info["IsRegisteredWithLakeFormation"]
    return info

def update_table(database_name, table_name, src_data, is_preview, glue_client):
    curr_data = describe_table(database_name, table_name, glue_client)
    if src_data == curr_data:
        return (src_data, curr_data)
    cmd = f"glue_client.update_table(DatabaseName = {database_name}, Name = {table_name}, ...)"
    if not is_preview:
        update_data = copy.deepcopy(src_data)
        update_data["Name"] = table_name
        print(cmd, file = sys.stderr)
        glue_client.update_table(DatabaseName = database_name, TableInput = update_data)
    res_data = copy.deepcopy(src_data)
    res_data["#command"] = cmd
    return (res_data, curr_data)

####################################################################################################
