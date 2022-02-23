import copy

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.common_action as common_action

####################################################################################################
# DataCatalog
####################################################################################################

def execute_datacatalog(action, src_data, session):
    glue_client = session.client("glue")
    return common_action.execute_elem_properties(action, src_data,
        common_action.null_describe_fetcher,
        common_action.null_updator,
        {
            "Databases": lambda action, src_data: execute_databases(action, src_data, glue_client),
        },
    )

####################################################################################################
# DataCatalog -> Databases
####################################################################################################

def execute_databases(action, src_data, glue_client):
    return common_action.execute_elem_items(action, src_data,
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
    return common_action.execute_elem_properties(action, src_data,
        lambda: describe_database(name, glue_client),
        lambda src_data, curr_data: update_database(name, src_data, curr_data, glue_client),
        {
            "Tables": lambda action, src_data: execute_tables(action, name, src_data, glue_client),
        },
    )

def describe_database(name, glue_client):
    res = glue_client.get_database(Name = name)
    info = copy.copy(res["Database"])
    sic_lib.removeKey(info, "Name")
    sic_lib.removeKey(info, "CreateTime")
    sic_lib.removeKey(info, "CatalogId")
    return info

def update_database(name, src_data, curr_data, glue_client):
    if curr_data == None:
        # 新規作成
        sic_main.add_update_message(f"glue_client.create_database(Name = {name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = copy.copy(src_data)
            update_data["Name"] = name
            glue_client.create_database(DatabaseInput = update_data)

    elif src_data == None:
        # 削除
        raise Exception("TODO")

    else:
        # 更新
        sic_main.add_update_message(f"glue_client.update_database(Name = {name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = copy.copy(src_data)
            update_data["Name"] = name
            glue_client.update_database(Name = name, DatabaseInput = update_data)

####################################################################################################
# DataCatalog -> Databases -> <database_name> -> Tables
####################################################################################################

def execute_tables(action, database_name, src_data, glue_client):
    return common_action.execute_elem_items(action, src_data,
        lambda: list_tables(database_name, glue_client),
        lambda action, name, src_data: execute_table(action,  database_name, name, src_data, glue_client))

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
    return common_action.execute_elem_properties(action, src_data,
        lambda: describe_table(database_name, table_name, glue_client),
        lambda src_data, curr_data: update_table(database_name, table_name, src_data, curr_data, glue_client),
        {},
    )

def describe_table(database_name, table_name, glue_client):
    res = glue_client.get_table(DatabaseName = database_name, Name = table_name)
    info = copy.copy(res["Table"])
    sic_lib.removeKey(info, "DatabaseName")
    sic_lib.removeKey(info, "Name")
    sic_lib.removeKey(info, "CreateTime")
    sic_lib.removeKey(info, "UpdateTime")
    sic_lib.removeKey(info, "LastAccessTime")
    sic_lib.removeKey(info, "LastAnalyzedTime")
    sic_lib.removeKey(info, "CreatedBy")
    sic_lib.removeKey(info, "IsRegisteredWithLakeFormation")
    sic_lib.removeKey(info, "CatalogId")
    return info

def update_table(database_name, table_name, src_data, curr_data, glue_client):
    if curr_data == None:
        # 新規作成
        sic_main.add_update_message(f"glue_client.create_table(DatabaseName = {database_name}, Name = {table_name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = copy.copy(src_data)
            update_data["Name"] = table_name
            glue_client.create_table(DatabaseName = database_name, TableInput = update_data)

    elif src_data == None:
        # 削除
        raise Exception("TODO")

    else:
        # 更新
        sic_main.add_update_message(f"glue_client.update_table(DatabaseName = {database_name}, Name = {table_name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = copy.copy(src_data)
            update_data["Name"] = table_name
            glue_client.update_table(DatabaseName = database_name, TableInput = update_data)

####################################################################################################
