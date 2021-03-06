import copy

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.common_action as common_action

####################################################################################################
# GlueCrawlers
####################################################################################################

def execute_crawlers(action, is_new, src_data, session):
    glue_client = session.client("glue")
    return common_action.execute_elem_items(action, src_data,
        list_fetcher = lambda: list_crawlers(glue_client),
        item_executor = lambda action, name, src_data: execute_crawler(action, name, src_data, glue_client))

def list_crawlers(glue_client):
    result = []
    res = glue_client.get_crawlers()
    while True:
        for elem in res['Crawlers']:
            name = elem["Name"]
            result.append(name)
        if not "NextToken" in res:
            break
        res = glue_client.get_crawlers(NextToken = res["NextToken"])
    return result

####################################################################################################
# GlueCrawlers -> <crawler_name>
####################################################################################################

def execute_crawler(action, name, src_data, glue_client):
    return common_action.execute_elem_properties(action, src_data,
        describe_fetcher = lambda: describe_crawler(name, glue_client),
        updator = lambda src_data, curr_data: update_crawler(name, src_data, curr_data, glue_client),
    )

def describe_crawler(name, glue_client):
    res = glue_client.get_crawler(Name = name)
    info = copy.copy(res["Crawler"])
    sic_lib.removeKey(info, "Name")
    sic_lib.removeKey(info, "CrawlElapsedTime")
    sic_lib.removeKey(info, "CreationTime")
    sic_lib.removeKey(info, "LastUpdated")
    sic_lib.removeKey(info, "LastCrawl")
    sic_lib.removeKey(info, "State")
    sic_lib.removeKey(info, "Version")
    return info

def update_crawler(name, src_data, curr_data, glue_client):
    if curr_data == None:
        # ????????????
        sic_main.add_update_message(f"glue_client.create_crawler(Name = {name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = copy.copy(src_data)
            update_data["Name"] = name
            glue_client.create_crawler(**update_data)

    elif src_data == None:
        # ??????
        raise Exception("TODO")

    else:
        # ??????
        sic_main.add_update_message(f"glue_client.update_crawler(Name = {name}, ...)")
        if sic_main.put_confirmation_flag:
            update_data = copy.deepcopy(src_data)
            update_data["Name"] = name
            glue_client.update_crawler(**update_data)

####################################################################################################
