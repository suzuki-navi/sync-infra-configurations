import copy
import sys

import sync_infra_configurations.lib as sic_lib

####################################################################################################
# GlueCrawlers
####################################################################################################

def execute_crawlers(action, src_data, session):
    glue_client = session.client("glue")
    return sic_lib.execute_elem_items(action, src_data,
        lambda: list_crawlers(glue_client),
        lambda action, name, src_data: execute_crawler(action, name, src_data, glue_client))

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
    return sic_lib.execute_elem_properties(action, src_data,
        lambda: describe_crawler(name, glue_client),
        lambda src_data, is_preview: update_crawler(name, src_data, is_preview, glue_client),
        {},
    )

def describe_crawler(name, glue_client):
    res = glue_client.get_crawler(Name = name)
    info = copy.deepcopy(res["Crawler"])
    del info["Name"]
    del info["CrawlElapsedTime"]
    del info["CreationTime"]
    del info["LastUpdated"]
    del info["LastCrawl"]
    del info["State"]
    del info["Version"]
    return info

def update_crawler(name, src_data, is_preview, glue_client):
    curr_data = describe_crawler(name, glue_client)
    if src_data == curr_data:
        return (src_data, curr_data)
    cmd = f"glue_client.update_crawler(Name = {name}, ...)"
    if not is_preview:
        update_data = copy.deepcopy(src_data)
        update_data["Name"] = name
        print(cmd, file = sys.stderr)
        glue_client.update_crawler(**update_data)
    res_data = copy.deepcopy(src_data)
    res_data["#command"] = cmd
    return (res_data, curr_data)

####################################################################################################
