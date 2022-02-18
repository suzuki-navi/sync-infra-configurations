import copy
import sys

import sync_infra_configurations.main as sic_main
import sync_infra_configurations.lib as sic_lib
import sync_infra_configurations.common_action as common_action

####################################################################################################
# GlueCrawlers
####################################################################################################

def execute_crawlers(action, is_new, src_data, session):
    glue_client = session.client("glue")
    return common_action.execute_elem_items(action, src_data,
        lambda: list_crawlers(glue_client),
        lambda action, is_new, name, src_data: execute_crawler(action, is_new, name, src_data, glue_client))

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

def execute_crawler(action, is_new, name, src_data, glue_client):
    return common_action.execute_elem_properties(action, is_new, src_data,
        lambda: describe_crawler(name, glue_client),
        lambda src_data, is_new, is_preview: update_crawler(name, src_data, is_new, is_preview, glue_client),
        {},
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

def update_crawler(name, src_data, is_new, is_preview, glue_client):
    if is_new:
        cmd = f"glue_client.create_crawler(Name = {name}, ...)"
        print(cmd, file = sys.stderr)
        if not is_preview:
            if not sic_main.put_confirmation_flag: # バグにより意図せず更新してしまうの防ぐために更新処理の直前にフラグをチェック
                raise Exception(f"put_confirmation_flag = False")
            update_data = copy.copy(src_data)
            update_data["Name"] = name
            glue_client.create_crawler(**update_data)
        return None

    elif src_data == None:
        # 削除
        raise Exception("TODO")

    else:
        curr_data = describe_crawler(name, glue_client)
        if src_data == curr_data:
            return curr_data
        cmd = f"glue_client.update_crawler(Name = {name}, ...)"
        print(cmd, file = sys.stderr)
        if not is_preview:
            if not sic_main.put_confirmation_flag: # バグにより意図せず更新してしまうの防ぐために更新処理の直前にフラグをチェック
                raise Exception(f"put_confirmation_flag = False")
            update_data = copy.deepcopy(src_data)
            update_data["Name"] = name
            glue_client.update_crawler(**update_data)
        return curr_data

####################################################################################################
