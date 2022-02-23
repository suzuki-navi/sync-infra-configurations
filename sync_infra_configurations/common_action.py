import copy

import sync_infra_configurations.main as sic_main

# execute_elem_properties: リソースそのものを処理
# execute_elem_items: リソース一覧を処理

def execute_elem_properties(action, src_data, describe_fetcher, updator, executor_map, help_message_generator = None):
    if action == "put-new":
        dst_data = None
    else:
        dst_data = describe_fetcher()
    if isinstance(dst_data, dict):
        dst_data2 = {}
        if help_message_generator == None:
            help_message = {}
        else:
            help_message = help_message_generator()
        for name, msg in help_message.items():
            if name in dst_data:
                dst_data2[name] = dst_data[name]
        for name, value in dst_data.items():
            if not name in dst_data2:
                dst_data2[name] = value
        dst_data = dst_data2
    if action == "get":
        if src_data == None or isinstance(src_data, dict) and len(src_data) == 0:
            if isinstance(dst_data, dict):
                res_data = copy.copy(dst_data)
                for name in executor_map:
                    res_data[name] = {}
                for name, msg in help_message.items():
                    sic_main.put_help_message(name, msg)
            elif isinstance(dst_data, str):
                res_data = dst_data
        else:
            if isinstance(dst_data, dict):
                res_data = {}
                for name in src_data.keys():
                    if name in dst_data:
                        res_data[name] = dst_data[name]
                for name in dst_data.keys():
                    if not name in src_data:
                        res_data[name] = dst_data[name]
                for name in executor_map:
                    if name in src_data:
                        res_data[name] = executor_map[name](action, src_data[name])
                    #else:
                    #    res_data[name] = {}
            elif isinstance(dst_data, str):
                res_data = dst_data
    elif action == "put" or action == "put-new":
        if src_data == None:
            if dst_data == None:
                res_data = None
            # 削除
            res_data = {}
            res_data2 = {}
            for name in executor_map:
                res_data2[name] = executor_map[name]("put", None)
            updator(src_data, dst_data)
            res_data = copy.copy(dst_data)
            for name in executor_map:
                res_data[name] = res_data2[name]
        elif isinstance(src_data, dict) and len(src_data) == 0:
            pass
        else:
            # 作成または更新
            src_data2 = copy.copy(src_data)
            for name in executor_map:
                if name in src_data2:
                    del src_data2[name]
            if dst_data != src_data2:
                updator(src_data2, dst_data)
            res_data = copy.copy(dst_data)
            for name in executor_map:
                if name in src_data:
                    res_data[name] = executor_map[name]("put", src_data[name])
    return res_data

def execute_elem_items(action, src_data, list_fetcher, executor):
    items = list_fetcher()
    if action == "get":
        if src_data == None:
            pass
        elif isinstance(src_data, dict) and len(src_data) == 0:
            res_data = {}
            for name in items:
                res_data[name] = {}
        elif isinstance(src_data, list) and len(src_data) == 0:
            res_data = []
            for name in items:
                res_data.append(name)
        elif isinstance(src_data, dict):
            res_data = {}
            for name in src_data:
                if name in items:
                    res_data[name] = executor(action, name, src_data[name])
        elif isinstance(src_data, list):
            res_data = []
            for name in src_data:
                if name in items:
                    res_data.append(name)
            for name in items:
                if not name in src_data:
                    res_data.append(name)
    elif action == "put":
        if src_data == None:
            # 子要素をすべて削除の意味
            raise Exception("TODO")
        elif isinstance(src_data, dict):
            res_data = {}
            # 作成または更新または削除
            for name in src_data:
                if name in items:
                    res_data[name] = executor(action, name, src_data[name])
                else:
                    res_data[name] = executor("put-new", name, src_data[name])
        elif isinstance(src_data, list):
            res_data = []
            for name in src_data:
                if name in items:
                    res_data.append(name)
    return res_data

def null_describe_fetcher():
    return {}

def null_updator(src_data, curr_data):
    return src_data

def null_items_executor(action, src_data):
    return src_data

def null_item_executor(action, name, src_data):
    return src_data

