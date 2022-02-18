import copy

def execute_elem_properties(action, is_new, src_data, describe_fetcher, updator, executor_map):
    res_data1 = copy.deepcopy(src_data)
    res_data2 = copy.deepcopy(src_data)
    if action == "get":
        if src_data == None or len(src_data) == 0:
            res_data2 = describe_fetcher()
            for name in executor_map:
                res_data2[name] = {}
        else:
            dst_data = describe_fetcher()
            res_data2 = {}
            for name in src_data.keys():
                if name in dst_data:
                    res_data2[name] = dst_data[name]
            for name in dst_data.keys():
                if not name in src_data:
                    res_data2[name] = dst_data[name]
            for name in executor_map:
                if name in src_data:
                    res_data1[name], res_data2[name] = executor_map[name](action, False, src_data[name])
                #else:
                #    res_data2[name] = {}
    elif action == "preview" or action == "put":
        if src_data == None:
            # 削除
            for name in executor_map:
                if name in src_data:
                    res_data1[name], res_data2[name] = executor_map[name](action, False, None)
            res_data1, res_data2 = updator(None, is_new, action == "preview")
        elif len(src_data) == 0:
            pass
        else:
            # 作成または更新
            src_data2 = copy.deepcopy(src_data)
            for name in executor_map:
                if name in src_data2:
                    del src_data2[name]
            res_data1, res_data2 = updator(src_data2, is_new, action == "preview")
            res_data1 = copy.deepcopy(res_data1)
            res_data2 = copy.deepcopy(res_data2)
            for name in executor_map:
                if name in src_data:
                    res_data1[name], res_data2[name] = executor_map[name](action, False, src_data[name])
    return (res_data1, res_data2)

def execute_elem_items(action, src_data, list_fetcher, executor):
    res_data1 = copy.deepcopy(src_data)
    res_data2 = copy.deepcopy(src_data)
    items = list_fetcher()
    if action == "get":
        if src_data == None:
            pass
        elif isinstance(src_data, dict) and len(src_data) == 0:
            res_data2 = {}
            for name in items:
                res_data2[name] = {}
        elif isinstance(src_data, list) and len(src_data) == 0:
            res_data2 = []
            for name in items:
                res_data2.append(name)
        elif isinstance(src_data, dict):
            for name in src_data:
                if name in items:
                    res_data1[name], res_data2[name] = executor(action, False, name, src_data[name])
                else:
                    del res_data2[name]
        elif isinstance(src_data, list):
            res_data2 = []
            for name in src_data:
                if name in items:
                    res_data2.append(name)
            for name in items:
                if not name in src_data:
                    res_data2.append(name)
    elif action == "preview" or action == "put":
        if src_data == None:
            pass
        elif isinstance(src_data, dict):
            res_data1 = {}
            res_data2 = {}
            # 作成または更新または削除
            for name in src_data:
                is_new = not (name in items)
                res_data1[name], res_data2[name] = executor(action, is_new, name, src_data[name])
        elif isinstance(src_data, list):
            pass
    return (res_data1, res_data2)

def null_describe_fetcher():
    return {}

def null_updator(src_data, is_new, is_preview):
    return (src_data, src_data)

def null_items_executor(action, src_data):
    return (src_data, src_data)

def null_item_executor(action, name, src_data):
    return (src_data, src_data)

