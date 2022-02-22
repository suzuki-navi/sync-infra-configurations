import copy

# execute_elem_properties: リソースそのものを処理
# execute_elem_items: リソース一覧を処理

def execute_elem_properties(action, is_new, src_data, describe_fetcher, updator, executor_map):
    if action == "get":
        if src_data == None or len(src_data) == 0:
            res_data = copy.copy(describe_fetcher())
            for name in executor_map:
                res_data[name] = {}
        else:
            dst_data = describe_fetcher()
            res_data = {}
            for name in src_data.keys():
                if name in dst_data:
                    res_data[name] = dst_data[name]
            for name in dst_data.keys():
                if not name in src_data:
                    res_data[name] = dst_data[name]
            for name in executor_map:
                if name in src_data:
                    res_data[name] = executor_map[name](action, False, src_data[name])
                #else:
                #    res_data[name] = {}
    elif action == "put":
        curr_data = describe_fetcher()
        if src_data == None:
            if curr_data == None:
                raise Exception()
            # 削除
            res_data = {}
            res_data2 = {}
            for name in executor_map:
                res_data2[name] = executor_map[name](action, False, None)
            updator(src_data, curr_data)
            res_data = copy.copy(curr_data)
            for name in executor_map:
                res_data[name] = res_data2[name]
        elif len(src_data) == 0:
            pass
        else:
            # 作成または更新
            if curr_data == src_data:
                return curr_data
            src_data2 = copy.copy(src_data)
            for name in executor_map:
                if name in src_data2:
                    del src_data2[name]
            updator(src_data2, curr_data)
            res_data = copy.copy(curr_data)
            for name in executor_map:
                if name in src_data:
                    res_data[name] = executor_map[name](action, False, src_data[name])
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
                    res_data[name] = executor(action, False, name, src_data[name])
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
                is_new = not (name in items)
                res_data[name] = executor(action, is_new, name, src_data[name])
        elif isinstance(src_data, list):
            res_data = []
            for name in src_data:
                is_new = not (name in items)
                res_data[name] = executor(action, is_new, name, src_data[name])
    return res_data

def null_describe_fetcher():
    return {}

def null_updator(src_data, is_new):
    return src_data

def null_items_executor(action, src_data):
    return src_data

def null_item_executor(action, name, src_data):
    return src_data

