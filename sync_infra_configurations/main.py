import datetime
from re import S
import sys
import yaml

import sync_infra_configurations.aws as sic_aws
import sync_infra_configurations.lib as sic_lib

put_confirmation_flag = False

def main():
    global put_confirmation_flag
    help_flag, action, repeat_count, option_yaml, option_diff, confirm, src_file, dst_file, resource_type, resource_profile, resource_query = parse_args()

    resource_queries = []
    is_completion = False
    if src_file == None and resource_type != None:
        if resource_query == None:
            resource_query = ""
        query = {}
        data0 = {
            "type": resource_type,
            "profile": resource_profile,
            "resources": query,
        }
        if resource_query.endswith("."):
            resource_query = resource_query[:len(resource_query) - 1]
            is_completion = True
        if resource_query != "":
            resource_queries = resource_query.split(".")
            for elem in resource_queries:
                query1 = {}
                query[elem] = query1
                query = query1
    else:
        data0 = load_yaml(src_file)

    if resource_query != None:
        action = "get"
        repeat_count = 1

    if action == None:
        # actionが指定されていない場合はなにもせずにそのままYAML出力
        action = "get"
        repeat_count = 0
    elif action != "get":
        # getコマンド以外では --repeat オプションが無意味
        repeat_count = 1

    if action == "put":
        # putコマンドでは --confirm オプションをチェック
        if isinstance(confirm, str):
            check_confirm(confirm)
            confirm = True
        if not confirm:
            raise Exception(f"put action needs option --confirm")

        # バグにより意図せず更新してしまうの防ぐために更新系のAPIコールの直前にこのフラグをチェックしている
        # このフラグが True でないと更新系のAPIコールをしないようにしている
        put_confirmation_flag = True

    data1 = data0
    for i in range(repeat_count):
        if isinstance(data1, list):
            data2 = []
            for elem in data1:
                r = do_action(action, elem)
                data2.append(r)
        else:
            data2 = do_action(action, data1)
        data1 = data2

    if action == "preview" or action == "put":
        r1 = data1 # 更新前のクラウド側
        r2 = data0 # src
    else:
        r1 = data0 # src
        r2 = data1 # クラウド側
    if resource_query != None and not option_yaml and not option_diff:
        output_simple(data1, resource_queries, is_completion)
    else:
        if action == None and option_diff == None:
            option_yaml = True
        if action == "get" and option_diff == None:
            option_yaml = True
        if action == "preview" and option_yaml == None:
            option_diff = True
        if option_yaml:
            save_yaml(r2, dst_file)
        if option_diff:
            diff_yaml(r1, r2, dst_file)

def parse_args():
    help_flag = False
    dry_run = False
    action = None
    repeat_count = 1
    src_file = None
    dst_file = None
    i = 1
    argCount = len(sys.argv)
    option_i = False
    option_diff = None
    option_yaml = None
    confirm = False
    resource_type = None
    resource_profile = None
    resource_query = None
    while i < argCount:
        a = sys.argv[i]
        i = i + 1
        if a == "--help":
            help_flag = True
        elif a == "--dry-run":
            dry_run = True
        elif a == "--repeat":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            repeat_count = int(sys.argv[i])
            i = i + 1
        elif a == "-i":
            option_i = True
        elif a == "--diff":
            option_diff = True
        elif a == "--yaml":
            option_yaml = True
        elif a == "--force":
            confirm = True
        elif a == "--confirm":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            confirm = sys.argv[i]
            i = i + 1
        elif a == "--type":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            resource_type = sys.argv[i]
            i = i + 1
        elif a == "--profile":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            resource_profile = sys.argv[i]
            i = i + 1
        elif a == "--query":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            resource_query = sys.argv[i]
            i = i + 1
        elif a.startswith("-"):
            raise Exception(f"Unknown option: {a}")
        elif action == None:
            if a == "get":
                action = a
            elif a == "preview":
                action = a
            elif a == "put":
                action = a
            else:
                raise Exception(f"Unknown action: {a}")
        elif src_file == None:
            src_file = a
        elif dst_file == None:
            dst_file = a
        else:
            raise Exception(f"Unknown parameter: {a}")
    if dry_run and action == "put":
        action = "preview"
    if option_i and not option_diff and src_file != None and dst_file == None and action == "get":
        dst_file = src_file
    #if option_diff and dst_file != None:
    #    raise Exception(f"Unknown parameter: {dst_file}")
    return (help_flag, action, repeat_count, option_yaml, option_diff, confirm, src_file, dst_file, resource_type, resource_profile, resource_query)

def check_confirm(confirm):
    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(3):
        time_str = (now + datetime.timedelta(minutes = i - 1)).isoformat()
        hm = time_str[11:13] + time_str[14:16]
        if hm == confirm:
            return True
    time_str = now.isoformat()
    hm = time_str[11:13] + time_str[14:16]
    raise Exception(f"put action needs correct parameter --confirm {hm}")

def load_yaml(src_file):
    #loader = yaml.CLoader
    if src_file:
        with open(src_file) as f:
            data = yaml.safe_load(f)
    elif sys.stdin.isatty():
        raise Exception(f"src file not specified")
    else:
        data = yaml.safe_load(sys.stdin)
    return data

def represent_str(dumper, s):
    if "\n" in s:
        return dumper.represent_scalar('tag:yaml.org,2002:str', s, style='|')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', s)

yaml.add_representer(str, represent_str)

def output_simple(data1, resource_queries, is_completion):
    result = data1["resources"]
    for elem in resource_queries:
        result = result[elem]
    if result == None:
        pass
    elif isinstance(result, dict):
        is_simple = True
        if not is_completion:
            for name, value in result.items():
                if value != {}:
                    is_simple = False
        if is_simple:
            for name, value in result.items():
                print(name)
        else:
            save_yaml(result, None)
    elif isinstance(result, list):
        for name in result:
            print(name)
    elif isinstance(result, str):
        if not is_completion:
            print(result)
    elif isinstance(result, int):
        if not is_completion:
            print(result)
    else:
        if not is_completion:
            save_yaml(result, None)

def save_yaml(data, dst_file):
    yaml_str = yaml.dump(data, sort_keys = False, allow_unicode = True, width = 120, default_flow_style = False)
    if dst_file:
        with open(dst_file, "w") as f:
            f.write(yaml_str)
    else:
        sys.stdout.write(yaml_str)

def diff_yaml(src_data, dst_data, dst_file):
    src_yaml_str = yaml.dump(src_data, sort_keys = False, allow_unicode = True, width = 120)
    dst_yaml_str = yaml.dump(dst_data, sort_keys = False, allow_unicode = True, width = 120)
    sic_lib.exec_diff(src_yaml_str, dst_yaml_str, dst_file)

def do_action(action, src_data):
    global update_message_prefix
    if src_data["type"] == "aws":
        update_message_prefix = sic_aws.get_message_prefix(src_data)
        return sic_aws.do_action(action, src_data)
    else:
        return (src_data, src_data)

update_message_prefix = ""
update_message = []

# 更新系APIコールの直前で呼ばれる
def add_update_message(message):
    message = f"{update_message_prefix}: {message}"
    print(message, file = sys.stderr)
    update_message.append(message)
