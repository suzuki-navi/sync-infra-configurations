import datetime
from re import S
import sys
import yaml

import sync_infra_configurations.aws as sic_aws
import sync_infra_configurations.lib as sic_lib

put_confirmation_flag = False

def main():
    (help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm) = parse_args()
    (help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm) = check_args \
        (help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm)
    exec_main \
        (help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm)

def parse_args():
    help_flag = False
    action = None # get, put, exec
    output_format = None # y, yaml
    is_diff = None
    type = None # aws
    profile = None
    path = None
    src_file = None
    is_inplace = False
    repeat_count = None
    is_dryrun = False
    confirm = None
    i = 1
    argCount = len(sys.argv)
    while i < argCount:
        a = sys.argv[i]
        i = i + 1
        if a == "--help":
            help_flag = True
        elif a == "-y":
            output_format = "y"
        elif a == "--yaml":
            output_format = "yaml"
        elif a == "--diff":
            is_diff = True
        elif a == "--no-diff":
            is_diff = False
        elif a == "--profile":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            profile = sys.argv[i]
            i = i + 1
        elif a == "-p":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            path = sys.argv[i]
            i = i + 1
        elif a == "-s":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            src_file = sys.argv[i]
            i = i + 1
        #elif a == "-i":
        #    is_inplace = True
        elif a == "--repeat":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            repeat_count = int(sys.argv[i])
            i = i + 1
        elif a == "--force":
            confirm = True
        elif a == "--dry-run":
            is_dryrun = True
        elif a == "--confirm":
            if i >= argCount:
                raise Exception(f"Option parameter not found: {a}")
            confirm = sys.argv[i]
            i = i + 1
        elif a.startswith("-"):
            raise Exception(f"Unknown option: {a}")
        elif action == None and a == "get":
            action = "get"
        elif action == None and a == "put":
            action = "put"
        #elif action == None and a == "exec":
        #    action = "exec"
        elif a == "aws":
            type = "aws"
        else:
            raise Exception(f"Unknown parameter: {a}")
    return (help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm)

def check_args(help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm):
    if path != None and type == None:
        raise Exception("-p option needs aws parameter")

    # pathが . で終わっている場合はその次に続く文字列候補を出力する
    if path != None and path.endswith("."):
        path = path[0:len(path) - 1]
        action = "get"
        output_format = "completion"
        is_diff = False
        src_file = None
        is_inplace = False
        repeat_count = 1

    # actionの指定がない場合は get とみなす
    if action == None:
        action = "get"
        if repeat_count == None:
            if type == None and src_file == None:
                # 標準入力から取り込む場合はデフォルトは0
                repeat_count = 0
            else:
                repeat_count = 1

    # is_diffの指定がない場合のデフォルト値設定
    if action == "get":
        if is_diff == None:
            is_diff = False
    elif action == "put":
        if is_dryrun:
            if is_diff == None:
                is_diff = True
        else:
            pass

    # 入力がない場合はエラー
    if type == None and src_file == None and sys.stdin.isatty():
        raise Exception(f"either aws parameter or -s must be expected")

    # put の場合は -p 形式が未実装
    if action == "put":
        if type != None:
            raise Exception("TODO")

    # put の場合は -y 形式が未実装
    if action == "put":
        if output_format == "y":
            raise Exception("TODO")

    # -y の場合は -p が必須
    if output_format == "y":
        if type == None:
            raise Exception(f"-y option needs aws parameter")

    # --repeat は get でのみ有効
    if action != "get":
        if repeat_count != None:
            raise Exception(f"put action must not have --repeat option")
    if repeat_count == None:
        repeat_count = 1

    #if is_inplace and src_file == None:
    #    raise Exception("-i option needs -s option")

    #if is_inplace and action != "get":
    #    raise Exception("-i option needs get command")

    #if type != None and src_file != None:
    #    raise Exception(f"only one of {type} and -s can be specified")

    #if action == "get":
    #    pass
    #elif action == "put":
    #    if type != None:
    #        if output_format == "yaml":
    #            raise Exception(f"only one of {type} and --yaml when put action")
    #elif action == "exec":
    #    raise Exception(f"TODO")

    if type != None:
        if path == None or path == "":
            path = []
        else:
            path = path.split(".")
        if output_format == None:
            output_format = "simple"

    if output_format == None:
        output_format = "yaml"

    return (help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm)

def exec_main(help_flag, action, output_format, is_diff, type, profile, path, src_file, is_dryrun, is_inplace, repeat_count, confirm):
    global put_confirmation_flag

    if action == "put":
        if not is_dryrun:
            # putコマンドでは --confirm オプションをチェック
            if confirm == None:
                raise Exception("put action needs --dry-run or --confirm HHMM")
            if confirm != True:
                check_confirm(confirm)
                confirm = True

            # バグにより意図せず更新してしまうの防ぐために更新系のAPIコールの直前にこのフラグをチェックしている
            # このフラグが True でないと更新系のAPIコールをしないようにしている
            # --dry-run のときは False
            put_confirmation_flag = True

    if path != None:
        if action != "get":
            # put aws -p ... < data.yml
            # のパターンが未実装
            raise Exception("TODO")
        data0 = build_path_data(type, profile, path)
    else:
        data0 = load_yaml(src_file)

    # 実装していない出力形式は事前にエラーにする
    if action == "put" and is_dryrun:
        if output_format == "simple":
            raise Exception("TODO")
        elif output_format == "y":
            raise Exception("TODO")
        elif output_format == "yaml":
            pass
    elif action == "put":
        if output_format == "simple":
            raise Exception("TODO")
        elif output_format == "y":
            raise Exception("TODO")
        elif output_format == "yaml":
            pass

    data1 = do_actions(action, data0, repeat_count)

    if action == "put" and not is_dryrun:
        add_update_completion_message()

    if action == "get":
        r1 = data0 # src
        r2 = data1 # クラウド側
    elif action == "put":
        r1 = data1 # 更新前のクラウド側
        r2 = data0 # src

    if output_format == "completion":
        output_completion(get_by_path(data1, path))
    elif action == "get":
        if output_format == "simple":
            if is_diff:
                output_simple_diff(get_by_path(r1, path), get_by_path(r2, path))
            else:
                output_simple(get_by_path(data1, path))
        elif output_format == "y":
            if is_diff:
                diff_yaml(get_by_path(r1, path), get_by_path(r2, path))
            else:
                save_yaml(get_by_path(data1, path), None)
        elif output_format == "yaml":
            if is_diff:
                diff_yaml(r1, r2)
            elif is_inplace:
                save_yaml(data1, src_file)
            else:
                save_yaml(data1, None)
    elif action == "put" and is_dryrun:
        if output_format == "simple":
            raise Exception("TODO")
        elif output_format == "y":
            raise Exception("TODO")
        elif output_format == "yaml":
            if is_diff:
                diff_yaml(r1, r2)
            else:
                save_yaml(r1, None)
    elif action == "put":
        if output_format == "simple":
            raise Exception("TODO")
        elif output_format == "y":
            raise Exception("TODO")
        elif output_format == "yaml":
            if is_diff == True:
                diff_yaml(r1, r2)
            elif is_diff == False:
                save_yaml(r1, None)
            else:
                pass

def check_confirm(confirm):
    now = datetime.datetime.now(datetime.timezone.utc)
    for i in range(3):
        time_str = (now + datetime.timedelta(minutes = i - 1)).isoformat()
        hm = time_str[11:13] + time_str[14:16]
        if hm == confirm:
            return True
    time_str = now.isoformat()
    hm = time_str[11:13] + time_str[14:16]
    raise Exception(f"put action needs --confirm {hm}")

def build_path_data(type, profile, path):
    data = {}
    data0 = {
        "type": type,
        "profile": profile,
        "resources": data,
    }
    for elem in path:
        data1 = {}
        data[elem] = data1
        data = data1
    return data0

def get_by_path(data, path):
    def sub(data):
        if path == None:
            result = data
        else:
            result = data["resources"]
            for elem in path:
                result = result[elem]
        return result
    if isinstance(data, list):
        result = []
        for elem in data:
            result.append(sub(elem))
    else:
        result = sub(data)
    return result

def load_yaml(src_file):
    if src_file:
        with open(src_file) as f:
            data = yaml.safe_load(f)
    elif sys.stdin.isatty():
        raise Exception(f"-s not specified")
    else:
        data = yaml.safe_load(sys.stdin)
    return data

def output_completion(result):
    if result == None:
        pass
    elif isinstance(result, dict):
        for name, value in result.items():
            print(name)
    elif isinstance(result, list):
        for name in result:
            print(name)

def output_simple(result):
    if result == None:
        pass
    elif isinstance(result, dict):
        is_simple = True
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
        print(result)
    elif isinstance(result, int):
        print(result)
    else:
        save_yaml(result, None)

def output_simple_diff(result1, result2):
    if result1 == {} and (isinstance(result2, str) or isinstance(result2, int)):
        result1 = ""
    if (result1 == "" or isinstance(result1, str)) and isinstance(result2, str):
        sic_lib.exec_diff(result1, result2, None)
    elif (result1 == "" or isinstance(result1, int)) and isinstance(result2, int):
        sic_lib.exec_diff(str(result1), str(result2), None)
    else:
        diff_yaml(result1, result2)

def represent_str(dumper, s):
    if "\n" in s:
        return dumper.represent_scalar('tag:yaml.org,2002:str', s, style='|')
    else:
        return dumper.represent_scalar('tag:yaml.org,2002:str', s)

yaml.add_representer(str, represent_str)

def save_yaml(data, dst_file):
    yaml_str = yaml.dump(data, sort_keys = False, allow_unicode = True, width = 120, default_flow_style = False)
    if dst_file:
        with open(dst_file, "w") as f:
            f.write(yaml_str)
    else:
        sys.stdout.write(yaml_str)

def diff_yaml(src_data, dst_data):
    src_yaml_str = yaml.dump(src_data, sort_keys = False, allow_unicode = True, width = 120)
    dst_yaml_str = yaml.dump(dst_data, sort_keys = False, allow_unicode = True, width = 120)
    sic_lib.exec_diff(src_yaml_str, dst_yaml_str, None)

def do_actions(action, data0, repeat_count):
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
    return data1

def do_action(action, src_data):
    global update_message_prefix
    if src_data["type"] == "aws":
        update_message_prefix = sic_aws.get_message_prefix(src_data)
        ret = sic_aws.do_action(action, src_data)
        update_message_prefix = None
        return ret
    else:
        return (src_data, src_data)

update_message_prefix = None
update_message = []

# 更新系APIコールの直前で呼ばれる
def add_update_message(message):
    if update_message_prefix:
        message = f"{update_message_prefix}: {message}"
    else:
        message = f"{message}"
    print(message, file = sys.stderr)
    update_message.append(message)

def add_update_completion_message():
    if len(update_message) > 0:
        message = "complete put action"
        print(message, file = sys.stderr)
