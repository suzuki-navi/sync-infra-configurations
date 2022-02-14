import copy
from re import S
import sys
import yaml

import sync_infra_configurations.aws as sic_aws
import sync_infra_configurations.lib as sic_lib

def main():
    help_flag, action, repeat_count, option_diff, src_file, dst_file = parse_args()
    data0 = load_yaml(src_file)
    if action != "get":
        repeat_count = 1
    data1 = (data0, data0)
    for i in range(repeat_count):
        if isinstance(data1[1], list):
            data2 = ([], [])
            for elem in data1[1]:
                r1, r2 = do_action(action, elem)
                data2[0].append(r1)
                data2[1].append(r2)
        else:
            data2 = do_action(action, data1[1])
        data1 = data2
    if action == "preview" or action == "update":
        r1 = data1[1]
        r2 = data1[0]
    else:
        r1 = data1[0]
        r2 = data1[1]
    if option_diff:
        diff_yaml(r1, r2, dst_file)
    else:
        save_yaml(r2, dst_file)

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
    option_diff = False
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
        elif a.startswith("-"):
            raise Exception(f"Unknown option: {a}")
        elif action == None:
            if a == "get":
                action = a
            elif a == "drift":
                action = a
            elif a == "preview":
                action = a
            elif a == "update":
                action = a
            else:
                raise Exception(f"Unknown action: {a}")
        elif src_file == None:
            src_file = a
        elif dst_file == None:
            dst_file = a
        else:
            raise Exception(f"Unknown parameter: {a}")
    if option_i and src_file != None and (action == "get" or action == "drift"):
        dst_file = src_file
    if dry_run and action == "update":
        action = "preview"
    return (help_flag, action, repeat_count, option_diff, src_file, dst_file)

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

def save_yaml(data, dst_file):

    dumper = yaml.CDumper
    yaml_str = yaml.dump(data, sort_keys = False, allow_unicode = True, width = 120, default_flow_style = False)
    #yaml_str = yaml.safe_dump(data, sort_keys = False, allow_unicode = True, width = 120, default_flow_style = False)
    if dst_file:
        with open(dst_file, "w") as f:
            f.write(yaml_str)
    else:
        sys.stdout.write(yaml_str)

def diff_yaml(src_data, dst_data, dst_file):
    src_yaml_str = yaml.dump(src_data, sort_keys = False, allow_unicode = True, width = 120)
    dst_yaml_str = yaml.dump(dst_data, sort_keys = False, allow_unicode = True, width = 120)
    diff_str = sic_lib.exec_diff(src_yaml_str, dst_yaml_str, dst_file)

def do_action(action, src_data):
    if src_data["type"] == "aws":
        return sic_aws.do_action(action, src_data)
    else:
        return (src_data, src_data)

