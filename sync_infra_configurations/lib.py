import copy
import os
import sys
import tempfile

def execute_elem_properties(action, src_data, describe_fetcher, updator, executor_map):
    res_data1 = copy.deepcopy(src_data)
    res_data2 = copy.deepcopy(src_data)
    if action == "get":
        if src_data == None:
            res_data2 = describe_fetcher()
            for name in executor_map:
                res_data2[name] = None
        else:
            for name in executor_map:
                if name in src_data:
                    res_data1[name], res_data2[name] = executor_map[name](action, src_data[name])
    elif action == "drift":
        if src_data == None:
            pass
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
                    res_data1[name], res_data2[name] = executor_map[name](action, src_data[name])
    elif action == "preview" or action == "update":
        if src_data == None:
            pass
        else:
            src_data2 = copy.deepcopy(src_data)
            for name in executor_map:
                if name in src_data2:
                    del src_data2[name]
            res_data1, res_data2 = updator(src_data2, action == "preview")
            res_data1 = copy.deepcopy(res_data1)
            res_data2 = copy.deepcopy(res_data2)
            for name in executor_map:
                if name in src_data:
                    res_data1[name], res_data2[name] = executor_map[name](action, src_data[name])
    return (res_data1, res_data2)

def execute_elem_items(action, src_data, list_fetcher, executor):
    res_data1 = copy.deepcopy(src_data)
    res_data2 = copy.deepcopy(src_data)
    if action == "get":
        if src_data == None:
            res_data2 = {}
            for name in list_fetcher():
                res_data2[name] = None
        else:
            for name in src_data:
                res_data1[name], res_data2[name] = executor(action, name, src_data[name])
    elif action == "drift":
        if src_data == None:
            pass
        else:
            res_data2 = {}
            for name in list_fetcher():
                if name in src_data:
                    res_data1[name], res_data2[name] = executor(action, name, src_data[name])
                else:
                    pass
                    #res_data2[name] = None
    elif action == "preview" or action == "update":
        if src_data == None:
            pass
        else:
            res_data1 = {}
            res_data2 = {}
            for name in src_data:
                res_data1[name], res_data2[name] = executor(action, name, src_data[name])
    return (res_data1, res_data2)

def null_describe_fetcher():
    return {}

def null_updator(src_data, is_preview):
    return (src_data, src_data)

def null_items_executor(action, src_data):
    return (src_data, src_data)

def null_item_executor(action, name, src_data):
    return (src_data, src_data)

def exec_diff(content1, content2, dst_file, name1 = "1", name2 = "2"):
    sys.stdout.flush()
    sys.stderr.flush()
    tmpdir = tempfile.mkdtemp()
    fifo1 = tmpdir + "/" + name1
    fifo2 = tmpdir + "/" + name2
    os.mkfifo(fifo1)
    os.mkfifo(fifo2)
    pid = os.fork()
    if pid == 0:
        if dst_file != None:
            sys.stdout = open(dst_file, "w")
        os.execvp("diff", ["diff", "-u", fifo1, fifo2])
    pid1 = os.fork()
    if pid1 == 0:
        writer1 = open(fifo1, "w")
        writer1.write(content1)
        writer1.close()
        sys.exit()
    pid2 = os.fork()
    if pid2 == 0:
        writer2 = open(fifo2, "w")
        writer2.write(content2)
        writer2.close()
        sys.exit()
    os.waitpid(pid1, 0)
    os.waitpid(pid2, 0)
    os.waitpid(pid, 0)
    os.remove(fifo1)
    os.remove(fifo2)
    os.rmdir(tmpdir)
