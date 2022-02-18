import copy
import os
import sys
import tempfile

def removeKey(info: dict, key: str):
    if key in info:
        del info[key]

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
