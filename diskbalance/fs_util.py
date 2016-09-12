#coding=utf-8
#author@alingse
#2015.11.19

from __future__ import print_function
from subprocess import check_output
import subprocess
#import operator
import sys
import os
import re


def lspath(path):
    out = check_output(['ls',path])
    return out

def lsthings(path):
    out = check_output(['ls',path])
    things = out.rstrip('\n').split('\n')
    return things

def childpath(path):
    things = lsthings(path)
    childs = []
    for thing in things:
        child = os.path.join(path,thing)
        childs.append(child)
    return childs


def get_subdir_host(datapath):
    current = os.path.join(datapath,'current')
    things = lsthings(current)

    for thing in things:
        if 'BP' in things:
            host = os.path.join(current,thing,'current/finalized')
            if os.path.exists(host):
                return host
    err = "there is no hadoop file dir in path: {}".format(datapath)
    raise Exception(err)

def get_subdirs(host, level=2):
    childs = [host]
    for i in range(level):
        childs = reduce(list.__add__,map(childpath,childs))
    subdirs = map(lambda p:p[len(host):],childs)
    return subdirs

def get_blocks(host, subdir):
    path = os.path.join(host,subdir)
    things = lsthings(path)

    blockfiles = set(things)

    block_pairs = []
    for file in blockfiles:
        if 'meta' in file:
            block_meta = file
            block = block_meta[:block_meta.rfind('_')]
            if block in blockfiles:
                block_pairs.append([block, block_meta])

    return block_pairs


def execmd(args):
    child = subprocess.Popen(args,stdout=subprocess.PIPE)
    out,err = child.communicate()
    code = child.returncode
    if code == 0:
        return True,out
    return False,err

def copyfile(file_from, file_to):
    cmds = ['cp',file_from,file_to]
    status,msg = execmd(cmds)
    return status,msg

def rmfile(filepath):
    cmds = ['rm',filepath]
    status,msg = execmd(cmds)
    return status,msg

def safe_mv(host_from, host_to, subdir, block_pair,log=print):
    block, meta = block_pair
    path_from = os.path.join(host_from,subdir)
    path_to = os.path.join(host_to,subdir)

    istrue_block = None
    istrue_meta = None

    needexit = None
    rmdown = None

    try:
        status,msg = execmd(['mkdir','-p',to_path])

        #copy block
        block_from = os.path.join(path_from,block)
        block_to = os.path.join(path_to,block)
        istrue_block,msg = copyfile(block_from,block_to)

        #copy meta
        meta_from = os.path.join(path_from,meta)
        meta_to = os.path.join(path_to,meta)
        istrue_meta,msg = copyfile(meta_from,meta_to)

        #rm the olds
        if istrue_block and istrue_meta:
            #copy sucess && rm the olds
            rmfile(block_from)
            rmfile(meta_from)
            rmdown = True
            log('mv block:',block_from,' sucess')
            return True
    except KeyboardInterrupt, e:
        log('I know you want exit,please wait minutes !!!',istrue_block,istrue_meta)
        needexit = True

    #patch for roll back
    if istrue_block and istrue_meta:
        if not rmdown:
            log('not down the job: rm olds ')
            rmfile(block_from)
            rmfile(meta_from)
    else:
        log('copy interrupted && roll back')
        rmfile(block_to)
        rmfile(meta_to)

    if not needexit:
        return False
    raise Exception("!!sucess exit")

def countblock():
    pass

def dfpath(datapath):
    out = check_output(['df',datapath])
    info = out.split('\n')[1]
    info = re.sub("[ ]+", "\t",info)
    disk_metas = info.split('\t')
    #Capacity /precent 30%
    disk_metas[4] = disk_metas[4].replace("%", "")
    #all---used---available---Capacity
    disk_metas[1:5] = map(int, disk_metas[1:5])
    return disk_metas


if __name__ == '__main__':
    path = sys.argv[2]
    if sys.argv[1] == "df":
        metas = dfpath(path)
        print(metas)
    if sys.argv[1] == "ls":
        out = lspath(path)
        print(out)
    if sys.argv[1] == 'things':
        things = lsthings(path)
        print(things)
    if sys.argv[1] == "host":
        datapath = path
        host = get_subdir_host(datapath)
        print(host)
    if sys.argv[1] == "subdir":
        host = path
        subdirs = get_subdirs(host)
        for s in subdirs:
            print(s)
