#coding=utf-8
#author@alingse
#2015.11.19

from __future__ import print_function
from copy import deepcopy
from operator import itemgetter

from fs_util import dfpath
from fs_util import get_subdir_host, get_subdirs, get_blocks
from fs_util import safe_mv

#config from hadoop-site.xml/hdfs-site.xml

block_size = 256 * 1024


#disk_meta
#devsd,allb,usedb,availb,precent,mnt,dir
def stat_dir(datapaths):
    tmp = {}
    for datapath in datapaths:
        disk_meta = dfpath(datapath)
        disk_meta.append(datapath)
        devsd = disk_meta[0]
        tmp[devsd] = disk_meta

    disk_metas = tmp.values()
    #sort by available-blocks
    disk_metas = sorted(disk_metas,key=itemgetter(3))
    for disk_meta in disk_metas:
        print(disk_meta)
    return disk_metas


def calculate_disk(disk_metas):
    N = len(disk_metas)
    disk_avails = [disk_meta[3] for disk_meta in disk_metas]

    disk_avail_avg = sum(disk_avails) / N
    disk_avails_diff = [disk_avail - disk_avail_avg
                        for disk_avail in disk_avails]
    disk_block_diff = [disk_diff / block_size
                       for disk_diff in disk_avails_diff]
    #for print
    for i in range(N):
        print(meta[i],disk_block_diff[i])
    return disk_block_diff


def balance_diff(disk_block_diff):
    mv_jobs = []
    start = 0
    end = len(disk_block_diff) - 1
    while start != end:
        s_block = disk_block_diff[start]
        e_block = disk_block_diff[end]
        mv_block = min(abs(s_block), abs(e_block))
        mv_jobs.append([mv_block, start, end])
        disk_block_diff[start] += mv_block
        disk_block_diff[end] -= mv_block
        if disk_block_diff[start] == 0:
            start += 1
        if disk_block_diff[end] == 0:
            end -= 1

    #mv jobs
    for job in mv_jobs:
        print(job)
    print('after balance:',disk_block_diff)
    return mv_jobs


def explain_mv_jobs(disk_metas, mv_jobs):
    mv_details = []
    for job in mv_jobs:
        mv_block, mv_from, mv_to = job
        mv_detail = {}
        mv_detail['mv_from'] = disk_metas[mv_from][6]
        mv_detail['mv_to'] = disk_metas[mv_to][6]
        mv_detail['mv_block'] = mv_block
        mv_details.append(mv_detail)
        print mv_detail
    return mv_details


def do_mv_detail(mv_detail):
    mv_to = mv_detail['mv_to']
    mv_from = mv_detail['mv_from']
    mv_block = mv_detail['mv_block']

    host_to = get_subdir_host(mv_to)
    host_from = get_subdir_host(mv_from)

    subdirs = get_subdirs(host_from)

    for subdir in subdirs:
        if mv_block <= 0:
            break
        block_pairs = get_blocks(host_from, subdir)

        print(mv_from, mv_block, subdir, len(block_pairs), mv_to)

        for block_pair in block_pairs:
            try:
                istrue = safe_mv(host_from, host_to, subdir, block_pair)
                if istrue:
                    mv_block -= 1
            except Exception as e:
                print(e)
                return False
            #break
            if mv_block <= 0:
                break
    return True


if __name__ == '__main__':
    #this should read from hdfs-site.xml
    datapaths = ['/mnt/hdfs/data%s/hadoop_data'.format(i) for i in range(1, 8)]
    
    disk_metas = stat_dir(datapaths)

    disk_block_diff = calculate_disk(disk_metas)

    mv_jobs = balance_diff(disk_block_diff)
    mv_details = explain_mv_jobs(disk_metas, mv_jobs)
    
    for mv_detail in mv_details:
        status = do_mv_detail(mv_detail)
        if status == False:
            break
