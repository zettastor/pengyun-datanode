# -*- encoding=utf-8 -*-
"""
find all datanode disk then delete partition in it and dd it
"""
#  Copyright (c) 2022. PengYunNetWork
#
#  This program is free software: you can use, redistribute, and/or modify it
#  under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
#  as published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
#   without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
#   You should have received a copy of the GNU Affero General Public License along with
#   this program. If not, see <http://www.gnu.org/licenses/>.

import Common
import argparse
import config_reader
import logging.handlers
import os
import time
import utils.disk
from utils import logging_config

MAX_TRY_COUNT = 10
TRY_INTERVAL = 5

logger = logging_config.get_logger()

# also output log to /tmp, so when wipeout, we can remain some log
formatter = logging.Formatter(
    "%(levelname)s [%(asctime)s] [%(threadName)s] [%(name)s] [%(filename)s:%(funcName)s:%(lineno)s]: %(message)s")
tmp_file_handler = logging.handlers.RotatingFileHandler("/tmp/zero_disk.log",
                                                        "a", 100 * 1024 * 1024,
                                                        3, "utf-8")
tmp_file_handler.setFormatter(formatter)
tmp_file_handler.setLevel(logging.NOTSET)
root = logging.root
root.addHandler(tmp_file_handler)

parser = argparse.ArgumentParser(description="clean up all datanode disks")
parser.add_argument("--dd_size", action="store", type=int, default=16,
                    help="dd size, unit is MB")

args = parser.parse_args()

dd_size = args.dd_size

logger.info("begin to clean up all datanode disks")

# modprobe raw copy from perl script
ret_code, ret_lines = Common.run_cmd("modprobe raw")
if ret_code == 0:
  logger.info("modprobe raw module is loaded to the kernel")
else:
  logger.error("can not modprobe raw module to the kernel")

time.sleep(2)

# umount all datanode file system
for i in range(0, MAX_TRY_COUNT):
  ignored_sub_folders = [config_reader.get_rocksdb_default_folder_name()]
  base_folder_path = config_reader.partition_folder_path

  logger.info("umount file system in datanode folder:[%s] try index:[%s]",
              base_folder_path, i)

  Common.umount_disks_in_folder(base_folder_path, ignored_sub_folders)

  remained_sub_folders = list(
      set(os.listdir(base_folder_path)) - set(ignored_sub_folders))
  if len(remained_sub_folders) > 0:
    logger.error(
        "some folder:[%s] in base folder:[%s] can't umount, retry again",
        remained_sub_folders, base_folder_path)
    time.sleep(TRY_INTERVAL)
  else:
    logger.info("umount file system in datanode folder:[%s] finish.",
                base_folder_path)
    break
else:
  logger.error("umount file system in datanode failed. don't try any more.")

# find all datanode disks
dev_names_without_partition, partition_infos = Common.get_all_origin_disks()

dev_names_without_partition = filter(
    lambda dev_name: Common.check_whether_datanode_disk_by_java(
        Common.get_dev_path_by_dev_name(dev_name)), dev_names_without_partition)

# add partition disk which saved in file
partition_info_from_file = Common.get_partition_record_contents(reload=True)
partition_infos = list(set(partition_infos + partition_info_from_file))

# add dev names in 60 rule and filter out partitioned ones
dev_names_from_60_rule = [content.get_dev_name() for content in
                          Common.get_60_rule_contents(reload=True)]
raw_partition_names = [info.get_raw_disk_partition() for info in
                       partition_infos]
dev_names_without_partition = list(
    set(dev_names_without_partition + dev_names_from_60_rule) - set(
        raw_partition_names))

logger.info(
    "find datanode disks: not partitioned dev_names:[%s], partitioned disk infos:[%s]",
    dev_names_without_partition, partition_infos)

for i in range(0, MAX_TRY_COUNT):
  logger.info("clean disk with partition, disk info:[%s], try index:[%s]",
              partition_infos, i)

  for partition_info in partition_infos[:]:
    dev_name = partition_info.get_dev_name()
    Common.clean_up_disk(dev_name)

    if len(Common.get_partition_paths(
        Common.get_dev_path_by_dev_name(dev_name))) <= 0:
      partition_infos.remove(partition_info)
      print("remove partition and dd disk:[%s]" % dev_name)

  if len(partition_infos) > 0:
    logger.error(
        "clean disk with partition, some disk failed, retry again, remained disk:[%s]",
        partition_infos)
    utils.disk.partprobe()
    time.sleep(TRY_INTERVAL)
  else:
    logger.info("clean disk with partition finish.")
    break
else:
  logger.error("clean disk with partition failed. don't try any more.")

for dev_name in dev_names_without_partition:
  Common.clean_up_disk(dev_name)
  print("dd disk:[%s]" % dev_name)

logger.info("end   to clean up all datanode disks")
