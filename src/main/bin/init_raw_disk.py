# -*- encoding=utf-8 -*-
"""
find all blank disks in system, and raw them to raw disks
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
import common_models
import config_reader
import link_raw_disk
import os
import os_version
import time
from utils import logging_config
from utils.common import exit_process
from utils.enums import DevType

logger = logging_config.get_logger()

parser = argparse.ArgumentParser(
    description="find disks usable to datanode, and link then init these disks")
parser.add_argument("--only_check_plugin", action="store_true", default=False,
                    help="just check new plugined disks, default will scan all disks")

args = parser.parse_args()

only_check_plugin_disk = args.only_check_plugin

if "datanode" not in os.path.basename(config_reader.project_folder):
  exit_process(-1, "This script must be run from the datanode environment.")

if config_reader.need_check_os_version() and not os_version.is_support():
  exit_process(-1, "system is not supported")

logger.info("begin init raw disk, whether only check plugin disk:[%s]",
            only_check_plugin_disk)

Common.mkdir(config_reader.system_raw_folder)

# check and correct rule file
Common.correct_60_rule_file()

# get existed biggest raw num
last_raw_num = 0
if only_check_plugin_disk:
  last_raw_num = Common.get_max_raw_num()
else:
  last_raw_num = Common.get_max_raw_num(scan_system=False)

Common.force_delete_file(config_reader.rollback_file_path)

if not only_check_plugin_disk:
  Common.force_delete_file(config_reader.ssd_record_file_path)
  Common.force_delete_file(config_reader.partition_record_file_path)

  # unraw all exist raw device
  logger.info("unraw all raw devices from %s", config_reader.system_raw_folder)
  for raw_name in Common.get_raw_names_in_system():
    Common.unraw_raw_disk(raw_name)

  # umount all datanode file system
  logger.info("umount file system in datanode folder:[%s]",
              config_reader.partition_folder_path)
  Common.umount_disks_in_folder(config_reader.partition_folder_path, [
    config_reader.get_rocksdb_default_folder_name()])

  # create necessary folders
  for path in Common.get_all_disk_folder_paths() + [
    config_reader.get_rocksdb_default_folder_path()]:
    Common.mkdir(path)

# get all disks in current system
dev_names_without_partition, partition_infos = [], []
if only_check_plugin_disk:
  rule_contents = Common.get_60_rule_contents()
  partition_name_to_dev_name_map = {
    info.get_raw_disk_partition(): info.get_dev_name() for info in
    Common.get_partition_record_contents()}

  old_dev_names = []
  for content in rule_contents:
    dev_name = content.get_dev_name()

    # check whether is partitioned name
    if dev_name in partition_name_to_dev_name_map:
      partition_name = dev_name
      dev_name = partition_name_to_dev_name_map[partition_name]

    old_dev_names.append(dev_name)

  old_serial_nums = [content.get_serial_num() for content in rule_contents]
  dev_names_without_partition, partition_infos = Common.get_all_origin_disks(
      old_dev_names, old_serial_nums)
else:
  dev_names_without_partition, partition_infos = Common.get_all_origin_disks()

if len(dev_names_without_partition) + len(partition_infos) <= 0:
  logger.info("init raw disk finished, no disk found, process finished")
  exit(0)

# get dev types for disks without partition
dev_type_map = {dev_name: Common.get_dev_type_by_measure(dev_name) for dev_name
                in dev_names_without_partition}

# check whether need partition
if config_reader.need_partition_ssd_disk():
  if Common.need_overwrite_disk():
    logger.info("overwrite disk, clean up all partitioned disks:[%s]",
                partition_infos)
    for partition_info in partition_infos:
      dev_name = partition_info.get_dev_name()
      Common.clean_up_disk(dev_name)

      partition_infos.remove(partition_info)
      dev_names_without_partition.append(dev_name)
      dev_type_map[dev_name] = Common.get_dev_type_by_measure(dev_name)

  else:
    # mount already partitioned disks
    for info in partition_infos[:]:
      dev_path = Common.get_dev_path_by_dev_name(
          info.get_file_system_partition())
      if not Common.mount_into_datanode(dev_path):
        logger.error("can't mount dev_path:[%s], ignore this device", dev_path)
        partition_infos.remove(info)

  # get dev types for disks with partition
  for info in partition_infos:
    dev_name = info.get_dev_name()
    raw_disk_partition_name = info.get_raw_disk_partition()
    dev_type_map[raw_disk_partition_name] = Common.get_dev_type_by_measure(
        dev_name, raw_disk_partition_name)

  # partition, format, mount ssd disks
  for dev_name in dev_names_without_partition[:]:
    dev_type = dev_type_map[dev_name]
    dev_path = Common.get_dev_path_by_dev_name(dev_name)

    if not DevType.is_fast_type(dev_type) or (
        Common.check_whether_datanode_disk_by_java(
            dev_path) and not Common.need_overwrite_disk()):
      continue

    dev_names_without_partition.remove(dev_name)

    # only disks that is ssd|pcie and not formatted by datanode can partition
    partition_info = Common.partition_format_mount(dev_name)
    if partition_info is None:
      logger.error(
          "can't partition dev_type:[%s] dev_name:[%s], ignore this device",
          dev_type, dev_name)
      continue

    partition_infos.append(partition_info)
    logger.info("add one partition info:[%s]", partition_info)

    # insert partition disk dev type and serial num
    raw_disk_partition_name = partition_info.get_raw_disk_partition()
    dev_type_map[raw_disk_partition_name] = dev_type
else:
  logger.info(
      "don't need partition ssd disk, just ignore disks that partitioned by datanode before:[%s]",
      partition_infos)
  del partition_infos[:]

# save partition info
Common.append_partition_record_contents(partition_infos)
Common.get_partition_record_contents(reload=True)

# full fill device info
rule_contents = Common.get_60_rule_contents(reload=True)
all_usable_dev_names = dev_names_without_partition + [
  partition_info.get_raw_disk_partition() for partition_info in partition_infos]
dev_infos = []
for dev_name in all_usable_dev_names:
  serial_num = Common.get_serial_num_for_device(
      Common.get_dev_path_by_dev_name(dev_name))
  dev_type = dev_type_map[dev_name]
  disk_size = Common.get_disk_size(Common.get_dev_path_by_dev_name(dev_name))

  raw_name = None
  matched_rule_contents = filter(
      lambda content: content.get_serial_num() == serial_num, rule_contents)
  if only_check_plugin_disk or len(matched_rule_contents) <= 0:
    last_raw_num += 1
    raw_name = config_reader.raw_disk_base_name + str(last_raw_num)
    logger.info("generate new raw name:[%s] for serial num:[%s] dev name:[%s]",
                raw_name, serial_num, dev_name)
  else:
    raw_name = matched_rule_contents[0].get_raw_name()
    logger.info("get old raw name:[%s] for serial num:[%s] dev name:[%s]",
                raw_name, serial_num, dev_name)

  dev_info = common_models.DevInfo(dev_name, raw_name, dev_type, disk_size,
                                   serial_num)
  dev_infos.append(dev_info)

if not only_check_plugin_disk:
  # delete 60rule file
  Common.force_delete_file(config_reader.six_zero_rule_file_path)
  # recreate all disk folders
  link_raw_disk.recreate_all_disk_folder()

Common.raw_device_and_append_device_info(dev_infos)

# disable disk native cache function
Common.disable_disk_native_cache(dev_infos, only_check_plugin_disk)

dev_infos = Common.filter_out_by_rollback_file(dev_infos)
Common.delete_device_by_rollback_file()

logger.info("datanode find disk, dev infos:[%s]", dev_infos)

# at before, need sleep here for script to recognize new created raw file
time.sleep(2)

# link all disks
link_raw_disk.link_for_all_disk(dev_infos)

# call java program to init all disks
link_raw_disk.init_all_archives(
    [dev_info.get_raw_name() for dev_info in dev_infos], only_check_plugin_disk)

dev_infos = Common.filter_out_by_rollback_file(dev_infos)
Common.delete_device_by_rollback_file()

if only_check_plugin_disk:
  # print info to notify java program result
  msg_pattern = config_reader.get_archive_plugin_matcher()
  link_paths = Common.get_link_paths(
      [dev_info.get_raw_name() for dev_info in dev_infos])
  for link_path in link_paths:
    folder_path = os.path.dirname(link_path)
    link_name = os.path.basename(link_path)
    sub_folder = os.path.basename(folder_path)

    msg = msg_pattern % (link_name, sub_folder)
    logger.info("notify java program plugin result:[%s]", msg)
    print(msg)

logger.info("end   init raw disk, whether only check plugin disk:[%s]",
            only_check_plugin_disk)

exit(0)
