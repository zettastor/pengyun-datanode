# -*- encoding=utf-8 -*-
"""
deprecated file
check whether plugin new disk, init and link new disk
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
import common_models
import config_reader
import link_raw_disk
import os
import os_version
from utils import logging_config
from utils.common import exit_process

logger = logging_config.get_logger()

if "datanode" not in os.path.basename(config_reader.project_folder):
  exit_process(-1, "This script must be run from the datanode environment.")

if not os_version.is_support():
  exit_process(-1, "system is not supported")

logger.info("check plugin archive begin")

# check and correct rule file
Common.correct_60_rule_file()

rule_contents = Common.get_60_rule_contents(reload=True)[:]
logger.info("get old device info from rule file, [%s]", rule_contents)

old_serial_nums = [content.get_serial_num() for content in rule_contents]

## special for plugin
old_dev_names = []
for content in rule_contents:
  dev_name = content.get_dev_name()

  # check whether is partitioned name
  matched_partition_infos = filter(
      lambda info: info.get_raw_disk_partition() == dev_name,
      Common.get_partition_record_contents())
  if len(matched_partition_infos) > 0:
    dev_name = matched_partition_infos[0].get_dev_name()

  old_dev_names.append(dev_name)
## special for plugin

# get all disks in current system
all_simple_disk_infos = Common.get_all_origin_disks(
    filtered_dev_names=old_dev_names)

plugin_simple_disk_infos = filter(lambda t: t[0] not in old_serial_nums,
                                  all_simple_disk_infos)
if len(plugin_simple_disk_infos) <= 0:
  logger.info(
      "check plugin archive end, there is no disk plugin, process finished")
  exit(0)

# get existed biggest raw num
last_raw_num = Common.get_max_raw_num()

# full fill device info
dev_infos = []
dev_name_tuples = []
for serial_num, dev_name in plugin_simple_disk_infos:
    dev_name_tuples.append((dev_name,))

dev_type_map = Common.get_dev_type_by_measure_concurrent(dev_name_tuples)

for serial_num, dev_name in plugin_simple_disk_infos:
  last_raw_num += 1
  raw_name = config_reader.raw_disk_base_name + str(last_raw_num)
  dev_type = dev_type_map[dev_name]
  disk_size = Common.get_disk_size(Common.get_dev_path_by_dev_name(dev_name))

  dev_infos.append(
      common_models.DevInfo(dev_name, raw_name, dev_type, disk_size,
                            serial_num))

Common.raw_device_and_append_device_info(dev_infos)
logger.info("find plugin device dev infos:[%s]", dev_infos)

Common.delete_device_by_rollback_file()

raw_names = [dev_info.get_raw_name() for dev_info in dev_infos]

# link all disks
link_raw_disk.link_for_all_disk(dev_infos)

# call java program to init all disks
link_raw_disk.init_all_archives(raw_names)

Common.delete_device_by_rollback_file()

## special for plugin
# print info to notify java program result
msg_pattern = config_reader.get_archive_plugin_matcher()
for folder_path in Common.get_all_disk_folder_paths():
  for link_name in os.listdir(folder_path):
    link_path = os.path.join(folder_path, link_name)
    if os.path.islink(link_path) and os.path.basename(
        os.path.realpath(link_path)) in raw_names:
      sub_folder = os.path.basename(folder_path)
      msg = msg_pattern % (link_name, sub_folder)
      logger.info("notify java program plugin result:[%s]", msg)
      print(msg)
## special for plugin

logger.info("check plugin archive end")
