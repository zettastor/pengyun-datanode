# -*- encoding=utf-8 -*-
"""
after datanode recognized all raw disks, this script will decide all disks's usage,
and link disk to datanode folders
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
import config_reader
from utils import logging_config
from utils.enums import DevType

logger = logging_config.get_logger()

def link_for_raw_disk(dev_infos):
  """
  link all devices as rawDisks
  :param dev_infos:
  :return:
  """
  for dev_info in dev_infos:
    logger.info("link disk for app type:[%s], dev info:[%s]",
                Common.AppType.RAW, dev_info)
    Common.link_disk_by_app_types(dev_info.get_raw_name(),
                                  dev_info.get_dev_type(), [Common.AppType.RAW])

  while len(dev_infos) > 0:
    dev_infos.pop()


def link_for_unset_disk(dev_infos):
  """
  check whether auto decide disk usage, if not auto decide, link ssd and pcie disk into unset disk
  :param dev_infos:
  :return:
  """
  if config_reader.get_auto_distribute_disk_usage_switch():
    logger.info(
        "auto distribute disk usage switch is on, don't need link for unset disks")
    return

  for dev_info in dev_infos[:]:
    raw_name = dev_info.get_raw_name()
    dev_type = dev_info.get_dev_type()

    if dev_type == DevType.SSD or dev_type == DevType.PCIE:
      Common.link_disk_by_app_types(raw_name, dev_type,
                                    [Common.AppType.UNSET_DISK])
      dev_infos.remove(dev_info)


def link_for_datanode_formated_disk(dev_infos):
  """
  link for disks which has datanode disk format
  :param dev_infos:
  :return:
  """
  for dev_info in dev_infos[:]:
    if Common.query_and_link_by_java(dev_info.get_raw_name(),
                                     dev_info.get_dev_type()):
      logger.info("link for one datanode formated disk, dev info:[%s]",
                  dev_info)
      dev_infos.remove(dev_info)


def link_for_all_disk(dev_infos):
  """
  link all disks from parameter
  :param dev_infos:
  :return:
  """
  dev_infos = dev_infos[:]

  # link for disks which has datanode disk format
  link_for_datanode_formated_disk(dev_infos)

  # check whether auto decide disk usage
  link_for_unset_disk(dev_infos)

  # link for raw disk
  link_for_raw_disk(dev_infos)


def init_all_archives(raw_names, is_plugin_progress):
  """
  use java program to init all disks
  :param raw_names:
  :param is_plugin_progress: whether is in plugin progress
  :return:
  """
  for sub_folder in Common.get_all_disk_sub_folders():
    Common.init_archives_in_folder(sub_folder, raw_names, is_plugin_progress)


def recreate_all_disk_folder():
  """
  recreate all disk folder in datanode
  :return:
  """
  for path in Common.get_all_disk_folder_paths():
    Common.force_delete_folder(path)
    Common.mkdir(path)

