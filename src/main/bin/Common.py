# -*- encoding=utf-8 -*-
"""
some common subroutine
some common init routine run in last line, please check
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

import common_models
import common_utils
import config_reader
import json
import os
import os_version
import re
import threading
import time
import utils.disk
import uuid
from common_exception import MkfsException
from common_exception import MountException
from common_exception import PartitionException
from common_exception import PartitionSizeTooBig
from common_models import PartitionInfo
from utils import logging_config
from utils.common import dd_disk
from utils.common import exit_process
from utils.common import force_delete_file
from utils.common import force_delete_folder
from utils.common import run_cmd
from utils.common import set_lang_to_english
from utils.disk import get_all_dev_name_in_system
from utils.disk import get_iscsi_virtual_disk
from utils.disk import get_mount_point
from utils.disk import get_partition_paths
from utils.disk import umount_disk
from utils.enums import DevType
from utils.exceptions import JavaReadWriteException

logger = logging_config.get_logger()

# contants
LINK_NAME_SEPARATOR = "_"

six_zero_rule_contents = None
ssd_record_contents = None
partition_record_contents = None


class AppType:
  RAW = config_reader.get_raw_app_name()  # "RAW_DISK"
  UNSET_DISK = "UNSET"


class DiskSubFolder:
  RAW = "rawDisks"
  CLEAN = config_reader.get_clean_archive_directory()


app_type_info_map = {
  AppType.RAW: common_models.AppTypeInfo(app_type=AppType.RAW,
                                         link_name_pattern=config_reader.get_raw_disk_link_name_pattern(),
                                         sub_folder=DiskSubFolder.RAW,
                                         folder_path=os.path.join(
                                             config_reader.storage_folder,
                                             DiskSubFolder.RAW),
                                         min_disk_size_limit=None,
                                         switch=True),

  AppType.UNSET_DISK: common_models.AppTypeInfo(app_type=AppType.UNSET_DISK,
                                                link_name_pattern=config_reader.get_unset_disk_link_name_pattern(),
                                                sub_folder=DiskSubFolder.CLEAN,
                                                folder_path=os.path.join(
                                                    config_reader.storage_folder,
                                                    DiskSubFolder.CLEAN),
                                                min_disk_size_limit=None,
                                                switch=True)
}

# cache serial num, because we need use java program to query old serial num, I don't want to query too much, because
# call java may slow
# key: dev path value: serial num
cached_serial_nums = {}


# parse 60rule file
# ACTION=="add",KERNEL=="sdg",PROGRAM=="/lib/udev/scsi_id -g -u /dev/%k",RESULT=="350014ee65ad68545",RUN+="/bin/raw /dev/raw/raw7 %N"
def parse_60_rule_file(filepath):
  if not os.path.exists(filepath):
    logger.warn("60rule file doesn't exist, please check:[%s]", filepath)
    return []

  logger.info("Begin to parse 60rule path:[%s].", filepath)

  contents = []
  with open(filepath, "r") as f:
    for line in f.readlines():
      dev_name = None
      scsiid = None
      raw_name = None

      matchObj = re.search(r'KERNEL=="(.+?)"', line)
      if matchObj:
        dev_name = matchObj.group(1).strip()

      matchObj = re.search(r'RESULT=="(.+?)"', line)
      if matchObj:
        scsiid = matchObj.group(1).strip()

      matchObj = re.search(r'(raw\d+)', line)
      if matchObj:
        raw_name = matchObj.group(1).strip()

      if dev_name and scsiid and raw_name:
        content = common_models.SixRuleContent(dev_name, scsiid, raw_name)

        logger.info("Parse one line:[%s] from 60rule, result:[%s].",
                    line.strip(), content)
        contents.append(content)
      else:
        logger.warn("Parse one line:[%s] from 60rule failed.", line.strip())

  logger.info("End to parse 60rule path:[%s].", filepath)
  return contents


def get_60_rule_contents(reload=False):
  """
  get 60 rule content, if reload is True, will reread from file, if reload is False, will try to use cached data
  :param reload: whether force read from file
  :return: list of 60 rule content
  """
  global six_zero_rule_contents

  if reload or six_zero_rule_contents is None:
    six_zero_rule_contents = parse_60_rule_file(
        config_reader.six_zero_rule_file_path)

  return six_zero_rule_contents


def get_ssd_record_contents(reload=False):
  """
  get ssd record content, if reload is True, will reread from file, if reload is False, will try to use cached data
  :param reload: whether force read from file
  :return: list of ssd record content
  """
  global ssd_record_contents

  if not reload and ssd_record_contents is not None:
    return ssd_record_contents

  ssd_record_contents = []
  file_path = config_reader.ssd_record_file_path

  if not os.path.exists(file_path):
    logger.warning("file:[%s] doesn't exist, please check.", file_path)
    return []

  with open(file_path, "r") as f:
    for line in f.readlines():
      matchObj = re.match(r"^\s*(\w+)\s*,\s*(\w+)\s*$", line)
      if matchObj:
        raw_name = matchObj.group(1)
        dev_name = matchObj.group(2)
        content = common_models.SSDRecordContent(raw_name, dev_name)
        ssd_record_contents.append(content)
        logger.info(
            "parse one line from ssd.record success, line:[%s] content:[%s]",
            line.strip(), content)
      else:
        logger.info("parse one line from ssd.record failed, line:[%s]",
                    line.strip())

  return ssd_record_contents


# dev_path like /dev/sda
# dev_type is SSD|SATA
# link_path is link file full path like /var/testing/packages/system-datanode/var/storage/rawDisks/ssd2
# sub_dir like cache|rawDisks
def run_java_init_program(serial_num, dev_path, dev_type, link_path,
    run_in_real_time, force_rebuild=False, file_system_partition_name=None):
  """
  run java program to init archive
  :param serial_num:
  :param dev_path: like /dev/sda
  :param dev_type: DevType
  :param link_path: disk link file full path like /var/testing/packages/system-datanode/var/storage/rawDisks/ssd2
  :param run_in_real_time: if datanode is running, this parameter should true
  :param force_rebuild: whether force rebuild archive, default False
  :param file_system_partition_name
  :return:
  """
  sub_dir = os.path.basename(os.path.dirname(link_path))
  logger.info(
      "run java init archive program, parameters: serial_num:%s dev_path:%s dev_type:%s link_path:%s sub_dir:%s force_rebuild:%s file_system_partition_name:[%s]",
      serial_num, dev_path, dev_type, link_path, sub_dir, force_rebuild,
      file_system_partition_name)

  file_system_partition_parameter = ""
  if file_system_partition_name is not None:
    file_system_partition_parameter = "--fileSystemPartitionName " + file_system_partition_name

  cmd = "java -noverify -cp :{project_folder}/build/jar/*:{project_folder}/lib/*:{project_folder}/config " \
        "py.datanode.archive.ArchiveInitializer " \
        "--firstTimeStart {firstTimeStart} " \
        "--serialNumber {serial_num} " \
        "--devName {dev_path} " \
        "--storageType {dev_type} " \
        "--runInRealTime {run_in_real_time} " \
        "--storage {link_path} " \
        "--forceInitBuild {force_rebuild} " \
        "{file_system_partition_parameter} " \
        "--subDir {sub_dir} 2>&1 >> /dev/null ".format(
      firstTimeStart=str(bool(is_datanode_first_time_start())).lower(),
      project_folder=config_reader.project_folder, serial_num=serial_num,
      dev_path=dev_path, dev_type=dev_type,
      run_in_real_time=str(bool(run_in_real_time)).lower(),
      link_path=link_path, force_rebuild=str(bool(force_rebuild)).lower(),
      file_system_partition_parameter=file_system_partition_parameter,
      sub_dir=sub_dir)

  return run_cmd(cmd)


def get_dev_type_by_measure(dev_name, partition_name=None):
  """
  get dev type by measure: ssd pattern and pcie pattern from datanode config, disk speed test by java program
  :param dev_name:
  :param partition_name: if pass this parameter, ssd speed test will test on partition; if not pass, speed test will test on dev name
  :return:
  """
  if is_pcie_according_to_dev_name_pattern(dev_name):
    dev_type = DevType.PCIE
    logger.info("get dev type:[%s] for dev name:[%s] partition name:[%s]",
                dev_type, dev_name, partition_name)
    return dev_type

  speed_test_dev_name = dev_name
  if partition_name is not None:
    speed_test_dev_name = partition_name

  if is_ssd_according_to_dev_name_pattern(
      dev_name) or is_ssd_according_speed_test(speed_test_dev_name):
    dev_type = DevType.SSD
    logger.info("get dev type:[%s] for dev name:[%s] partition name:[%s]",
                dev_type, dev_name, partition_name)
    return dev_type

  dev_type = DevType.SATA
  logger.info("get dev type:[%s] for dev name:[%s] partition name:[%s]",
              dev_type, dev_name, partition_name)
  return dev_type


def is_ssd_according_to_dev_name_pattern(dev_name):
  """
  check whether disk is ssd by ssd pattern from datanode config
  :param dev_name: like sda
  :return: bool
  """
  name_patterns = config_reader.get_ssd_reg_array()
  return utils.common.check_whether_ssd_by_name_pattern(dev_name, name_patterns)


def is_ssd_according_speed_test(dev_name):
  """
  check whether disk is ssd by speed test
  :param dev_name: like sda
  :return: bool
  """
  if not config_reader.need_detect_ssd_by_speed():
    logger.info(
        "don't check ssd by speed test for dev name:[%s], because switch is off",
        dev_name)
    return False

  dev_path = get_dev_path_by_dev_name(dev_name)
  java_classpath = config_reader.java_classpath
  class_name = "py.datanode.StorageDetect"
  threshold = config_reader.get_ssd_min_threshold()

  try:
    return utils.common.check_whether_ssd_by_speed_test(dev_path,
                                                        java_classpath,
                                                        class_name, threshold)
  except JavaReadWriteException:
    logger.error(
        "check ssd by speed for dev name:[%s] failed, save rollback file",
        dev_name)
    save_rollback_file(dev_name)
    return False


def get_dev_type_from_datanode(raw_name):
  """
  get dev type by from datanode, check if ssd by ssd.record, check if pcie by pcie name pattern in datanode.properties
  :param raw_name: like raw1
  :return: enum DevType, like ssd|pcie|sata
  """
  matched_ssd_records = filter(lambda record: record.get_raw_name() == raw_name,
                               get_ssd_record_contents())
  if len(matched_ssd_records) <= 0:
    logger.info("raw name:[%s] is sata disk.", raw_name)
    return DevType.SATA

  dev_name = matched_ssd_records[0].get_dev_name()
  logger.info(
      "map raw name:[%s] to dev name:[%s] by ssd.record file, this is a ssd disk",
      raw_name, dev_name)

  dev_name_for_pattern = dev_name
  matched_partition_infos = filter(
      lambda info: info.get_raw_disk_partition() == dev_name,
      get_partition_record_contents())
  if len(matched_partition_infos) > 0:
    dev_name_for_pattern = matched_partition_infos[0].get_dev_name()

  # find dev name from ssd.record, now this disk at least is ssd
  if is_pcie_according_to_dev_name_pattern(dev_name_for_pattern):
    logger.info("raw name:[%s] is pcie according to name pattern", raw_name)
    return DevType.PCIE
  else:
    logger.info("raw name:[%s] is ssd", raw_name)
    return DevType.SSD


def is_pcie_according_to_dev_name_pattern(dev_name):
  """
  check whether disk is pcie, by name pattern from datanode config
  :param dev_name: like sda
  :return: bool
  """
  patterns = config_reader.get_pcie_regs()
  return utils.common.check_whether_pcie_by_name_pattern(dev_name, patterns)


def generate_link_name(raw_name, dev_type, app_types):
  logger.info(
      "Generate link name by raw_name:[%s] dev_type:[%s] app_types:[%s]",
      raw_name, dev_type, app_types)

  raw_num = None
  match_obj = re.search(r"(\d+)$", raw_name)
  if match_obj:
    raw_num = match_obj.group(1)
  else:
    logger.error(
        "raw_name:[%s] does not have number in it, can't generate link name.",
        raw_name)
    return None

  link_pattern = LINK_NAME_SEPARATOR.join(
      filter(lambda x: bool(x),
             [get_link_pattern_by_app_type(app_type) for app_type in
              app_types]))

  link_name_postfix = dev_type + raw_num
  link_name = None
  if link_pattern:
    link_name = LINK_NAME_SEPARATOR.join([link_pattern, link_name_postfix])
  else:
    link_name = link_name_postfix

  logger.info(
      "Generate link name:[%s] by raw_name:[%s] dev_type:[%s] app_types:[%s]",
      link_name, raw_name, dev_type, app_types)

  return link_name


def get_dev_path_by_dev_name(dev_name):
  return os.path.join(config_reader.system_dev_folder, dev_name)


def get_dev_name_by_dev_path(dev_path):
  return os.path.relpath(dev_path, config_reader.system_dev_folder)


def get_raw_path_by_raw_name(raw_name):
  return os.path.join(config_reader.system_raw_folder, raw_name)


def get_mount_path_by_partition_name(partition_name):
  return os.path.join(config_reader.partition_folder_path, partition_name)


def get_app_type_info(app_type):
  if app_type in app_type_info_map:
    return app_type_info_map[app_type]
  else:
    logger.error(
        "can't get app type info:[%s], this should not happen, please check.",
        app_type)
    raise RuntimeError(
        "can't get link pattern for app type:[%s], this should not happen, please check." % app_type)


def get_link_pattern_by_app_type(app_type):
  return get_app_type_info(app_type).get_link_name_pattern()


def get_sub_folder_by_app_type(app_type):
  return get_app_type_info(app_type).get_sub_folder()


def get_folder_path_by_app_type(app_type):
  return get_app_type_info(app_type).get_folder_path()


def get_disk_size_limit_by_app_type(app_type):
  return get_app_type_info(app_type).get_min_disk_size_limit()


def get_switch_by_app_type(app_type):
  return get_app_type_info(app_type).get_switch()


# get disk size by blockdev
# dev_path like /dev/raw/raw1 or /dev/sda
# return -1 if fail
def get_disk_size_by_blockdev(dev_path):
  cmd = "blockdev --getsize64 {dev_path}".format(dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)

  if ret_code == 0:
    disk_size = int(ret_lines[0].strip())
    logger.info("get disk size:[%s] for dev_path:[%s] by blockdev success",
                disk_size, dev_path)
    return disk_size
  else:
    logger.info("get disk size for dev_path:[%s] by blockdev failed", dev_path)
    return -1


# get disk size by lsblk
# dev_path like /dev/sda
# return -1 if fail
def get_disk_size_by_lsblk(dev_path):
  cmd = "lsblk -i -b -n -p -o size {dev_path}".format(dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)

  if ret_code == 0:
    disk_size = int(ret_lines[0].strip())
    logger.info("get disk size:[%s] for dev_path:[%s] by lsblk success",
                disk_size, dev_path)
    return disk_size
  else:
    logger.info("get disk size for dev_path:[%s] by lsblk failed", dev_path)
    return -1


# map raw path like /dev/raw/raw1 to dev path like /dev/sda
# return None if failed
def map_raw_path_to_dev_path(raw_path):
  """
  map raw path like /dev/raw/raw1 to dev path like /dev/sda, first map by 60-rule, second map by cmd lsblk and raw
  :param raw_path: like /dev/raw/raw1
  :return: dev path like /dev/sda, or None if failed
  """
  if os.path.dirname(raw_path) != config_reader.system_raw_folder:
    logger.info("path:[%s] is not raw path", raw_path)
    return None

  for rule_content in get_60_rule_contents():
    if rule_content.get_raw_name() == os.path.basename(raw_path):
      dev_path = get_dev_path_by_dev_name(rule_content.get_dev_name())
      logger.info("map raw path:[%s] to dev path:[%s] success, by 60-rule file",
                  raw_path, dev_path)
      return dev_path

  # get major and minor number
  # raw -q /dev/raw/raw1
  # /dev/raw/raw1:  bound to major 8, minor 0
  raw_query_cmd = "{raw_cmd_path} -q {raw_path}".format(
      raw_cmd_path=get_raw_cmd_path_by_os(), raw_path=raw_path)
  ret_code, ret_lines = run_cmd(raw_query_cmd)
  if ret_code != 0 or len(ret_lines) == 0:
    logger.error(
        "query raw info failed for raw path:[%s], map raw path to dev path failed.",
        raw_path)
    return None

  match_obj = re.search(r"major\s*(\d+)\s*,\s*minor\s*(\d+)")
  if not match_obj:
    logger.error(
        "query raw info failed for raw path:[%s], map raw path to dev path failed.",
        raw_path)
    return None

  major_num = match_obj.group(1)
  minor_num = match_obj.group(2)
  logger.debug("Find major num:[%s] minor num:[%s] for raw path:[%s]",
               major_num, minor_num, raw_path)

  # lsblk -i -b -n -p -P -o name,maj:min
  # NAME="/dev/sda" MAJ:MIN="8:0"
  lsblk_majmin_cmd = "lsblk -i -b -n -p -P -o name,maj:min"
  ret_code, ret_lines = run_cmd(lsblk_majmin_cmd)
  if ret_code != 0:
    logger.error(
        "query dev info by lsblk failed, map raw path:[%s] to dev path failed.",
        raw_path)
    return None

  pattern = r'NAME="(.+)"\s*MAJ:MIN="{major_num}:{minor_num}"'.format(
      major_num=major_num, minor_num=minor_num)
  for line in ret_lines:
    match_obj = re.search(pattern, line, re.I)
    if match_obj:
      dev_path = match_obj.group(1)
      logger.info(
          "map raw path:[%s] to dev path:[%s] success, by system cmd lsblk and raw",
          raw_path, dev_path)
      return dev_path

  else:
    logger.error("map raw path:[%s] to dev path failed", raw_path)
    return None


def map_dev_name_to_raw_name(dev_name):
  """
  map dev name to raw name, first by 60-rule file, second by cmd lsblk and raw
  :param dev_name: like sda
  :return: raw_name like raw1, None if failed
  """
  for rule_content in get_60_rule_contents():
    if rule_content.get_dev_name() == dev_name:
      raw_name = rule_content.get_raw_name()
      logger.info("map dev name:[%s] to raw name:[%s] success, by 60-rule file",
                  dev_name, raw_name)
      return raw_name

  # get major and minor number
  # lsblk -i -b -n -p -o maj:min /dev/sda
  #     8:0
  lsblk_query_cmd = "lsblk -i -b -n -p -o maj:min {dev_path}".format(
      dev_path=get_dev_path_by_dev_name(dev_name))
  ret_code, ret_lines = run_cmd(lsblk_query_cmd)
  if ret_code != 0 or len(ret_lines) != 1:
    logger.error(
        "query dev info failed for dev name:[%s], map dev name to raw name failed.",
        dev_name)
    return None

  match_obj = re.search(r"\b(\d+):(\d+)\b")
  if not match_obj:
    logger.error(
        "query dev info failed for dev name:[%s], map dev name to raw name failed.",
        dev_name)
    return None

  major_num = match_obj.group(1)
  minor_num = match_obj.group(2)
  logger.debug("find major num:[%s] minor num:[%s] for dev name:[%s]",
               major_num, minor_num, dev_name)

  # raw -qa
  # /dev/raw/raw1:  bound to major 8, minor 0
  raw_query_cmd = "{raw_cmd_path} -qa".format(
      raw_cmd_path=get_raw_cmd_path_by_os())
  ret_code, ret_lines = run_cmd(raw_query_cmd)
  if ret_code != 0:
    logger.error(
        "query dev info by raw failed, map dev name:[%s] to raw name failed.",
        dev_name)
    return None

  pattern = r"(raw\d+):\s*bound\s*to\s*major\s*{major_num},\s*minor\s*{minor_num}".format(
      major_num=major_num,
      minor_num=minor_num)
  for line in ret_lines:
    match_obj = re.search(pattern, line, re.I)
    if match_obj:
      raw_name = match_obj.group(1)
      logger.info(
          "map dev name:[%s] to raw name:[%s] success, by lsblk and raw cmd",
          dev_name, raw_name)
      return raw_name

  else:
    logger.error("map dev name:[%s] to raw name failed", dev_name)
    return None


# get disk size in bytes
# return -1 if fail
def get_disk_size(dev_path):
  """
  get disk size
  :param dev_path: support both /dev/raw1 and /dev/sda
  :return: disk size in bytes or -1 if fail
  """
  disk_size = get_disk_size_by_blockdev(dev_path)

  if disk_size >= 0:
    logger.info("get disk size:[%s] for dev_path:[%s]", disk_size, dev_path)
    return disk_size

  dev_name_path = None
  if os.path.dirname(dev_path) == config_reader.system_dev_folder:
    logger.info("dev path:[%s] is dev name path", dev_path)
    dev_name_path = dev_path
  elif os.path.dirname(dev_path) == config_reader.system_raw_folder:
    logger.info("dev path:[%s] is raw device path", dev_path)
    raw_path = dev_path
    dev_name_path = map_raw_path_to_dev_path(raw_path)

  if not dev_name_path:
    logger.error(
        "can't get dev name path for dev path:[%s], get disk size failed",
        dev_path)
    return -1

  disk_size = get_disk_size_by_lsblk(dev_name_path)
  if disk_size < 0:
    logger.error("can't get disk size for dev_path:[%s]", dev_path)
    return -1

  logger.info("get disk size:[%s] for dev_path:[%s] success", disk_size,
              dev_path)
  return disk_size


def check_exist_link_file(app_types):
  logger.info("check whether exist link file for app_types:[%s]", app_types)
  for app_type in app_types:
    folder_path = get_folder_path_by_app_type(app_type)
    link_name_pattern = get_link_pattern_by_app_type(app_type)

    if not link_name_pattern:
      logger.error(
          "link pattern for app_type:[%s] is empty, can't check whether exist link file",
          app_type)
      return False

    for filename in os.listdir(folder_path):
      if link_name_pattern in filename:
        logger.info(
            "find exist link file:[%s] in folder:[%s] for app_type:[%s]",
            filename, os.path.basename(folder_path), app_type)
        return True

  logger.info("there is no exist link file for app_types:[%s]", app_types)
  return False


def get_exist_link_count_by_app_type(app_type):
  """
  get exist link file count by app type
  :param app_type:
  :return: count int
  """
  return len(get_exist_link_by_app_type(app_type))


def get_exist_link_by_app_type(app_type):
  """
  get exist link file by app type
  :param app_type:
  :return: list of link path
  """
  folder_path = get_folder_path_by_app_type(app_type)
  link_name_pattern = get_link_pattern_by_app_type(app_type)

  if not link_name_pattern:
    logger.error(
        "can't get link pattern for app type:[%s], can't get exist link file ",
        app_type)
    return []

  exist_link_names = filter(lambda name: link_name_pattern in name,
                            os.listdir(folder_path))
  logger.info("find exist link files:[%s] in folder:[%s] for app type:[%s]",
              exist_link_names,
              os.path.basename(folder_path), app_type)
  return [os.path.join(folder_path, link_name) for link_name in
          exist_link_names]


def get_sub_folder_by_app_types(app_types):
  logger.info("get sub folder for app types:[%s]", app_types)

  if AppType.RAW in app_types:
    sub_folder = get_sub_folder_by_app_type(AppType.RAW)
    logger.info(
        "there is raw app type:[%s] in app types:[%s], sub folder is [%s]",
        AppType.RAW, app_types, sub_folder)
    return sub_folder

  if len(app_types) == 1:
    app_type = app_types[0]
    sub_folder = get_sub_folder_by_app_type(app_type)
    logger.info(
        "get sub folder for app type:[%s] in app types:[%s], sub folder is [%s]",
        app_type, app_types, sub_folder)
    return sub_folder
  return None


def get_all_disk_sub_folders():
  return [DiskSubFolder.RAW, DiskSubFolder.CLEAN]


def get_all_disk_folder_paths():
  return [os.path.join(config_reader.storage_folder, sub_folder) for sub_folder
          in get_all_disk_sub_folders()]


def get_raw_names_in_system():
  """
  get all raw names in /dev/raw, raw name like raw1
  :return: raw name list
  """

  raw_names = []
  for name in os.listdir(config_reader.system_raw_folder):
    if re.match(r"raw\d+", name):
      raw_names.append(name)

  logger.info("get all raw names:[%s] in system", raw_names)
  return raw_names


def is_archive_init_mode_overwrite():
  return config_reader.get_archive_init_mode().strip() == "overwrite"


def get_dev_infos_by_raw_names(raw_names):
  """
  get all dev infos by raw names, get info from datanode, for example, get dev type by ssd.record, get dev name by 60-rule
  :param raw_names: list like [raw1, raw2]
  :return: list of DevInfo
  """
  logger.info("get dev infos by raw_names:[%s]", raw_names)
  dev_infos = []
  for raw_name in raw_names:
    raw_path = get_raw_path_by_raw_name(raw_names)
    dev_type = get_dev_type_from_datanode(raw_name)
    dev_path = map_raw_path_to_dev_path(get_raw_path_by_raw_name(raw_names))
    disk_size = get_disk_size(raw_path)
    rule_contents = filter(lambda content: content.get_raw_name() == raw_name,
                           get_60_rule_contents())

    if len(rule_contents) == 1 and dev_path is not None and disk_size > 0:
      serial_num = rule_contents[0].get_serial_num()
      dev_info = common_models.DevInfo(dev_name=os.path.basename(dev_path),
                                       raw_name=raw_name, dev_type=dev_type,
                                       disk_size=disk_size,
                                       serial_num=serial_num)
      logger.info("get dev info:[%s] by raw name:[%s]", dev_info, raw_name)
      dev_infos.append(dev_info)
    else:
      logger.error("get dev info by raw name:[%s] failed", raw_name)

  logger.info("get dev infos:[%s] by raw names:[%s]", dev_infos, raw_names)
  return dev_infos


def is_datanode_first_time_start():
  """
  if DataNodeStartTime file not exist means datanode is start for the first time
  :return: True or False
  """
  return not os.path.exists(config_reader.datanode_start_time_file_path)


def need_overwrite_disk():
  """
  whether need overwrite disk
  :return: bool
  """
  if is_archive_init_mode_overwrite() and is_datanode_first_time_start():
    return True
  else:
    return False


def query_and_link_by_java(raw_name, dev_type):
  """
  run java program to analyse disk format, if has formatted by datanode, java program will direct link this disk to datanode disk folder
  :param raw_name: like raw1
  :param dev_type: DevType
  :return: True if has datanode disk format
  """
  if need_overwrite_disk():
    logger.info(
        "archive init mode is overwrite and datanode is first time start, "
        "don't query and link by java program for raw name:[%s]", raw_name)
    return False

  raw_path = get_raw_path_by_raw_name(raw_name)
  cmd = "java -noverify -cp {classpath} " \
        "py.datanode.ArchiveTypeQueryAndLink " \
        "--storagePath {raw_path} " \
        "--storageType {dev_type}".format(
      classpath=config_reader.java_classpath, raw_path=raw_path,
      dev_type=dev_type)

  ret_code, ret_lines = run_cmd(cmd)

  if int(ret_code) == 200:
    logger.info("run java program to query and link for raw name:[%s] success, "
                "ret code:[%s](200:format OK, 201:format unknown)", raw_name,
                ret_code)
    return True
  else:
    logger.info("run java program to query and link for raw name:[%s] failed, "
                "ret code:[%s](200:format OK, 201:format unknown)", raw_name,
                ret_code)
    return False


def check_whether_datanode_disk_by_java(dev_path):
  """
  run java program to check whether this is datanode disk
  :param dev_path: like /dev/sda or /dev/sda1
  :return: bool
  """
  cmd = "java -noverify -cp {classpath} " \
        "py.datanode.ArchiveTypeQueryAndLink " \
        "--storagePath {dev_path} " \
        "--needLink false".format(classpath=config_reader.java_classpath,
                                  dev_path=dev_path)

  ret_code, ret_lines = run_cmd(cmd)

  if int(ret_code) == 200:
    logger.info("dev_path:[%s] is formatted by datanode", dev_path)
    return True
  else:
    logger.info("dev_path:[%s] is not formatted by datanode", dev_path)
    return False


def link_disk_by_app_types(raw_name, dev_type, app_type_strs):
  """
  link disk by determined app types
  :param raw_name: like raw1
  :param dev_type:
  :param app_type_strs:
  :return:
  """
  link_name = generate_link_name(raw_name, dev_type, app_type_strs)
  sub_folder = get_sub_folder_by_app_types(app_type_strs)
  link_file_path = os.path.join(config_reader.storage_folder, sub_folder,
                                link_name)

  if os.path.exists(link_file_path):
    os.remove(link_file_path)
    logger.warn("delete link file:[%s]", link_file_path)

  raw_path = get_raw_path_by_raw_name(raw_name)
  os.symlink(raw_path, link_file_path)
  logger.info(
      "create soft link file, origin:[%s] link file:[%s] raw name:[%s] dev_type:[%s] app types:[%s]",
      raw_path, link_file_path, raw_name, dev_type, app_type_strs)


def init_archives_in_folder(sub_folder, raw_names, is_plugin_progress):
  """
  use java program to init archive, init disks in the sub folder which link to specific raw disks
  :param sub_folder
  :param raw_names only there raw device need init
  :param is_plugin_progress: whether is in plugin progress
  :return:
  """
  folder_path = os.path.join(config_reader.storage_folder, sub_folder)
  link_names = filter(
      lambda name: os.path.islink(os.path.join(folder_path, name)),
      os.listdir(folder_path))
  logger.info(
      "use java program to init archive, in sub folder:[%s] link names:[%s]",
      sub_folder, link_names)

  start_time = time.time()
  threads = []
  for link_name in link_names:
    link_file_path = os.path.join(folder_path, link_name)
    raw_name = os.path.basename(os.path.realpath(link_file_path))

    if raw_name not in raw_names:
      # this device don't need init
      continue

    dev_type = get_dev_type_from_datanode(raw_name)

    rule_contents = filter(lambda content: content.get_raw_name() == raw_name,
                           get_60_rule_contents())
    if len(rule_contents) != 1:
      logger.error(
          "can't get dev name and serial number by raw name:[%s] from 60rule file,"
          "init archive for link file:[%s] failed", raw_name, link_name)
      continue

    dev_name = rule_contents[0].get_dev_name()
    serial_num = rule_contents[0].get_serial_num()

    # if it is partitioned disk, should use the origin dev_name
    file_system_partition_name = None
    partition_infos = filter(
        lambda info: info.get_raw_disk_partition() == dev_name,
        get_partition_record_contents())
    if len(partition_infos) > 0:
      dev_name = partition_infos[0].get_dev_name()
      file_system_partition_name = partition_infos[
        0].get_file_system_partition()

    dev_path = get_dev_path_by_dev_name(dev_name)

    logger.info("ArchiveInit: link name:[%s] raw name:[%s]", link_name,
                raw_name)
    run_in_real_time = is_plugin_progress
    t = threading.Thread(target=run_java_init_program, args=(serial_num,
                                                             dev_path,
                                                             dev_type,
                                                             link_file_path,
                                                             run_in_real_time,
                                                             False,
                                                             file_system_partition_name
                                                             ))
    t.start()
    threads.append(t)

  for t in threads:
    t.join()
  end_time = time.time()
  logger.info("init archive cost [%d] seconds", end_time - start_time)


def pick_fastest_and_bigest_disk(dev_infos):
  """
  pick one fatest and biggest disk
  :param dev_infos: list of common_models.DevInfo
  :return: common_models.DevInfo or None if no disk is pcie or ssd
  """
  fast_dev_types = DevType.get_fast_types()

  for dev_type in fast_dev_types:
    dev_infos_with_special_dev_type = filter(
        lambda dev_info: dev_info.get_dev_type() == dev_type, dev_infos)
    if len(dev_infos_with_special_dev_type) > 0:
      fastest_and_biggest_disk = max(dev_infos_with_special_dev_type,
                                     key=common_models.DevInfo.get_disk_size)
      logger.info("select fastest and biggest disk:[%s] from dev infos:[%s]",
                  fastest_and_biggest_disk, dev_infos)
      return fastest_and_biggest_disk

  logger.info("can't select fastest and biggest disk in dev infos:[%s]",
              dev_infos)
  return None


def pick_fastest_and_smallest_disk(dev_infos):
  """
  pick one fastest and smallest disk
  :param dev_infos: list of common_models.DevInfo
  :return: common_models.DevInfo or None if no disk is pcie or ssd
  """
  fast_dev_types = DevType.get_fast_types()

  for dev_type in fast_dev_types:
    dev_infos_with_special_dev_type = filter(
        lambda dev_info: dev_info.get_dev_type() == dev_type, dev_infos)
    if len(dev_infos_with_special_dev_type) > 0:
      fastest_and_smallest_disk = min(dev_infos_with_special_dev_type,
                                      key=common_models.DevInfo.get_disk_size)
      logger.info("select fastest and smallest disk:[%s] from dev infos:[%s]",
                  fastest_and_smallest_disk,
                  dev_infos)
      return fastest_and_smallest_disk

  logger.info("can't select fastest and smallest disk in dev infos:[%s]",
              dev_infos)
  return None


def save_rollback_file(dev_name):
  """
  save dev name to rollback file
  :param dev_name: like sda
  :return:
  """
  with open(config_reader.rollback_file_path, "a") as f:
    f.write(dev_name + "\n")

  logger.info("save dev name:[%s] to rollback file", dev_name)


def get_rollback_file_content():
  """
  get content in rollback file
  :return: list of dev_name
  """
  if not os.path.exists(config_reader.rollback_file_path):
    return []

  with open(config_reader.rollback_file_path, "r") as f:
    dev_names = [line.strip() for line in f.readlines()]
    return dev_names


def delete_device_by_rollback_file():
  """
  delete device by rollback file, these devices are init failed
  :return:
  """
  logger.info("Try to rollback init failed disks.")

  if not os.path.exists(config_reader.rollback_file_path):
    logger.info("rollback file:[%s] does not exist, don't need rollback",
                config_reader.rollback_file_path)
    return

  dev_names = get_rollback_file_content()
  raw_names = [map_dev_name_to_raw_name(dev_name) for dev_name in dev_names]
  raw_names = filter(lambda raw_name: raw_name is not None, raw_names)

  unlink_disk_in_datanode(raw_names)
  remove_disk_info_from_datanode_info_file(raw_names, dev_names)

  for raw_name in raw_names:
    unraw_raw_disk(raw_name)

  # deals with partition disk, umount disk, if it does not has datanode archive info means this is new partitioned disk,
  # need to to clean partition
  removed_partition_infos = remove_partition_info_and_umount(dev_names)
  for partition_info in removed_partition_infos:
    dev_name = partition_info.get_dev_name()
    raw_disk_partition_name = partition_info.get_raw_disk_partition()

    if not check_whether_datanode_disk_by_java(
        get_dev_path_by_dev_name(raw_disk_partition_name)):
      logger.warn(
          "dev name:[%s] is new partitioned by datanode, by something bad happend,"
          "this disk can't use, so we need clear its partition info", dev_name)
      common_utils.clean_partition(get_dev_path_by_dev_name(dev_name))

  force_delete_file(config_reader.rollback_file_path)
  logger.info("rollback init failed disks finished.")


def unlink_disk_in_datanode(raw_names):
  """
  remove link file in datanode which link to specific raw names
  :param raw_names: like raw1
  :return:
  """
  logger.info("delete link file by raw names:[%s]", raw_names)

  for link_file_path in get_link_paths(raw_names):
    raw_name = os.path.basename(os.path.realpath(link_file_path))
    logger.info("unlink disk, link file:[%s] raw name:[%s]", link_file_path,
                raw_name)

    force_delete_file(link_file_path)


def remove_line_if_contain_word(word, file_path):
  """
  modify one file, delete line if this line contain specific word
  :param word: word to be search
  :param file_path:
  :return:
  """
  if not os.path.exists(file_path):
    logger.error(
        "try to remove line in file:[%s] if contain word:[%s], but this file does not exist",
        file_path, word)
    return

  origin_lines = []
  with open(file_path, "r") as f:
    origin_lines = f.readlines()

  pattern = r"\b{word}\b".format(word=word)
  new_lines = filter(lambda line: not re.search(pattern, line), origin_lines)

  if len(new_lines) == len(origin_lines):
    logger.info(
        "try to remove line in file:[%s] if contain word:[%s], but after search, no line removed, "
        "origin lines:[%s] after filtered lines:[%s]", file_path, word,
        origin_lines, new_lines)
    return

  logger.info(
      "try to remove line in file:[%s] if contain word:[%s], remove line:[%s]",
      file_path, word, list(set(origin_lines) - set(new_lines)))
  with open(file_path, "w") as f:
    f.writelines(new_lines)


def remove_partition_info_and_umount(dev_names):
  """
  remove partition info, and umount disk
  :param dev_names: maybe dev name like sda or partition name like sda1
  :return: removed partition infos
  """
  origin_partition_infos = get_partition_record_contents(reload=True)
  remained_partition_infos = []
  removed_partition_infos = []

  for partition_info in origin_partition_infos:
    dev_name = partition_info.get_dev_name()
    raw_disk_partition_name = partition_info.get_raw_disk_partition()
    file_system_partition_name = partition_info.get_file_system_partition()

    if dev_name in dev_names or raw_disk_partition_name in dev_names:
      # need remove and umount disk
      mount_path = get_mount_path_by_partition_name(file_system_partition_name)
      umount_disk(mount_path)
      force_delete_folder(mount_path)
      removed_partition_infos.append(partition_info)
      logger.info("remove partition info:[%s] and umount file system",
                  partition_info)

    else:
      remained_partition_infos.append(partition_info)

  if len(remained_partition_infos) != len(origin_partition_infos):
    force_delete_file(config_reader.partition_record_file_path)
    append_partition_record_contents(remained_partition_infos)

  return removed_partition_infos


def raw_device_and_append_device_info(dev_infos):
  """
  raw raw device, and save rule file and ssd.record
  :param dev_infos: list of common_modesl.DevInfo
  :return:
  """
  # raw device and save disk info
  for dev_info in dev_infos:
    dev_name = dev_info.get_dev_name()
    serial_num = dev_info.get_serial_num()
    raw_name = dev_info.get_raw_name()
    dev_type = dev_info.get_dev_type()

    # raw disk
    raw_raw_disk(raw_name, dev_name)

    # save rule file
    append_six_rule_file(raw_name, serial_num, dev_name)

    # save ssd.record
    if dev_type == DevType.SSD or dev_type == DevType.PCIE:
      append_ssd_record(raw_name, dev_name)

  # reload record file
  get_60_rule_contents(reload=True)
  get_ssd_record_contents(reload=True)

  logger.info("raw raw device, and save disk info, dev infos:[%s]", dev_infos)


def remove_disk_info_from_datanode_info_file(raw_names, dev_names=[]):
  """
  delete disk info from 60rule and ssd.record
  :param raw_names:
  :param dev_names:
  :return:
  """
  info_file_paths = [config_reader.six_zero_rule_file_path,
                     config_reader.ssd_record_file_path]
  words = raw_names + dev_names
  for word in words:
    logger.info(
        "remove the record by word:[%s] from ssd record and 60rule file", word)
    for info_file_path in info_file_paths:
      remove_line_if_contain_word(word=word, file_path=info_file_path)


def correct_60_rule_file():
  """
  check 60 rule file, delete malformed lines
  :return:
  """
  rule_file_path = config_reader.six_zero_rule_file_path
  if not os.path.exists(rule_file_path):
    logger.info("60rule:[%s] does not exist, don't need correct it",
                rule_file_path)
    return

  rule_lines = []
  with open(rule_file_path, "r") as f:
    rule_lines = f.readlines()

  patterns = [r"ACTION==", r",KERNEL==", r",PROGRAM==", r",RESULT==", r",RUN"]

  corrected_lines = []
  for line in rule_lines:
    if all([re.search(pattern, line) for pattern in patterns]):
      corrected_lines.append(line)

  if len(rule_lines) == len(corrected_lines):
    logger.info("60rule:[%s] all line are well formed, don't need correct",
                rule_file_path)
    return

  logger.info("60rule:[%s] correct, remove lines:[%s]", rule_file_path,
              set(rule_lines) - set(corrected_lines))
  with open(rule_file_path, "w") as f:
    f.writelines(corrected_lines)


def get_raw_cmd_path_by_os():
  """
  different os system, raw cmd is in different path
  :return: raw cmd full path, like /bin/raw
  """
  cmd = "raw"
  cmd_path = None
  if os_version.is_ubuntu() or os_version.is_kylin():
    cmd_path = "/sbin/raw"
  elif os_version.is_redhat() or os_version.is_centos():
    cmd_path = "/bin/raw"

  if common_utils.is_executable(cmd_path):
    return cmd_path

  cmd_path = common_utils.get_cmd_path(cmd)
  if cmd_path:
    return cmd_path
  else:
    exit_process(-1,
                 "can't find [raw] cmd, we must use this cmd, please install it")


def raw_raw_disk(raw_name, dev_name):
  """
  use raw command to raw device
  :param raw_name: like raw1
  :param dev_name: like sda
  :return: bool whether success
  """
  mkdir(config_reader.system_raw_folder)

  raw_cmd_path = get_raw_cmd_path_by_os()
  raw_path = get_raw_path_by_raw_name(raw_name)
  dev_path = get_dev_path_by_dev_name(dev_name)

  cmd = "{raw_cmd_path} {raw_path} {dev_path}".format(raw_cmd_path=raw_cmd_path,
                                                      raw_path=raw_path,
                                                      dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)

  if ret_code == 0:
    logger.info("raw raw disk success, raw name:[%s] dev name:[%s]", raw_name,
                dev_name)
    return True
  else:
    logger.error(
        "raw raw disk failed, raw name:[%s] dev name:[%s], save rollback file",
        raw_name, dev_name)
    save_rollback_file(dev_name)
    return False


def unraw_raw_disk(raw_name):
  """
  unraw disk
  :param raw_name: like raw1
  :return:
  """
  raw_cmd_path = get_raw_cmd_path_by_os()
  raw_path = get_raw_path_by_raw_name(raw_name)
  cmd = "{cmd_path} {raw_path} 0 0".format(cmd_path=raw_cmd_path,
                                           raw_path=raw_path)
  ret_code, ret_lines = run_cmd(cmd)

  if os.path.exists(raw_path):
    os.remove(raw_path)

  logger.info("unraw the raw device:[%s] ret code:[%s]", raw_name, ret_code)


def get_max_raw_num(scan_system=True, scan_rule_file=True,
    scan_datanode_disk=True):
  """
  get max raw num from 60rule, /dev/raw, link files in datanode disk folders
  :param scan_system whether scan /dev/raw folder
  :param scan_rule_file whether scan 60rule
  :param scan_datanode_disk whether scan link files in datanode disk folders
  :return: num like 7
  """
  raw_names = []

  if scan_system:
    raw_names += get_raw_names_in_system()

  if scan_rule_file:
    raw_names += [content.get_raw_name() for content in get_60_rule_contents()]

  if scan_datanode_disk:
    folder_paths = filter(lambda path: os.path.exists(path),
                          get_all_disk_folder_paths())
    link_file_paths = [os.path.join(folder_path, file_name) for folder_path in
                       folder_paths for file_name in os.listdir(folder_path)]
    real_paths = [os.path.realpath(file_path) for file_path in link_file_paths
                  if os.path.islink(file_path)]
    raw_names += [os.path.basename(path) for path in real_paths]

  numbers = []
  for raw_name in raw_names:
    match_obj = re.search(r"raw(\d+)", raw_name)
    if match_obj:
      numbers.append(int(match_obj.group(1)))

  max_num = 0
  if len(numbers) > 0:
    max_num = max(numbers)

  logger.info("get max raw number:[%s]", max_num)
  return max_num


def get_all_origin_disks(filtered_dev_names=[], filtered_serial_nums=[]):
  """
  get all origin disks that can be used by datanode
  :param filtered_dev_names ignored dev names, if plugin check old device don't need examine
  :param filtered_serial_nums ignored serial nums
  :return: (not partitoned dev names, datanode partition infos)
  """
  dev_names = get_all_dev_name_in_system()
  dev_names = filter_out_ignore_device_by_config(dev_names)
  dev_names = filter_out_pyd_device(dev_names)

  # filter out old devices by dev names
  if len(filtered_dev_names) > 0:
    dev_names = list(set(dev_names) - set(filtered_dev_names))
    logger.info("after filter by old dev name:[%s] remained dev names:[%s]",
                filtered_dev_names, dev_names)

  # filter out by mount info
  dev_names = filter_out_mounted_device(dev_names)

  # filter out virtual disk
  dev_names = filter_out_virtual_device(dev_names)

  # get datanode formatted disks
  datanode_partition_infos = [get_datanode_partitioned_info(dev_name) for
                              dev_name in dev_names]
  datanode_partition_infos = filter(lambda info: info is not None,
                                    datanode_partition_infos)
  logger.info("get datanode partition infos:[%s]", datanode_partition_infos)

  # continue filter to get raw disks that can used by datanode
  # filter out datanode partitioned disk
  dev_names_in_partition_info = [info.get_dev_name() for info in
                                 datanode_partition_infos]
  dev_names = filter(
      lambda dev_name: dev_name not in dev_names_in_partition_info, dev_names)
  logger.info(
      "after filter by datanode partitioned disks, remained dev names:[%s]",
      dev_names)

  # filter out partitioned disk
  dev_names = filter(lambda dev_name: len(
      get_partition_paths(get_dev_path_by_dev_name(dev_name))) <= 0, dev_names)
  logger.info("after filter out partitioned disks, remained dev names:[%s]",
              dev_names)

  # filter out mountable disk
  dev_names = filter(lambda dev_name: not check_mountable(dev_name), dev_names)
  logger.info("after filter by whether mountable, remained dev names:[%s]",
              dev_names)

  # filter old devices by serial num
  if len(filtered_serial_nums) > 0:
    dev_names = filter(lambda dev_name: get_serial_num_for_device(
        get_dev_path_by_dev_name(dev_name)) not in filtered_serial_nums,
                       dev_names)
    datanode_partition_infos = filter(lambda info: get_serial_num_for_device(
        get_dev_path_by_dev_name(
            info.get_raw_disk_partition())) not in filtered_serial_nums,
                                      datanode_partition_infos)
    logger.info("filter out device by serial_nums:[%s]", filtered_serial_nums)

  dev_names = sorted(dev_names)
  datanode_partition_infos = sorted(datanode_partition_infos)
  logger.info(
      "get all origin disks: not partitioned disks:[%s] datanode partitioned disks:[%s]",
      dev_names, datanode_partition_infos)
  return dev_names, datanode_partition_infos


def is_disk_partitioned(dev_path):
  """
  check whether disk is partitioned
  :param dev_path: like /dev/sda
  :return: bool
  """
  if len(get_partition_paths(dev_path)) > 0:
    logger.info("dev_path:[%s] is partitioned", dev_path)
    return True
  else:
    logger.info("dev_paht:[%s] is not partitioned", dev_path)
    return False


def get_datanode_partitioned_info(dev_name):
  """
  use java program to recognize partitioned disk which is partitioned by datanode
  :param dev_name:
  :return: PartitionInfo or None if this is not partitioned or not partitioned by datanode
  """
  dev_path = get_dev_path_by_dev_name(dev_name)
  partition_paths = get_partition_paths(dev_path)

  if len(partition_paths) != 2:
    # datanode will only have 2 partition on one disk
    logger.info(
        "partition paths:[%s] for dev_name:[%s], this is not partitioned by datanode",
        partition_paths, dev_name)
    return None

  partition_paths = sorted(partition_paths, reverse=True)
  raw_disk_partition_path = None
  for partition_path in partition_paths:
    if check_whether_datanode_disk_by_java(partition_path):
      raw_disk_partition_path = partition_path
      break
  else:
    logger.info("dev_name:[%s] is not datanode disk", dev_name)
    return None

  partition_paths.remove(raw_disk_partition_path)
  file_system_partition_path = partition_paths[0]
  partition_info = common_models.PartitionInfo(dev_name=dev_name,
                                               file_system_partition=get_dev_name_by_dev_path(
                                                   file_system_partition_path),
                                               raw_disk_partition=get_dev_name_by_dev_path(
                                                   raw_disk_partition_path))
  logger.info("get partition info:[%s], partitioned by datanode",
              partition_info)
  return partition_info


def get_disks_without_partition():
  """
  get all disks that didn't partition
  :return: list of dev names
  """
  # lsblk -i -n  -o name,type
  # sda             disk
  # sdc             disk
  # |-sdc1          part
  # `-sdc2          part

  cmd = "lsblk -i -n  -o name,type"
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0:
    logger.error("get disks without partition failed, lsblk cmd exec failed")
    return []

  dev_names = []
  for i in range(0, len(ret_lines)):
    line = ret_lines[i]
    if "disk" in line:
      if i == len(ret_lines) - 1 or (
          i < len(ret_lines) - 1 and re.search(r"^[a-z]", ret_lines[i + 1])):
        dev_name = line.strip().split()[0]
        dev_names.append(dev_name)

  # check if ignored by config
  ignore_device_patterns = config_reader.get_ignore_device_regs()
  for dev_name in dev_names[:]:
    for ignore_pattern in ignore_device_patterns:
      if re.match(ignore_pattern, dev_name):
        logger.info(
            "get disk without partition, ignore device:[%s] due to ignore pattern config:[%s]",
            dev_name, ignore_pattern)
        dev_names.remove(dev_name)

  # filter out pyd device
  dev_names = filter(lambda dev_name: "pyd" not in dev_name, dev_names)

  logger.info("get disk without partition, dev names:[%s]", dev_names)
  return dev_names


def filter_out_mounted_device(dev_names):
  """
  filter out device which has mounted
  :param dev_names:
  :return: remained dev names
  """
  # filter out by mount info
  ret_code, ret_lines = run_cmd("mount -l | awk '{print $1}'  | grep '/dev/'")
  remained_dev_names = dev_names[:]
  for i in range(0, len(ret_lines)):
    line = ret_lines[i]
    new_line = re.sub(r'[0-9]+', '', line)
    new_dev_name = get_dev_name_by_dev_path(new_line);
    logger.info("mount disk info:[%s]", new_dev_name)
    if new_dev_name in remained_dev_names:
      remained_dev_names.remove(new_dev_name)
  logger.info(
      "origin dev_names:[%s] filter out mounted device, remained dev_names:[%s]",
      dev_names, remained_dev_names)
  return remained_dev_names


def filter_out_ignore_device_by_config(dev_names):
  """
  filter out device which ignore by config
  :param dev_names:
  :return: list of dev names
  """
  # check if ignored by config
  ignore_device_patterns = config_reader.get_ignore_device_regs()
  remained_dev_names = dev_names[:]
  for dev_name in dev_names:
    for ignore_pattern in ignore_device_patterns:
      if re.match(ignore_pattern, dev_name):
        logger.info(
            "filter out ignore device by config, ignore device:[%s] due to pattern:[%s]",
            dev_name, ignore_pattern)
        remained_dev_names.remove(dev_name)

  logger.info(
      "origin dev_names:[%s] filter out ignore device by config, remained dev_names:[%s]",
      dev_names, remained_dev_names)
  return remained_dev_names


def filter_out_pyd_device(dev_names):
  """
  filter out pyd devices
  :param dev_names:
  :return: list of remained dev_names
  """
  remained_dev_names = filter(lambda dev_name: "pyd" not in dev_name, dev_names)
  logger.info("origin dev_names:[%s] filter pyd device, ramined dev_names:[%s]",
              dev_names, remained_dev_names)
  return remained_dev_names


def filter_out_virtual_device(dev_names):
  """
  filter out virtual disk, some system client and datanode are at same server
  :param dev_names:
  :return:
  """
  virtual_dev_names = get_iscsi_virtual_disk()
  remained_dev_names = list(set(dev_names) - set(virtual_dev_names))
  logger.info(
      "origin dev_names:[%s] filter out virtual device:[%s], remained dev_names:[%s]",
      dev_names, virtual_dev_names, remained_dev_names)
  return remained_dev_names


def get_disk_name_of_all_multi_path_disk():
  """
  translate from perl script:Common.pm:getDiskNameOfAllMultiPathDisk
  i don't understand the theory, just translate, only not worse
  :return: map
  """
  result_map = {}
  disk_folder = "/dev/mapper"
  for disk_name in os.listdir(disk_folder):
    disk_path = os.path.join(disk_folder, disk_name)
    if not os.path.islink(disk_path):
      continue

    result_map[disk_name] = "mapper\/" + disk_name

  logger.info("get disk name of all multi path disk, result:[%s]", result_map)
  return result_map


def get_serial_num_for_device(dev_path, use_java_program=True,
    use_system_cmd=True):
  """
  get serial num for device, first by java program then by system cmd
  if both can't find serial num, just use dev_path as serial num
  :param use_java_program if True, will use java program to query serial num
  :param use_system_cmd if True, will use system cmd scsi_id to query serial num
  :param dev_path: support both /dev/sda and /dev/raw/raw1
  :return: serial_num
  """
  if dev_path in cached_serial_nums:
    serial_num = cached_serial_nums[dev_path]
    logger.info(
        "get serial num:[%s] for device:[%s] from cache, use java program:[%s] generate uuid:[%s]",
        serial_num, dev_path, use_java_program, use_system_cmd)
    return serial_num

  serial_num = None
  try:
    if use_java_program:
      serial_num = get_serial_num_by_java(dev_path)
      if serial_num is not None:
        logger.info(
            "get serial num:[%s] for device:[%s], use java program:[%s] generate uuid:[%s]",
            serial_num, dev_path, use_java_program, use_system_cmd)
        return serial_num

    if use_system_cmd:
      serial_num = str(uuid.uuid4())
      if serial_num is not None:
        logger.info(
            "get serial num:[%s] for device:[%s], use java program:[%s] generate uuid:[%s]",
            serial_num, dev_path, use_java_program, use_system_cmd)
        return serial_num

    # if no serial num exist, use dev path as serial num, some virtual machine may have this situation
    serial_num = dev_path
    logger.warn(
        "get serial num for dev_path:[%s] failed, use java program:[%s] generate uuid:[%s],"
        "so we just use dev_path:[%s] as serial num",
        dev_path, use_java_program, use_system_cmd, dev_path)
    return serial_num
  finally:
    if serial_num is not None:
      logger.info("save serial num:[%s] for device:[%s] to cache", serial_num,
                  dev_path)
      cached_serial_nums[dev_path] = serial_num


def get_serial_num_by_java(dev_path):
  """
  read serial num from archive metadata by java program
  :param dev_path: full path like /dev/sda
  :return: serial num or None if can't read
  """
  cmd = "java -noverify -cp {classpath} py.datanode.QuerySerialNumber --storagePath {dev_path}".format(
      classpath=config_reader.java_classpath, dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0 or len(ret_lines) != 1:
    # exec cmd failed
    logger.error("can't get serial num for dev path:[%s] by java program",
                 dev_path)
    return None

  serial_num = ret_lines[0].strip()
  logger.info("get serial num for dev_path:[%s] success, serial num:[%s]",
              dev_path, serial_num)
  return serial_num


def check_mountable(dev_name):
  """
  check whether device can mount
  :param dev_name: like sda
  :return: bool
  """
  dev_path = get_dev_path_by_dev_name(dev_name)
  return utils.disk.check_mountable(dev_path)


def mkdir(dir_path):
  """
  create folder if not exist
  :param dir_path:
  :return:
  """
  utils.common.mkdir(dir_path)


def append_six_rule_file(raw_name, serial_num, dev_name):
  """
  add one line to six rule file
  :param raw_name: like raw1
  :param serial_num:
  :param dev_name: like sda
  :return:
  """
  action = None
  if os_version.is_ubuntu() or os_version.is_kylin():
    action = "change"
  elif os_version.is_redhat() or os_version.is_centos():
    action = "add"
  else:
    if config_reader.need_check_os_version():
      exit_process(-1, "it can't happen, not ubuntu, centos, redhat")
    else:
      # don't need to check os version, use default value, actually this action value means nothing to datanode
      action = "change"

  raw_cmd_path = get_raw_cmd_path_by_os()

  line = 'ACTION=="{action}",KERNEL=="{dev_name}",PROGRAM=="/lib/udev/scsi_id -g -u /dev/%k",RESULT=="{serial_num}",RUN+="{raw_cmd_path} /dev/raw/{raw_name} %N" '.format(
      action=action, dev_name=dev_name, serial_num=serial_num,
      raw_cmd_path=raw_cmd_path, raw_name=raw_name)

  with open(config_reader.six_zero_rule_file_path, "a") as f:
    f.write(line + "\n")


def append_ssd_record(raw_name, dev_name):
  """
  add one line to ssd.record
  :param raw_name:
  :param dev_name:
  :return:
  """
  line = "{raw_name}, {dev_name}".format(raw_name=raw_name, dev_name=dev_name)
  with open(config_reader.ssd_record_file_path, "a") as f:
    f.write(line + "\n")


def umount_disks_in_folder(folder_path, ignore_folders=[]):
  """
  umount all disk in folder and delete disk folder in this folder except ignored ones
  :param folder_path:
  :param ignore_folders: ignored folders which will not delete or umount
  :return:
  """
  if not os.path.exists(folder_path):
    logger.info(
        "umount disks in folder, folder:[%s] does not exist, don't need umount",
        folder_path)
    return

  logger.info("umount all disks in folder:[%s]", folder_path)

  ret_code, ret_lines = run_cmd("mount")
  mount_info = " ".join(ret_lines)

  for folder_name in os.listdir(folder_path):
    if folder_name in ignore_folders:
      logger.info("ignore folder:[%s] it will not umount or deleted",
                  folder_name)
      continue

    disk_folder_path = os.path.join(folder_path, folder_name)

    # umount disks
    pattern = r"\s+{path}\s+".format(path=disk_folder_path)
    if re.search(pattern, mount_info):
      # this disk is in mount info, need umount
      umount_disk(disk_folder_path)

    # remove disk folders
    force_delete_folder(disk_folder_path)


def partition_format_mount(dev_name):
  """
  partition disk, then mount to datanode
  :param dev_name: like sda
  :return: PartitionInfo or None if failed
  """
  dev_path = get_dev_path_by_dev_name(dev_name)
  try:
    # partition disk
    partition_size_str = config_reader.get_rocksdb_partition_size()
    partition_bytes_size = common_utils.convert_size_str_to_byte_size(
        partition_size_str)
    disk_size = get_disk_size(dev_path)

    if partition_bytes_size >= disk_size:
      logger.error(
          "partition size:[%s] byte format:[%s] is too big for disk:[%s] size:[%s]",
          partition_size_str, partition_bytes_size, dev_name, disk_size)
      raise PartitionSizeTooBig()

    partition_count = 2
    common_utils.new_partition(dev_path, partition_count, [partition_size_str])
    partition_paths = get_partition_paths(dev_path)
    if len(partition_paths) != 2:
      logger.error("partition failed for dev_name:[%s]", dev_name)
      raise PartitionException()

    # dd disk, incase there is old data in disk
    for partition_path in partition_paths:
      dd_disk(partition_path)

    # mkfs disk
    file_system_partition_path = partition_paths[0]
    raw_disk_partition_path = partition_paths[1]
    if not common_utils.mkfs_disk(file_system_partition_path,
                                  config_reader.get_mkfs_cmd()):
      logger.error("mkfs failed for dev_path:[%s]", file_system_partition_path)
      raise MkfsException()

    # mount disk
    if not mount_into_datanode(file_system_partition_path):
      raise MountException()

    partition_info = common_models.PartitionInfo(dev_name,
                                                 get_dev_name_by_dev_path(
                                                     file_system_partition_path),
                                                 get_dev_name_by_dev_path(
                                                     raw_disk_partition_path))

    logger.info(
        "partition and mount success for dev_name:[%s] partition info:[%s]",
        dev_name, partition_info)
    return partition_info

  except (
      PartitionException, MkfsException, MountException, PartitionSizeTooBig):
    common_utils.clean_partition(dev_path)
    logger.error(
        "partition and mount failed for dev_name:[%s], clean partition for it",
        dev_name)
    return None


def mount_into_datanode(dev_path):
  """
  mount disk into datanode
  :param dev_path: like /dev/sda1
  :return: bool
  """
  # create mount point
  mount_point = get_mount_path_by_partition_name(os.path.basename(dev_path))

  if os.path.exists(mount_point):
    logger.error("mount_point:[%s] already exist, can't mount dev_path:[%s]",
                 mount_point, dev_path)
    return False

  mkdir(mount_point)

  # mount disk
  if utils.disk.mount_disk(dev_path, mount_point):
    return True
  else:
    force_delete_folder(mount_point)
    return False


def get_partition_record_contents(reload=False):
  """
  get partition info record in file
  :param reload: whether force read from file
  :return: list of PartitionInfo
  """
  global partition_record_contents
  if partition_record_contents is not None and not reload:
    return partition_record_contents

  partition_record_contents = []
  if os.path.exists(config_reader.partition_record_file_path):
    with open(config_reader.partition_record_file_path, "r") as f:
      l = json.load(f)
      for d in l:
        partition_record_contents.append(PartitionInfo.from_dict(d))

  logger.info("load partition record, file path:[%s] content:[%s]",
              config_reader.partition_record_file_path,
              partition_record_contents)
  return partition_record_contents


def append_partition_record_contents(partition_infos):
  """
  save partition info to record file
  :param partition_infos:
  :return:
  """
  origin_partition_infos = get_partition_record_contents(reload=True)
  whole_partition_infos = sorted(
      list(set(origin_partition_infos + partition_infos)))

  l = [info.to_dict() for info in whole_partition_infos]
  with open(config_reader.partition_record_file_path, "w") as f:
    json.dump(l, f, indent=4, sort_keys=True)

  logger.info("append partition record, file path:[%s] content:[%s]",
              config_reader.partition_record_file_path, partition_infos)


def filter_out_by_rollback_file(dev_infos):
  """
  filter out dev infos by rollback file
  :param dev_infos: list of DevInfo
  :return: remained list of DevInfo
  """
  dev_names_from_rollback_file = get_rollback_file_content()
  remained_dev_infos = filter(lambda
                                  dev_info: dev_info.get_dev_name() not in dev_names_from_rollback_file,
                              dev_infos)

  if len(remained_dev_infos) == len(dev_infos):
    return dev_infos
  else:
    logger.info(
        "filter dev info by rollback file, origin:[%s] remained:[%s] rollback file:[%s]",
        dev_infos, remained_dev_infos, dev_names_from_rollback_file)
    return remained_dev_infos


def get_link_paths(raw_names):
  """
  get disk link paths which link to specific raw names
  :param raw_names:
  :return: list of link path
  """
  if len(raw_names) <= 0:
    return []

  link_paths = []
  for folder_path in get_all_disk_folder_paths():
    for link_name in os.listdir(folder_path):
      link_path = os.path.join(folder_path, link_name)

      if os.path.islink(link_path) and os.path.basename(
          os.path.realpath(link_path)) in raw_names:
        link_paths.append(link_path)

  logger.info("find link paths:[%s] which link to raw names:[%s]", link_paths,
              raw_names)
  return link_paths


def clean_up_disk(dev_name):
  """
  1. umount partition
  2. dd all partition
  3. clean partition info
  4. dd disk
  :param dev_name: like sda
  :return:
  """
  dev_path = get_dev_path_by_dev_name(dev_name)
  partition_paths = get_partition_paths(dev_path)

  partitioned = False
  if len(partition_paths) > 0:
    # partitioned disk
    partitioned = True
    for partition_path in partition_paths:
      mount_point = get_mount_point(partition_path)
      if mount_point:
        umount_disk(mount_point)
        force_delete_folder(mount_point)

      dd_disk(partition_path)

    common_utils.clean_partition(dev_path)

  dd_disk(dev_path)
  logger.info("clean up dev name:[%s], partitioned:[%s]", dev_name, partitioned)


def set_path_env():
  """
  sometimes path environment miss some path
  :return:
  """
  path_separator = ":"
  path_variable_name = "PATH"

  important_paths = config_reader.get_path_environment()
  origin_paths = os.getenv(path_variable_name).split(path_separator)

  append_paths = []
  for path in important_paths:
    # i want to keep path sequence
    if path not in origin_paths:
      append_paths.append(path)

  if len(append_paths) > 0:
    logger.info("need update path environment, origin:[%s] append paths:[%s]",
                origin_paths, append_paths)
    os.environ[path_variable_name] = path_separator.join(
        origin_paths + append_paths)
    logger.info("new path variable:[%s]", os.getenv(path_variable_name))


def disable_disk_native_cache(dev_infos, is_plugin_progress):
  """
  disable datanode disk native cache
  :param dev_infos:
  :param is_plugin_progress:
  :return:
  """
  if config_reader.is_on_vms():
    logger.info(
        "this is virtual machine, can't disable disk native cache for dev infos:[%s]",
        dev_infos)
    return

  if not config_reader.need_disable_disk_native_cache():
    logger.info(
        "don't need to disable disk native cache according to config, for dev infos:[%s]",
        dev_infos)
    return

  for dev_info in dev_infos:
    dev_name = dev_info.get_dev_name()
    dev_path = get_dev_path_by_dev_name(dev_name)
    dev_type = dev_info.get_dev_type()

    if DevType.is_fast_type(dev_type):
      logger.info(
          "disable disk native cache, skip for dev name:[%s], because it is:[%s]",
          dev_name, dev_type)
      continue

    disable_one_disk_native_cache(dev_path)


def disable_one_disk_native_cache(dev_path):
  """
  disable datanode disk native cache
  :param dev_path: like /dev/sda
  :return:
  """
  hdparm_cmd = "hdparm -W 0 {dev_path}".format(dev_path=dev_path)
  ret_code, ret_lines = run_cmd(hdparm_cmd)

  if ret_code == 0:
    logger.info(
        "use hdparm disable disk native cache success for dev path:[%s]",
        dev_path)
    return

  sdparm_cmd = "sdparm --set WCE=0 {dev_path}".format(dev_path=dev_path)
  ret_code, ret_lines = run_cmd(sdparm_cmd)

  if ret_code == 0:
    logger.info(
        "use sdparm disable disk native cache success for dev path:[%s]",
        dev_path)
    return

  logger.error("disable disk native cache failed for dev path:[%s]", dev_path)
  save_rollback_file(get_dev_name_by_dev_path(dev_path))


set_lang_to_english()
set_path_env()
