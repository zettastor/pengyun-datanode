# -*- encoding=utf-8 -*-
# Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
"""
some basic independent common utils
import inspect
import logging
import os
import re
import shutil
import subprocess
import sys

try: #python2
  from exceptions import JavaReadWriteException
except ImportError: #python3
  from .exceptions import JavaReadWriteException

logger = logging.getLogger(__name__)

ENGLISH_LANG = "en_US.UTF-8"


def run_cmd(cmd):
  """
  run some system cmd
  :param cmd:
  :return:
  """
  logger.info("run cmd:[%s]", cmd)

  child = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           shell=True)
  std_out, std_err = child.communicate()
  # python3 return bytes, need decode, python2 return str, not need
  if is_python3():
    std_out = std_out.decode()
    std_err = std_err.decode()

  ret_code = child.poll()

  # filter out log4j error
  std_out = os.linesep.join(filter_out_log4j_error_line(std_out.splitlines()))
  std_err = os.linesep.join(filter_out_log4j_error_line(std_err.splitlines()))

  logger.info("run cmd:[%s] ret code:[%s] std out:[%s] std err:[%s]", cmd,
              ret_code, std_out, std_err)
  return ret_code, std_out.splitlines()


def is_python3():
  return sys.version_info.major >= 3


def filter_out_log4j_error_line(lines):
  """
  filter line that contain log4j error, because it is to noisy
  :param lines:
  :return: remained lines
  """
  if len(lines) <= 0:
    return []

  remained_lines = lines
  remained_lines = [line for line in remained_lines if "log4j:ERROR Attempted to append to closed appender named" not in line]
  remained_lines = [line for line in remained_lines if "log4j:ERROR Could not instantiate appender named" not in line]
  remained_lines = [line for line in remained_lines if "log4j:ERROR Could not find value for key" not in line]

  return remained_lines


def force_delete_folder(folder_path):
  """
  force delete folder
  :param folder_path:
  :return:
  """
  if os.path.exists(folder_path):
    logger.info("delete folder:[%s]", folder_path)
    shutil.rmtree(folder_path, ignore_errors=True)


def set_lang_to_english():
  """
  some linux set lang to chinese, may cause some problem, so we need to set LANG
  :return:
  """
  variable_name = "LANG"
  if os.getenv(variable_name) != ENGLISH_LANG:
    logger.info(
        "we need to set environment variable LANG to [%s], origin value is [%s]",
        ENGLISH_LANG, os.getenv(variable_name))
    os.environ[variable_name] = ENGLISH_LANG


def mkdir(dir_path):
  """
  create folder if not exist
  :param dir_path:
  :return:
  """
  if os.path.exists(dir_path):
    return

  os.makedirs(dir_path)
  logger.info("create folder:[%s]", dir_path)


def force_delete_file(filepath):
  """
  delete file if exist
  :param filepath:
  :return:
  """
  if os.path.exists(filepath):
    logger.info("delete file:[%s]", filepath)
    os.remove(filepath)


def dd_disk(dev_path, size=16):
  """
  use dd cmd to clean disk
  :param dev_path:
  :param size: unit is MB
  :return: bool
  """
  cmd = "dd if=/dev/zero of={dev_path} bs=1M count={size}".format(
      dev_path=dev_path, size=size)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code == 0:
    logger.info("%s success", cmd)
    return True
  else:
    logger.warn("%s failed", cmd)
    return False


def check_whether_pcie_by_name_pattern(dev_name, pcie_name_patterns):
  """
  check whether disk is pcie, by name pattern from datanode config
  :param dev_name: like sda
  :param pcie_name_patterns: list of strs, pcie name pattern
  :return: bool
  """
  for pattern in pcie_name_patterns:
    if re.search(pattern, dev_name):
      logger.info(
          "disk is pcie according name pattern, dev name:[%s] pattern:[%s]",
          dev_name, pattern)
      return True

  logger.info(
      "disk is not pcie according name pattern, dev name:[%s] pattern:[%s]",
      dev_name, pcie_name_patterns)
  return False


def check_whether_ssd_by_name_pattern(dev_name, ssd_name_patterns):
  """
  check whether disk is ssd by ssd pattern from datanode config
  :param dev_name: like sda
  :param ssd_name_patterns: list of strs
  :return: bool
  """
  for pattern in ssd_name_patterns:
    if re.search(pattern, dev_name):
      logger.info("dev name:[%s] is ssd according pattern:[%s]", dev_name,
                  pattern)
      return True

  logger.info("dev name:[%s] is not ssd according config", dev_name)
  return False


def check_whether_ssd_by_speed_test(dev_path, class_path, class_name,
    speed_threshold, detect_time=3):
  """
  check whether disk is ssd by speed test
  :param dev_path: like /dev/sda
  :param class_path: java class path
  :param class_name: speed test java program name
  :param speed_threshold:
  :param detect_time:
  :return: bool
  :exception JavaReadWriteException
  """
  cmd = "java -noverify -cp {classpath} " \
        "{class_name} " \
        "--storagepath {dev_path} " \
        "--timesecond {detect_time} " \
        "--threshold {threshold}".format(
      classpath=class_path, class_name=class_name, dev_path=dev_path,
      detect_time=detect_time, threshold=speed_threshold)

  ret_code, ret_lines = run_cmd(cmd)
  if int(ret_code) == 100:
    logger.info("check ssd by speed for dev path:[%s], result is [normal] disk",
                dev_path)
    return False
  elif int(ret_code) == 101:
    logger.info("check ssd by speed for dev path:[%s], result is [ssd] disk",
                dev_path)
    return True
  else:
    logger.error(
        "check ssd by speed for dev path:[%s] failed, there is something wrong for java program",
        dev_path)
    raise JavaReadWriteException()


def exit_process(exit_code, info):
  """
  exit process, log into log file, and print info at console
  :param exit_code:
  :param info:
  :return:
  """
  stack = inspect.stack()
  file_path = stack[1][1]
  line_num = stack[1][2]

  logger.error("exit process at [%s:%s] info:[%s] exit code:[%s]", file_path,
               line_num, info, exit_code)
  print(("exit process at [%s:%s] info:[%s] exit code:[%s]" % (
    file_path, line_num, info, exit_code)))
  exit(exit_code)


def get_scsi_id(dev_path):
  """
  use system cmd to get scsiid for device
  :param dev_path: full path like /dev/sda
  :return: scsiid or None if can't get
  """
  cmd = "/lib/udev/scsi_id -g -u {dev_path}".format(dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0 or len(ret_lines) != 1:
    logger.error("can't get scsiid for dev path:[%s]", dev_path)
    return None

  scsiid = ret_lines[0].strip()
  logger.info("get scsiid for dev_path:[%s] success, scsiid:[%s]", dev_path,
              scsiid)
  return scsiid
