# -*- encoding=utf-8 -*-
"""
some common utils
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

import distutils.spawn
import os
import re
import time

from utils import logging_config
from utils.common import run_cmd
from utils.disk import get_partition_paths

logger = logging_config.get_logger()

ENGLISH_LANG = "en_US.UTF-8"

GB_SIZE = 1024 ** 3
MB_SIZE = 1024 ** 2
KB_SIZE = 1024
B_SIZE = 1

SIZE_ANCHOR_MAP = {
  "gb": GB_SIZE,
  "g": GB_SIZE,
  "mb": MB_SIZE,
  "m": MB_SIZE,
  "kb": KB_SIZE,
  "k": KB_SIZE,
  "b": B_SIZE
}

cmd_path_cache = {}


def new_partition(dev_path, count, disk_size_strs):
  """
  partition disk on linux
  :param dev_path: full device path, like: /dev/sda
  :param count: partition count
  :param disk_size_strs: each partition disk size, if len(disk_sizes) == count -1, then last partition take the rest size,
  for example: ["1G", "3M", "7K"]
  :return: bool whether partition success
  """
  if count != len(disk_size_strs) and count != (len(disk_size_strs) + 1):
    logger.error(
        "new partition for disk:[%s] failed, count:[%s] disk size count:[%s]",
        dev_path, count, len(disk_size_strs))
    return False

  if count > 3:
    logger.error(
        "new partition for disk:[%s] failed, now we just support at most 3 partition,"
        " but you want count:[%s] partition", dev_path, count)
    return False

  for i in range(0, count):
    disk_size_str = None
    if i < len(disk_size_strs):
      disk_size_str = "+" + disk_size_strs[i]
    else:
      disk_size_str = ""

    cmd = '''echo "
        n
        p
        {index}

        {disk_size}
        w
        " | fdisk {dev_path}'''.format(index=i + 1, disk_size=disk_size_str,
                                       dev_path=dev_path)

    ret_code, ret_lines = run_cmd(cmd)
    if ret_code != 0:
      logger.error("new partition for disk:[%s] failed, index:[%s]", dev_path,
                   i)
      return False

    # sleep for a while for partition table to take effect
    time.sleep(1)

  logger.info("new partition for disk:[%s] success, count:[%s] disk_sizes:[%s]",
              dev_path, count, disk_size_strs)
  return True


def clean_partition(dev_path):
  """
  clean up disk partition info
  :param dev_path: full device path, like: /dev/sda
  :return: bool whether clean success
  """
  dev_names = get_partition_paths(dev_path)

  if len(dev_names) == 0:
    logger.info("There is no partition in dev_path:[%s], don't need to clean.",
                dev_path)
    return True

  for i in range(0, len(dev_names)):
    cmd = '''echo "
        d
        
        w
        " | fdisk {dev_path}'''.format(dev_path=dev_path)

    ret_code, ret_lines = run_cmd(cmd)
    if ret_code != 0:
      logger.error("clean partition for disk:[%s] failed", dev_path)
      return False

    # sleep for a while for partition table to take effect
    time.sleep(1)

  logger.info("clean partition success for disk:[%s]", dev_path)
  return True


def mkfs_disk(dev_path, mkfs_cmd="mkfs.ext4"):
  """
  mkfs disk
  :param mkfs_cmd:
  :param dev_path:
  :return: bool
  """
  cmd = "{mkfs_cmd} {dev_path}".format(mkfs_cmd=mkfs_cmd, dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)

  if ret_code == 0:
    logger.info("%s dev_path:[%s] success", mkfs_cmd, dev_path)
    return True
  else:
    logger.info("%s dev_path:[%s] fail", mkfs_cmd, dev_path)
    return False


def convert_size_str_to_byte_size(size_str):
  """
  convert size str like 3G, to byte size
  :param size_str: like 5G, 5g, 5GB, 5gb, 5m, 5k, 5b, 5
  :return: size in bytes or None if failed
  """
  if size_str.isdigit():
    logger.info("size str:[%s] is all digit, don't need convert", size_str)
    return int(size_str)

  match_obj = re.match(r"(\d+)([a-zA-Z]+)", size_str)
  if not match_obj or match_obj.group(2).lower() not in SIZE_ANCHOR_MAP:
    logger.info("size str:[%s] covert bytes size failed", size_str)
    return None

  size_num = int(match_obj.group(1))
  size_anchor = match_obj.group(2).lower()

  bytes_size = size_num * SIZE_ANCHOR_MAP[size_anchor]
  logger.info("size str:[%s] convert bytes size:[%s]", size_str, bytes_size)
  return bytes_size


def get_cmd_path(cmd):
  """
  get cmd full path from system path
  :param cmd: like wget curl raw
  :return: path or None
  """
  if cmd in cmd_path_cache:
    return cmd_path_cache[cmd]

  path = distutils.spawn.find_executable(cmd)

  if path and is_executable(path):
    logger.info("find cmd:[%s] full path:[%s]", cmd, path)
    cmd_path_cache[cmd] = path
    return path
  else:
    logger.error("can't find cmd:[%s] full path", cmd)
    cmd_path_cache[cmd] = None
    return None


def is_executable(file_path):
  """
  check whether file is executable
  :param file_path:
  :return: bool
  """
  if file_path and os.path.exists(file_path) and os.access(file_path, os.X_OK):
    return True
  else:
    return False
