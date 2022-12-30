# -*- encoding=utf-8 -*-
"""
encapsulate parted tool function
!!! becareful, after use parted command, you must run partprobe, even if you just parted /dev/sda -s print.
if not partprobe, later code can't find partitions
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

import logging

from common import run_cmd

logger = logging.getLogger(__name__)


def partprobe():
  """
  call system cmd partprobe to reread partition info
  :return:
  """
  logger.info("run partprobe to reload partition info")
  cmd = "partprobe"
  run_cmd(cmd)


def get_partition_nums(dev_path):
  """
  get all partition numbers in disk
  :param dev_path: like /dev/sda
  :return: list of partition numbers like [1, 2]
  """
  # parted /dev/sdd -s p
  # Model: QEMU QEMU HARDDISK (scsi)
  # Disk /dev/sdd: 7516MB
  # Sector size (logical/physical): 512B/512B
  # Partition Table: msdos
  # Disk Flags:
  #
  # Number  Start   End     Size    Type     File system  Flags
  #  1      1049kB  1075MB  1074MB  primary  ext4
  #  2      1075MB  7516MB  6441MB  primary

  # parted /dev/sda -s p
  # Error: /dev/sda: unrecognised disk label
  # Model: QEMU QEMU HARDDISK (scsi)
  # Disk /dev/sda: 5369MB
  # Sector size (logical/physical): 512B/512B
  # Partition Table: unknown
  # Disk Flags:

  cmd = "parted {dev_path} -s p".format(dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0:
    logger.error("can't get partition info for dev_path:[%s], exec cmd failed",
                 dev_path)
    return []

  partition_lines = []
  for i in range(0, len(ret_lines)):
    line = ret_lines[i]
    if line.startswith("Number"):
      partition_lines = ret_lines[i + 1:]
      break

  partition_nums = []
  for line in partition_lines:
    array = line.split()
    if len(array) > 0 and array[0].isdigit():
      num = int(array[0])
      partition_nums.append(num)

  sorted(partition_nums)
  logger.info("get partition numbers:[%s] for dev_path:[%s]", partition_nums,
              dev_path)
  return partition_nums


def mklabel(dev_path, table="gpt"):
  """
  change partition table to table
  :param dev_path: like /dev/sda
  :param table: like gpt or msdos
  :return:
  """
  try:
    logger.info("change dev_path:[%s] partition table to [%s]", dev_path, table)
    cmd = "parted {dev_path} -s mklabel {table}".format(dev_path=dev_path,
                                                        table=table)
    run_cmd(cmd)
  finally:
    partprobe()


def mkpart(dev_path, index, start, end):
  """
  partition disk on linux
  :param dev_path: full device path, like: /dev/sda
  :param index: partition index, number like 1
  :param start end: like 1M, 2G, 0% is begin, 100% is end
  :return: bool whether partition success
  """
  try:
    cmd = "parted {dev_path} -s mkpart {index} {start} {end}".format(
        dev_path=dev_path, index=index, start=start, end=end)

    ret_code, ret_lines = run_cmd(cmd)
    if ret_code != 0:
      logger.error(
          "new partition for disk:[%s] failed, index:[%s] start:[%s] end:[%s]",
          dev_path, index, start, end)
      return False

    logger.info(
        "new partition for disk:[%s] success, index:[%s] start:[%s] end:[%s]",
        dev_path, index, start, end)
    return True
  finally:
    partprobe()


def rm(dev_path, number):
  """
  rm one partition
  :param dev_path: like /dev/sdb
  :param number: partition number like 1
  :return: bool
  """
  try:
    cmd = "parted {dev_path} -s rm {number}".format(dev_path=dev_path,
                                                    number=number)

    ret_code, ret_lines = run_cmd(cmd)
    if ret_code != 0:
      logger.error("remove partition for disk:[%s] failed, number:[%s] ",
                   dev_path, number)
      return False

    logger.info("remove partition for disk:[%s] success, number:[%s]", dev_path,
                number)
    return True
  finally:
    partprobe()


def clear_partition(dev_path):
  """
  clear all partition
  :return: bool
  """
  try:
    numbers = get_partition_nums(dev_path)
    sorted(numbers, reverse=True)

    for number in numbers:
      if not rm(dev_path, number):
        logger.error("clear partition for disk:[%s] failed, numbers:[%s]",
                     dev_path, numbers)
        return False

    logger.info("clear partition for disk:[%s] success, numbers:[%s]", dev_path,
                numbers)
    return True
  finally:
    partprobe()
