# -*- encoding=utf-8 -*-
"""
disk utils
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

import common
import glob
import logging
import os
import re
from common import run_cmd

logger = logging.getLogger(__name__)


def get_all_dev_name_in_system():
  """
  recognize all disks in system
  :return: list of dev name
  """
  # lsblk -i -d -n -o name,type
  # >sda  disk
  # >sr0  rom
  cmd = "lsblk -i -d -n -o name,type"
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0:
    logger.error("can't get device from system")
    return []

  dev_names = []
  for line in ret_lines:
    array = line.split()
    if len(array) != 2:
      logger.error("can't parse device from line:[%s]", line)
      continue

    dev_name = array[0]
    type = array[1]
    if type == "disk":
      dev_names.append(dev_name)

  logger.info("find all dev name:[%s] in system", dev_names)
  return dev_names


def get_iscsi_virtual_disk():
  """
  translate from perl script:Common.pm:getAllIscsiVirtualDisk
  get all iscsi virtual disk dev name
  :return: list of dev_names
  """
  virtual_dev_names = list(
      set(get_iscsi_virtual_disk_v1() + get_iscsi_virtual_disk_v2()))
  logger.info("find all iscsi virtual disk, dev names:[%s]", virtual_dev_names)
  return virtual_dev_names


def get_iscsi_virtual_disk_v1():
  """
  translate from perl script:Common.pm:getAllIscsiVirtualDisk
  get all iscsi virtual disk dev name
  :return: list of dev_names
  """
  # iscsiadm -m session 2>&1
  # tcp: [10] 192.168.122.203:3260,1 iqn.2017-08.zettastor.iqn:459987651366127254-0 (non-flash)
  cmd = "iscsiadm -m session 2>&1"
  ret_code, session_infos = run_cmd(cmd)
  if ret_code != 0:
    logger.info(
        "get iscsi virtual disk by iscsiadm failed, run cmd:[%s] failed", cmd)
    return []

  # ls -ln /dev/disk/by-path
  # ip-192.168.122.203:3260-iscsi-iqn.2017-08.zettastor.iqn:459987651366127254-0-lun-0 -> ../../sde
  # pci-0000:00:09.0-virtio-pci-virtio4-scsi-0:0:0:0 -> ../../sda
  disk_folder = "/dev/disk/by-path"
  disk_paths = [os.path.join(disk_folder, filename) for filename in
                os.listdir(disk_folder)]

  virtual_dev_names = []
  for session_info in session_infos:
    # search ip and port
    match_obj = re.search(r"\s+(\d+\.\d+\.\d+\.\d+:\d+)", session_info)
    if not match_obj:
      continue

    host = match_obj.group(1)
    pattern = "ip-" + host
    for disk_path in disk_paths:
      if os.path.islink(disk_path) and pattern in disk_path:
        dev_name = os.path.basename(os.path.realpath(disk_path))
        logger.info(
            "find iscsi virtual disk by iscsiadm, session:[%s] disk path:[%s] virtual dev name:[%s]",
            session_info, disk_path, dev_name)
        virtual_dev_names.append(dev_name)

  virtual_dev_names = list(set(virtual_dev_names))
  logger.info("find all iscsi virtual disk by iscsiadm, dev names:[%s]",
              virtual_dev_names)
  return virtual_dev_names


def get_iscsi_virtual_disk_v2():
  """
  get iscsi virtual disk by lsscsi
  :return: list of dev_names
  """
  # lsscsi
  # [0:2:0:0]    disk    DELL     PERC H710P       3.13  /dev/sda
  # [5:0:0:0]    disk    LIO-ORG  pyd0             4.0   /dev/sde
  cmd = "lsscsi"
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0:
    logger.info("get iscsi virtual disk by lsscsi failed, run cmd:[%s] failed",
                cmd)
    return []

  virtual_dev_names = []
  for line in ret_lines:
    if "pyd" not in line:
      continue

    match_obj = re.search(r"/dev/(\w+)\s*$", line)
    if not match_obj:
      continue

    dev_name = match_obj.group(1)
    logger.info("find iscsi virtual disk by lsscsi, line:[%s] dev name:[%s]",
                line, dev_name)
    virtual_dev_names.append(dev_name)

  logger.info("find all iscsi virtual disk by lsscsi, dev names:[%s]",
              virtual_dev_names)
  return virtual_dev_names


def check_mountable(dev_path):
  """
  check whether device can mount
  :param dev_path: like /dev/sda
  :return: bool
  """
  tmp_mount_path = "/tmp/tmpMount"

  if not os.path.exists(tmp_mount_path):
    os.makedirs(tmp_mount_path)

  mount_cmd = "mount {dev_path} {mount_path} 2>&1".format(dev_path=dev_path,
                                                          mount_path=tmp_mount_path)
  ret_code, ret_lines = run_cmd(mount_cmd)

  umount_cmd = "umount {mount_path} 2>&1".format(mount_path=tmp_mount_path)
  run_cmd(umount_cmd)
  common.force_delete_folder(tmp_mount_path)

  if ret_code == 0:
    logger.info("mount dev_path:[%s] to mount_path:[%s] success", dev_path,
                tmp_mount_path)
    return True
  else:
    logger.info("mount dev_path:[%s] to mount_path:[%s] failed", dev_path,
                tmp_mount_path)
    return False


def get_mount_point(partition_path):
  """
  get mount point for partition path
  :param partition_path: like /dev/sda1
  :return: like /mnt/sda1 or None means not mount
  """
  # lsblk -i -n -o mountpoint /dev/sda1
  # /tmp/sda1
  cmd = "lsblk -i -n -o mountpoint {partition_path}".format(
      partition_path=partition_path)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0 or len(ret_lines) != 1 or ret_lines[0].strip() == "":
    logger.info("partition path:[%s] does not mounted, can't get mount point",
                partition_path)
    return None

  mount_point = ret_lines[0].strip()
  logger.info("partition path:[%s] mount point is [%s]", partition_path,
              mount_point)
  return mount_point


def umount_disk(mount_point):
  """
  call umount cmd to umount disk
  :param mount_point:
  :return: bool
  """
  if not os.path.exists(mount_point):
    logger.info("umount folder:[%s] success, because this folder doesn't exist",
                mount_point)
    return True

  cmd = "umount {path}".format(path=mount_point)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code == 0:
    logger.info("umount folder:[%s] success", mount_point)
    return True
  else:
    logger.warn("umount folder:[%s] failed", mount_point)
    return False


def mount_disk(dev_path, mount_point_path):
  """
  mount disk
  :param dev_path: like /dev/sda1
  :param mount_point_path: mount folder
  :return: bool
  """
  # mount /dev/sda1 sda1
  cmd = "mount {dev_path} {mount_point}".format(dev_path=dev_path,
                                                mount_point=mount_point_path)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code == 0:
    logger.info("%s success", cmd)
    return True
  else:
    logger.warn("%s failed", cmd)
    return False


def partprobe():
  """
  call system cmd partprobe to reread partition info
  :return:
  """
  logger.info("run partprobe to reload partition info")
  cmd = "partprobe"
  run_cmd(cmd)


def get_partition_paths(dev_path):
  """
  get all partitions in disk
  :param dev_path: like /dev/sda
  :return: list of partition paths like ["/dev/sda1", "/dev/sda2"]
  """
  cmd = "fdisk -l {dev_path}".format(dev_path=dev_path)
  ret_code, ret_lines = run_cmd(cmd)
  if ret_code != 0:
    logger.error("can't get partition info for dev_path:[%s], exec cmd failed",
                 dev_path)
    return []
  # fdisk -l /dev/sdc
  # Disk /dev/sdc: 10.7 GB, 10737418240 bytes, 20971520 sectors
  # Units = sectors of 1 * 512 = 512 bytes
  # Sector size (logical/physical): 512 bytes / 512 bytes
  # I/O size (minimum/optimal): 512 bytes / 512 bytes
  # Disk label type: dos
  # Disk identifier: 0x000787e7
  #   Device Boot      Start         End      Blocks   Id  System
  # /dev/sdc1            2048     2099199     1048576   83  Linux
  # /dev/sdc2         2099200    20971519     9436160   83  Linux
  partition_lines = filter(lambda line: line.startswith(dev_path), ret_lines)
  partition_paths = [line.split()[0] for line in partition_lines]
  if len(partition_paths) > 0:
    logger.info("get partition paths:[%s] for dev_path:[%s]", partition_paths,
                dev_path)
    return partition_paths

  # fdisk -l /dev/sdb
  # WARNING: fdisk GPT support is currently new, and therefore in an experimental phase. Use at your own discretion.
  #
  # Disk /dev/sdb: 7516 MB, 7516192768 bytes, 14680064 sectors
  # Units = sectors of 1 * 512 = 512 bytes
  # Sector size (logical/physical): 512 bytes / 512 bytes
  # I/O size (minimum/optimal): 512 bytes / 512 bytes
  # Disk label type: gpt
  ##         Start          End    Size  Type            Name
  # 1         2048      2099199      1G  Linux filesyste
  # 2      2099200     14680030      6G  Linux filesyste
  index_of_start_line = 0
  for i in range(0, len(ret_lines)):
    if "Start" in ret_lines[i]:
      index_of_start_line = i
      break
  else:
    logger.info("get partition paths:[] for dev_path:[%s]", dev_path)
    return []

  partition_lines = ret_lines[index_of_start_line + 1:]
  partition_paths = []
  for line in partition_lines:
    line = line.strip()
    if len(line.split()) > 0:
      dev_num = line.split()[0]
      paths = glob.glob(dev_path + "*" + dev_num)

      if len(paths) == 0:
        logger.error("can't find partition by dev num:[%s] for dev path:[%s]",
                     dev_num, dev_path)
      elif len(paths) == 1:
        partition_paths.append(paths[0])
      else:
        logger.error(
            "find multi partition for dev num:[%s] paths:[%s] for dev path:[%s]",
            dev_num, paths, dev_path)

  logger.info("get partition paths:[%s] for dev_path:[%s]", partition_paths,
              dev_path)
  return partition_paths
