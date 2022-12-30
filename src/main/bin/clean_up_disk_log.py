# -*- encoding=utf-8 -*-
"""
copy from CleanupDiskLog.pl
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

import os
import sys


def find_project_folder(script_folder):
  """
  find project base folder from real script folder
  :param script_folder: like /var/testing/_packages/datanode-2.4.0/bin
  :return: like /var/testing/packages/datanode-2.4.0
  """
  path = script_folder

  # first find packages folder
  while "packages" not in os.listdir(path):
    parent = os.path.dirname(path)

    if parent == path:
      print("can't find datanode project folder, clean failed")
      exit(-1)
    else:
      path = parent

  path = os.path.join(path, "packages")

  # find datanode project folder
  for name in os.listdir(path):
    if "datanode" in name:
      return os.path.join(path, name)

  print("can't find datanode project folder, clean failed")
  exit(-1)


# like /var/testing/_packages/datanode-2.4.0/bin
script_folder = sys.path[0]
# like /var/testing/packages/datanode
project_folder = find_project_folder(script_folder)
disk_log_folder = os.path.join(project_folder, "var/storage/disklog")

if not os.path.exists(disk_log_folder):
  print("there is no disk log folder, don't need clean")
  exit(0)

# delete the oldest file, sort file by file name, delete the smallest
# only allow 3 file in folder
max_file_count = 3
filenames = os.listdir(disk_log_folder)
while len(filenames) > max_file_count:
  oldest_file = min(filenames)
  filenames.remove(oldest_file)
  os.remove(os.path.join(disk_log_folder, oldest_file))
  print("delete disk log:[%s]" % oldest_file)
