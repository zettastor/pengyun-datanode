# -*- encoding=utf-8 -*-

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
import common_utils
import os
from utils import logging_config

logger = logging_config.get_logger()

if __name__ == "__main__":

  logger.info("Use fdisk tool script")
  parser = argparse.ArgumentParser(description="partition tool")
  parser.add_argument("--dev_path", action="store", required=True,
                      help="device path to manipulate")
  parser.add_argument("--new_partition", action="store_true", default=False,
                      help="create new partition")
  parser.add_argument("--count", action="store", type=int,
                      help="partition count, current max support 3 partition count")
  parser.add_argument("--partition_sizes", metavar="partition_size",
                      action="store", nargs="*",
                      help="each partition size, like 1G, 2M, 3K, "
                           "size count should equal count or smaller than count by only one, "
                           "unsupport decimal like 1.5G")
  parser.add_argument("--clean_partition", action="store_true", default=False,
                      help="remove all partition info")

  args = parser.parse_args()

  dev_path = args.dev_path
  if not os.path.dirname(dev_path):
    dev_path = Common.get_dev_path_by_dev_name(dev_path)

  if args.new_partition:
    count = args.count
    partition_sizes = args.partition_sizes

    if common_utils.new_partition(dev_path, count, partition_sizes):
      print("partition dev_path:[%s] success" % dev_path)
      exit(0)
    else:
      print("partition dev_path:[%s] failed" % dev_path)
      exit(-1)

  elif args.clean_partition:
    if common_utils.clean_partition(dev_path):
      print("clean partition for dev_path:[%s] success" % dev_path)
      exit(0)
    else:
      print("clean partition for dev_path:[%s] failed" % dev_path)
      exit(-1)
  else:
    parser.print_help()
    exit(-1)
