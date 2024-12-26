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
