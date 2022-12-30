# -*- encoding=utf-8 -*-
"""
when plugout one disk, use this script to unraw disk and delete disk info in datanode
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
import argparse
import config_reader
import os
from utils import logging_config
from utils.common import exit_process

FOUND_FAILED_EXIT_CODE = 3

logger = logging_config.get_logger()

if "datanode" not in os.path.basename(config_reader.project_folder):
  exit_process(-1, "This script must be run from the datanode environment.")

parser = argparse.ArgumentParser(description="plugout disk")
parser.add_argument("--serial_num", metavar="serial num", action="store",
                    required=True,
                    help="disk serial num which need to plugout")

args = parser.parse_args()

serial_num = args.serial_num
logger.info("plugout disk begin, serial num:[%s]", serial_num)

contents = filter(lambda content: content.get_serial_num() == serial_num,
                  Common.get_60_rule_contents(reload=True))
if len(contents) <= 0:
  logger.error("can't find raw name by serial num:[%s], plugout disk failed",
               serial_num)
  exit(FOUND_FAILED_EXIT_CODE)

# should only find one line, but just iter the contents
logger.info("find rule contents:[%s] by serial num:[%s]", contents, serial_num)

raw_names = [content.get_raw_name() for content in contents]
dev_names = [content.get_dev_name() for content in contents]

Common.unlink_disk_in_datanode(raw_names)
Common.remove_disk_info_from_datanode_info_file(raw_names, dev_names)
Common.get_60_rule_contents(reload=True)
Common.get_ssd_record_contents(reload=True)

for raw_name in raw_names:
  Common.unraw_raw_disk(raw_name)

Common.remove_partition_info_and_umount(dev_names)
Common.get_partition_record_contents(reload=True)

logger.info("plugout disk end, serial num:[%s]", serial_num)
exit(0)
