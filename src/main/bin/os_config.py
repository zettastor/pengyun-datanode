# -*- encoding=utf-8 -*-
"""
copy from OSConfig.pl
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
import os_version
import time
from utils import logging_config
from utils.common import exit_process

logger = logging_config.get_logger()

if os_version.is_ubuntu():
  ret_code, ret_line = Common.run_cmd("modprobe raw")
  if ret_code != 0:
    exit_process(-1, "fail to modprobe raw module to the kernel")

  logger.info("modprobe raw module is loaded to the kernel")
  time.sleep(2)

ret_code, ret_line = Common.run_cmd(
    "echo 67107840 > /proc/sys/vm/max_map_count")
if ret_code != 0:
  exit_process(-1, "fail to change max_map_count")
