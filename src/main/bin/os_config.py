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
copy from OSConfig.pl
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
