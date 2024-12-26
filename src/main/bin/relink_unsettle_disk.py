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
unsettle disk, disk type may not persist in disk.
when java program restart, unsettle disk still in unsettle folder,
java program can't figure out origin disk type of unsettle disk and
will fail.
this script will relink unsettle disk to its original disk type
import Common
import argparse
import os.path
from utils import logging_config

logger = logging_config.get_logger()

parser = argparse.ArgumentParser(
    description="relink unsettle disk to its original disk type folder")
parser.add_argument("link_name", action="store",
                    help="link name of unsettle disk")

args = parser.parse_args()

link_name = args.link_name

logger.info("relink unsettle disk begin, link name:[%s]", link_name)

folder_path = Common.get_folder_path_by_app_type(Common.AppType.UNSET_DISK)
link_path = os.path.join(folder_path, link_name)

if not os.path.exists(link_path):
  Common.exit_process(-1,
                      "link path:[%s] doesn't exist, relink failed." % link_path)

# get dev type
raw_path = os.path.realpath(link_path)
raw_name = os.path.basename(raw_path)
dev_type = Common.get_dev_type_from_datanode(raw_name)
logger.info("get dev type:[%s] and raw name:[%s] for link file:[%s]", dev_type,
            raw_name, link_name)

Common.force_delete_file(link_path)
if not Common.query_and_link_by_java(raw_name, dev_type):
  Common.exit_process(-1,
                      "relink failed, link name:[%s], query and link by java failed" % link_name)

logger.info("relink unsettle disk end, link name:[%s]", link_name)
