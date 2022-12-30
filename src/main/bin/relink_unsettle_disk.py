# -*- encoding=utf-8 -*-
"""
unsettle disk, disk type may not persist in disk.
when java program restart, unsettle disk still in unsettle folder,
java program can't figure out origin disk type of unsettle disk and
will fail.
this script will relink unsettle disk to its original disk type
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
