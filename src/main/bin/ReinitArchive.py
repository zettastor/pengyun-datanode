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
import config_reader
import os
import shutil
from utils import logging_config

# reinit archive to a new archive type
# don't need to check switch, because caller permit switches are all on

logger = logging_config.get_logger()

all_supported_app_types = [Common.AppType.RAW]

parser = argparse.ArgumentParser("Reinit archive")
parser.add_argument("--link_name", action="store", required=True,
                    help="archive disk link file name")
parser.add_argument("--force_rebuild", action="store_true", default=False,
                    help="whether need force rebuild archive, default don't need")
parser.add_argument("--type", action="store", default=[],
                    nargs="+", choices=all_supported_app_types,
                    help="the new archive types, multi value means mix archive")

args = parser.parse_args()

origin_link_name = args.link_name
app_types = args.type
force_rebuild = args.force_rebuild
origin_link_path = os.path.join(config_reader.storage_folder,
                                Common.DiskSubFolder.CLEAN, origin_link_name)
realpath = os.path.realpath(origin_link_path)
raw_name = os.path.basename(realpath)

logger.info(
    "Reinit Archive origin link name:[%s] force rebuild:[%s] types:[%s].",
    origin_link_name, force_rebuild, app_types)
logger.info("origin link path:[%s] real path:[%s] raw name:[%s]",
            origin_link_path, realpath, raw_name)

if not os.path.exists(origin_link_path):
  logger.error(
      "origin unset archive link path:[%s] not exist, reinit archive failed.",
      origin_link_path)
  print("unset disk:[%s] doest exist, reinit failed" % origin_link_name)
  exit(-1)

sub_folder = Common.get_sub_folder_by_app_types(app_types)

if not force_rebuild:
  # don't need to rebuild, so we just need move the link file
  new_link_path = os.path.join(config_reader.storage_folder, sub_folder,
                               origin_link_name)
  logger.info(
      "don't need to rebuild, just move the link file from:[%s] to:[%s]",
      origin_link_path, new_link_path)

  shutil.move(origin_link_path, new_link_path)
  print(os.path.basename(new_link_path))
  exit(0)

serial_num = None
dev_name = None
matched_contents = filter(lambda content: content.get_raw_name() == raw_name,
                          Common.get_60_rule_contents())
if len(matched_contents) > 0:
  content = matched_contents[0]
  logger.info("Find content:[%s] in 60rule that match raw_name:[%s]", content,
              raw_name)
  serial_num = content.get_serial_num()
  dev_name = content.get_dev_name()
else:
  logger.error(
      "Can't find content in 60rule that match raw_name:[%s], reinit archive failed",
      raw_name)
  print(
      "Can't find content in 60rule that match link name:[%s], reinit archive failed." % origin_link_name)
  exit(-1)

dev_path = Common.get_dev_path_by_dev_name(dev_name)
dev_type = Common.get_dev_type_from_datanode(raw_name)

if Common.AppType.RAW in app_types:
  # reinit to rawDisks
  logger.info("Reinit archive to raw disk.")

  if len(app_types) > 1:
    logger.warn(
        "there is %s in command line parameter, but type count is more than 1, please check why, "
        "the count should be 1", Common.AppType.RAW)

  app_types = [Common.AppType.RAW]

new_link_name = Common.generate_link_name(raw_name, dev_type, app_types)
new_link_path = os.path.join(config_reader.storage_folder, sub_folder,
                             new_link_name)
shutil.move(origin_link_path, new_link_path)

# if it is partitioned disk, should use the origin dev_name
file_system_partition_name = None
partition_infos = filter(lambda info: info.get_raw_disk_partition() == dev_name,
                         Common.get_partition_record_contents())
if len(partition_infos) > 0:
  dev_path = Common.get_dev_path_by_dev_name(partition_infos[0].get_dev_name())
  file_system_partition_name = partition_infos[0].get_file_system_partition()

ret_code, ret_lines = Common.run_java_init_program(serial_num, dev_path,
                                                   dev_type, new_link_path,
                                                   run_in_real_time=True,
                                                   force_rebuild=True,
                                                   file_system_partition_name=file_system_partition_name)
if ret_code == 0:
  logger.info("Reinit archive for link name:[%s] app types:[%s] success",
              origin_link_name, app_types)
  print(new_link_name)
else:
  logger.error(
      "Reinit archive for link name:[%s] app types:[%s] failed, run java reinit program failed. restore link file from:[%s] to:[%s]",
      origin_link_name, app_types, new_link_path, origin_link_path)
  shutil.move(new_link_path, origin_link_path)
  exit(-1)
