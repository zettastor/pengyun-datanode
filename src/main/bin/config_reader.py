# -*- encoding=utf-8 -*-
"""
read config
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

import ConfigParser
import StringIO
import common_utils
import config_location
import os
from common_utils import MB_SIZE
from utils import logging_config

# can't use this project_folder, because bin folder is linked to other place
# script_folder = sys.path[0]
# project_folder = os.path.dirname(script_folder)

logger = logging_config.get_logger()

project_folder = os.getcwd()
storage_folder = os.path.join(project_folder, "var", "storage")
system_raw_folder = "/dev/raw"
system_dev_folder = "/dev"
raw_disk_base_name = "raw"

java_classpath = ":{project_folder}/build/jar/*:{project_folder}/lib/*:{project_folder}/config".format(
    project_folder=project_folder)

six_zero_rule_file_path = os.path.join(storage_folder, "60-raw.rules")
ssd_record_file_path = os.path.join(storage_folder, "ssd.record")
datanode_start_time_file_path = os.path.join(storage_folder,
                                             "DataNodeStartTime")
rollback_file_path = os.path.join(storage_folder, "rollback.record")
partition_folder_path = os.path.join(storage_folder, "partition")
partition_record_file_path = os.path.join(storage_folder, "partition.json")

config_folder = os.path.join(project_folder, "config")
# config_folder = os.path.join(project_folder, "resources", "config")

default_section = "root"
config_files = list(set(config_location.config_location.itervalues()))
config_parsers = {}

for filename in config_files:
  config_parser = ConfigParser.ConfigParser()
  with open(os.path.join(config_folder, filename), "r") as f:
    section_str = "[%s]\n" % default_section
    config_parser.readfp(StringIO.StringIO(section_str + f.read()))
    config_parsers[filename] = config_parser


def get_config(func, name, default_value=None):
  if name in config_location.config_location:
    config_file_name = config_location.config_location[name]
    config_parser = config_parsers[config_file_name]
    if config_parser.has_option(default_section, name):
      value = func(config_parser, default_section, name)
      logger.debug("Read config:[%s] value:[%s] from file:[%s].", name, value,
                   config_file_name)
      return value
    else:
      logger.error(
          "Read config:[%s] failed, return default value:[%s], because can't find config in file:[%s]",
          name, default_value, config_file_name)
      return default_value
  else:
    logger.error("Read config:[%s] failed, return default value:[%s], "
                 "because it is not in config location definition, please check, this is big problem",
                 name, default_value)
    return default_value


def get_str_config(name, default_value=None):
  return get_config(func=ConfigParser.ConfigParser.get, name=name,
                    default_value=default_value)


def get_boolean_config(name, default_value=None):
  return get_config(func=ConfigParser.ConfigParser.getboolean, name=name,
                    default_value=default_value)


def get_int_config(name, default_value=None):
  return get_config(func=ConfigParser.ConfigParser.getint, name=name,
                    default_value=default_value)


def get_array_config(name, separator=","):
  value = get_str_config(name, "")

  array = []
  if value:
    array = value.strip().split(separator)

  logger.info(
      "read arrar config, name:[%s] value:[%s] separator:[%s] result:[%s]",
      name,
      value, separator, array)
  return array


def get_raw_disk_link_name_pattern():
  return ""


def get_unset_disk_link_name_pattern():
  return ""


def get_raw_app_name():
  return get_str_config("raw.app.name", "RAW")


def get_clean_archive_directory():
  return get_str_config("unset.archive.directory", "unsetDisks")


def get_pcie_regs():
  return get_array_config("pcie.device.name.reg", ",")


def get_auto_distribute_disk_usage_switch():
  return get_boolean_config("enable.auto.distribute.disk.usage", True)


def get_archive_init_mode():
  return get_str_config("archive.init.mode", "append")


def get_ignore_device_regs():
  return get_array_config("ignore.device.name.reg", ",")


def get_ssd_reg_array():
  return get_array_config("ssd.device.name.reg", ",")


def need_detect_ssd_by_speed():
  return get_boolean_config("ssd.detect.by.speed", True)


def get_ssd_min_threshold():
  return get_int_config("ssd.speed.min.threshold", 1000)


def get_archive_plugin_matcher():
  return get_str_config("archive.plugin.matcher",
                        "plugin rawName %s, archiveType %s")


def get_rocksdb_partition_size():
  default_value = "50G"
  size_str = get_str_config("rocksdb.partition.size", "50G")
  bytes_size = common_utils.convert_size_str_to_byte_size(size_str)
  if bytes_size is None or bytes_size == 0:
    logger.error(
        "rocksdb.partition.size config value:[%s] is not a size value, please check",
        size_str)
    return default_value
  else:
    return size_str


def get_mkfs_cmd():
  return get_str_config("mkfs.cmd", "mkfs.ext4")


def need_partition_ssd_disk():
  return get_boolean_config("need.partition.ssd.disk", False)


def get_path_environment():
  return get_array_config("path.environment", ",")


def get_rocksdb_default_folder_name():
  return get_str_config("rocksdb.default.folder", "default")


def get_rocksdb_default_folder_path():
  folder_name = get_rocksdb_default_folder_name()
  return os.path.join(partition_folder_path, folder_name)


def need_disable_disk_native_cache():
  return not get_boolean_config("enable.disk.native.cache", False)


def is_on_vms():
  return get_boolean_config("on.vms", False)


def need_check_os_version():
  return get_boolean_config("need.check.os.version", True)


def get_page_size():
  return get_int_config("page.size.byte", 8192)


def get_cache_bucket_metadata_size():
  return get_int_config("cache.bucket.metadata.size", 64)


def get_cache_metadata_chunk_size():
  return get_int_config("cache.metadata.chunk.size", 4096)


def get_segment_size():
  return get_int_config("segment.size.byte", 1073741824)


def get_flexible_count_limit_in_one_archive():
  return get_int_config("flexible.count.limit.in.one.archive", 500)
