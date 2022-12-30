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

import glob
import re

from utils import logging_config

logger = logging_config.get_logger()

supported_os_version_map = {
  "Ubuntu": [12, 14, 16, 17],
  "Red Hat": [5, 6, 7],
  "CentOS": [5, 6, 7],
  "Kylin": [4, 5, 6]
}

os_info_lines = None


def is_support():
  """
  check whether support current os system
  :return:
  """
  if is_kylin():
    return True

  os_major, os_minor = get_os_version()

  if is_ubuntu():
    return os_major in get_ubuntu_supported_versions()
  elif is_redhat():
    return os_major in get_redhat_supported_versions()
  elif is_centos():
    return os_major in get_centos_supported_version()
  else:
    return False


def get_ubuntu_supported_versions():
  return supported_os_version_map["Ubuntu"]


def get_redhat_supported_versions():
  return supported_os_version_map["Red Hat"]


def get_centos_supported_version():
  return supported_os_version_map["CentOS"]


def is_ubuntu():
  if "Ubuntu" in "".join(get_os_info()):
    logger.info("The current version is Ubuntu")
    return True
  else:
    return False


def is_redhat():
  if "Red Hat" in "".join(get_os_info()):
    logger.info("The current version is Red Hat")
    return True
  else:
    return False


def is_centos():
  if "CentOS" in "".join(get_os_info()):
    logger.info("The current version is CentOS")
    return True
  else:
    return False


def is_kylin():
  if "Kylin" in "".join(get_os_info()):
    logger.info("The current version is Kylin")
    return True
  else:
    return False


def get_os_info():
  """
  copy from perl, read all content in /etc/*-release
  :return: list of content line
  """
  global os_info_lines
  if os_info_lines is None:
    os_info_lines = []
    for filepath in glob.glob("/etc/*-release"):
      with open(filepath, "r") as f:
        os_info_lines += f.readlines()

  return os_info_lines


def get_os_version():
  """
  get os version
  :return: (major version, minor version)
  """
  with open("/etc/issue", "r") as f:
    content = "".join(f.readlines())
    match_obj = re.search(r"-?(\d+)\.?(\d+)", content)
    if match_obj:
      major_version = int(match_obj.group(1))
      minor_version = int(match_obj.group(2))
      logger.info("get os version major version:[%s] minor version:[%s]",
                  major_version, minor_version)
      return major_version, minor_version

  with open("/etc/redhat-release", "r") as f:
    content = "".join(f.readlines())
    match_obj = re.search(r"\s?(\d+)\.+(\d+)", content)
    if match_obj:
      major_version = int(match_obj.group(1))
      minor_version = int(match_obj.group(2))
      logger.info("get os version major version:[%s] minor version:[%s]",
                  major_version, minor_version)
      return major_version, minor_version

  return None, None
