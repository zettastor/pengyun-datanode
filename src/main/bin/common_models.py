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

class SixRuleContent:
  """
  content from 60-rule file
  """

  def __init__(self, dev_name, serial_num, raw_name):
    self.dev_name = dev_name
    self.serial_num = serial_num
    self.raw_name = raw_name

  def get_dev_name(self):
    return self.dev_name

  def get_serial_num(self):
    return self.serial_num

  def get_raw_name(self):
    return self.raw_name

  def __repr__(self):
    return "SixRuleContent{dev_name=%s, serial_num=%s, raw_name=%s}" % (
      self.dev_name, self.serial_num, self.raw_name)


class SSDRecordContent:
  """
  content from ssd.record file
  """

  def __init__(self, raw_name, dev_name):
    self.raw_name = raw_name
    self.dev_name = dev_name

  def get_raw_name(self):
    return self.raw_name

  def get_dev_name(self):
    return self.dev_name

  def __repr__(self):
    return "SSDRecordContent{raw_name:%s, dev_name=%s}" % (
      self.raw_name, self.dev_name)


class AppTypeInfo:

  def __init__(self, app_type, link_name_pattern, sub_folder, folder_path,
      min_disk_size_limit, switch):
    self.app_type = app_type
    self.link_name_pattern = link_name_pattern
    self.sub_folder = sub_folder
    self.folder_path = folder_path
    self.min_disk_size_limit = min_disk_size_limit
    self.switch = switch

  def __repr__(self):
    return "AppTypeInfo{app_type:%s, link_name_pattern=%s, sub_folder=%s, folder_path=%s, min_disk_size_limit=%s, switch=%s}" \
           % (self.app_type, self.link_name_pattern, self.sub_folder,
              self.folder_path, self.min_disk_size_limit, self.switch)

  def __eq__(self, other):
    if type(self) is type(other):
      return self.__dict__ == other.__dict__
    else:
      return False

  def __ne__(self, other):
    return not self.__eq__(other)

  def get_app_type(self):
    if self.app_type is None:
      raise NotImplementedError(
          "app_type is None, this should not happen, please check, self:[%s]",
          self)

    return self.app_type

  def get_link_name_pattern(self):
    if self.link_name_pattern is None:
      raise NotImplementedError(
          "link_name_pattern is None, this should not happen, please check, self:[%s]",
          self)

    return self.link_name_pattern

  def get_sub_folder(self):
    if self.sub_folder is None:
      raise NotImplementedError(
          "sub_folder is None, this should not happen, please check, self:[%s]",
          self)

    return self.sub_folder

  def get_folder_path(self):
    if self.folder_path is None:
      raise NotImplementedError(
          "folder_path is None, this should not happen, please check, self:[%s]",
          self)

    return self.folder_path

  def get_min_disk_size_limit(self):
    if self.min_disk_size_limit is None:
      raise NotImplementedError(
          "min_disk_size_limit is None, this should not happen, please check, self:[%s]",
          self)

    return self.min_disk_size_limit

  def get_switch(self):
    if self.switch is None:
      raise NotImplementedError(
          "switch is None, this should not happen, please check, self:[%s]",
          self)

    return self.switch


class DevInfo:
  """
  device info
  """

  def __init__(self, dev_name, raw_name, dev_type, disk_size, serial_num):
    self.dev_name = dev_name
    self.raw_name = raw_name
    self.dev_type = dev_type
    self.disk_size = disk_size
    self.serial_num = serial_num

  def __eq__(self, other):
    if type(self) is type(other):
      return self.__dict__ == other.__dict__
    else:
      return False

  def __ne__(self, other):
    return not self.__eq__(other)

  def get_dev_name(self):
    return self.dev_name

  def get_raw_name(self):
    return self.raw_name

  def get_dev_type(self):
    return self.dev_type

  def get_disk_size(self):
    return self.disk_size

  def get_serial_num(self):
    return self.serial_num

  def __repr__(self):
    return "DevInfo{dev_name=%s, raw_name=%s, dev_type=%s, disk_size=%s, serial_num=%s}" % (
      self.dev_name, self.raw_name, self.dev_type, self.disk_size,
      self.serial_num)


class PartitionInfo:
  """
  datanode partition info
  """

  def __init__(self, dev_name, file_system_partition, raw_disk_partition):
    self.dev_name = dev_name
    self.file_system_partition = file_system_partition
    self.raw_disk_partition = raw_disk_partition

  @classmethod
  def from_dict(cls, d):
    return PartitionInfo(d["dev_name"], d["file_system_partition"],
                         d["raw_disk_partition"])

  def to_dict(self):
    return self.__dict__.copy()

  def __eq__(self, other):
    if type(self) is type(other):
      return self.__dict__ == other.__dict__
    else:
      return False

  def __ne__(self, other):
    return not self.__eq__(other)

  def __hash__(self):
    return hash(self.dev_name) ^ hash(self.file_system_partition) ^ hash(
        self.raw_disk_partition)

  def __lt__(self, other):
    return self.dev_name < other.dev_name

  def get_dev_name(self):
    return self.dev_name

  def get_file_system_partition(self):
    """
    :return: like sda1
    """
    return self.file_system_partition

  def get_raw_disk_partition(self):
    """
    :return: like sda2
    """
    return self.raw_disk_partition

  def __repr__(self):
    return "PartitionInfo{dev_name=%s, file_system_partition=%s, raw_disk_partition=%s}" % (
      self.dev_name, self.file_system_partition, self.raw_disk_partition)
