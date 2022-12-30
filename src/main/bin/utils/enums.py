# -*- encoding=utf-8 -*-
"""
some common enums
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

class DevType:
  PCIE = "pcie"
  SSD = "ssd"
  SATA = "sata"

  @classmethod
  def get_fast_types(cls):
    return [DevType.PCIE, DevType.SSD]

  @classmethod
  def is_fast_type(cls, dev_type):
    return dev_type in cls.get_fast_types()

  def __init__(self):
    pass
