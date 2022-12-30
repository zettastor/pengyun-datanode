#!/bin/bash
#
# Copyright (c) 2022. PengYunNetWork
#
# This program is free software: you can use, redistribute, and/or modify it
# under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
#  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
#  You should have received a copy of the GNU Affero General Public License along with
#  this program. If not, see <http://www.gnu.org/licenses/>.
#

# param:
#       serial num

SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..

serial_num="$1"

# Initialize the storage env for the data node
python $ROOTPATH/bin/plugout_archive.py --serial_num $serial_num