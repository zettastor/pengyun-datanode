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
#       link name
#       force rebuild
#       app type, multi value

SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..

link_name="$1"
shift
force_rebuild="$1"
shift
app_type_str="$*"

if [[ "X$force_rebuild" == "Xtrue" ]]; then
    force_rebuild="--force_rebuild"
else
    force_rebuild=""
fi

# Initialize the storage env for the data node
python $ROOTPATH/bin/ReinitArchive.py --link_name $link_name $force_rebuild --type $app_type_str
