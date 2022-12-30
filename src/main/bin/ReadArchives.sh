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

# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..

echo $ROOTPATH

pageSize=`cat $ROOTPATH/config/storage.properties |grep "page" | tail -n 1 | cut -d "=" -f2- | awk '{print $1}'`
segSize=`cat $ROOTPATH/config/storage.properties |grep "segment" | tail -n 1 | cut -d "=" -f2- | awk '{print $1}'`

java -cp "$ROOTPATH/lib/*:$ROOTPATH/config" py.datanode.archive.ArchivesReader "var/storage/rawDisks" $pageSize $segSize
