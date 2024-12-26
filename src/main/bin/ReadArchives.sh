#!/bin/bash
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

# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(cd "$(dirname "$0")"; pwd)
ROOTPATH=$SCRIPTPATH/..

echo $ROOTPATH

pageSize=`cat $ROOTPATH/config/storage.properties |grep "page" | tail -n 1 | cut -d "=" -f2- | awk '{print $1}'`
segSize=`cat $ROOTPATH/config/storage.properties |grep "segment" | tail -n 1 | cut -d "=" -f2- | awk '{print $1}'`

java -cp "$ROOTPATH/lib/*:$ROOTPATH/config" py.datanode.archive.ArchivesReader "var/storage/rawDisks" $pageSize $segSize
