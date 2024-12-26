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
