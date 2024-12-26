#!/bin/sh
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


# This program has two features.
#
# 1. Unmount a disk image.
# 2. Detach the disk image from RAM.
#
# Usage:
#   $0 <dir>
#
#   dir:
#     The `dir' is a directory, the dir is mounting a disk image.
#
# See also:
#   - hdid(8)
#
 
mount_point=$1
if [ ! -d "${mount_point}" ]; then
    echo "The mount point didn't available." >&2
    exit 1
fi
mount_point=$(cd $mount_point && pwd)
 
device_name=$(df "${mount_point}" 2>/dev/null | tail -1 | grep "${mount_point}" | cut -d' ' -f1)
if [ -z "${device_name}" ]; then
    echo "The mount point didn't mount disk image." >&2
    exit 1
fi
 
umount "${mount_point}"
if [ $? -ne 0 ]; then
    echo "Could not unmount." >&2
    exit $?
fi
 
hdiutil detach -quiet $device_name
