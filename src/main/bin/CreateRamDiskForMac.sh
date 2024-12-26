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


# This program has two feature.
#
# 1. Create a disk image on RAM.
# 2. Mount that disk image.
#
# Usage:
#   $0 <dir> <size>
#
#   size:
#     The `size' is a size of disk image (MB). At the end of 'size', there should be a 'M'. For instance, 16M
#
#   dir:
#     The `dir' is a directory, the dir is used to mount the disk image.
#
# See also:
#   - hdid(8)
#
 
mount_point=${1}
size=${2%m}
size=${size%M}

mkdir -p $mount_point
if [ $? -ne 0 ]; then
    echo "The mount point didn't available." >&2
    exit $?
fi
 
# make the number of sectors a bit more than the requested, since the file system needs some spaces
# additional space is 20M
real_size=$(expr $size + 20)
sector=$(expr $real_size \* 1024 \* 1024 / 512)
echo ${sector}
device_name=$(hdid -nomount "ram://${sector}" | awk '{print $1}')
if [ $? -ne 0 ]; then
    echo "Could not create disk image." >&2
    exit $?
fi
 
newfs_hfs $device_name > /dev/null
if [ $? -ne 0 ]; then
    echo "Could not format disk image." >&2
    exit $?
fi
 
mount -t hfs $device_name $mount_point
if [ $? -ne 0 ]; then
    echo "Could not mount disk image." >&2
    exit $?
fi
#echo "remove with:"  
#echo "umount ${mount_point}"  
#echo "diskutil eject ${device_name}"  
