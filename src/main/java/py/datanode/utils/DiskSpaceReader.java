/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.datanode.utils;

import java.io.File;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskSpaceReader {
  private static final Logger logger = LoggerFactory.getLogger(DiskSpaceReader.class);

  private File dir;

  public DiskSpaceReader(File file) {
    Validate.isTrue(file.exists() && file.isDirectory());
    dir = file;
  }

  public String getAbsPath() {
    return dir.getAbsolutePath();
  }

  public long getTotalSpaceByte() {
    return dir.getTotalSpace();
  }

  public long getUsableSpaceByte() {
    return dir.getUsableSpace();
  }

  public long getUsableSpaceMb() {
    return getUsableSpaceByte() / 1024 / 1024;
  }

  public double getUsableSpaceRatio() {
    return (double) dir.getUsableSpace() / dir.getTotalSpace();
  }

}
