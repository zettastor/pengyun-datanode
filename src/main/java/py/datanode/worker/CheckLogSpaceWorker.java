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

package py.datanode.worker;

import java.io.File;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.utils.DiskSpaceReader;
import py.periodic.Worker;

/**
 * check if there is enough space in system disk to support writing log file(both Log4j and our <br>
 * logfile). if not, i will exit datanode .
 *
 *
 */
public class CheckLogSpaceWorker implements Worker {
  private static final Logger logger = LoggerFactory.getLogger(CheckLogSpaceWorker.class);
  private long minReservedMb;
  private File[] dirs;

  public CheckLogSpaceWorker(long minReservedMb, String[] logDirs) {
    this.minReservedMb = minReservedMb;
    this.dirs = new File[logDirs.length];
    for (int i = 0; i < logDirs.length; i++) {
      this.dirs[i] = new File(logDirs[i]);
    }

    validate(this.dirs);
  }

  public CheckLogSpaceWorker(long minReservedMb, File[] logDirs) {
    this.minReservedMb = minReservedMb;
    validate(logDirs);

    this.dirs = new File[logDirs.length];
    for (int i = 0; i < logDirs.length; i++) {
      this.dirs[i] = logDirs[i];
    }
  }

  public void validate(File[] dirs) {
    Validate.isTrue(dirs.length > 0);
    for (int i = 0; i < dirs.length; i++) {
      if (!dirs[i].exists()) {
        logger.error("directory not exist: {}", dirs[i]);
        Validate.isTrue(false);
      }
      if (!dirs[i].isDirectory()) {
        logger.error("{}: is a file not a directory", dirs[i]);
        Validate.isTrue(false);
      }
    }
  }

  @Override
  public void doWork() throws Exception {
    for (File dir : dirs) {
      DiskSpaceReader diskSpaceReader = new DiskSpaceReader(dir);
      long usable = diskSpaceReader.getUsableSpaceMb();
      logger.debug(
          "check if the usable space  more than reserved ,log dir:{},min required:{},current:{} ",
          diskSpaceReader.getAbsPath(), minReservedMb, usable);
      if (usable < minReservedMb) {
        logger.error("usable space  is too low:{}", usable);
        exit();
      }
    }
  }

  public void exit() {
    System.exit(1);
  }
}
