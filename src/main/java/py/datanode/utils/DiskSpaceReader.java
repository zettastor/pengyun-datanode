/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
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
