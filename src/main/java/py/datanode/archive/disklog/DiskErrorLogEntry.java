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

package py.datanode.archive.disklog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DiskErrorLogEntry {
  private final long archiveId;
  private final long exceptionCounter;
  private final String serialNumber;
  private String deviceName;

  @JsonCreator
  public DiskErrorLogEntry(@JsonProperty("deviceName") String deviceName,
      @JsonProperty("archiveId") long archiveId,
      @JsonProperty("exceptionCounter") long exceptionCounter,
      @JsonProperty("serialNumber") String serialNumber) {
    this.deviceName = deviceName;
    this.archiveId = archiveId;
    this.exceptionCounter = exceptionCounter;
    this.serialNumber = serialNumber;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public String getDeviceName() {
    return deviceName;
  }

  public void setDeviceName(String deviceName) {
    this.deviceName = deviceName;
  }

  public long getArchiveId() {
    return archiveId;
  }

  public long getExceptionCounter() {
    return exceptionCounter;
  }

  @Override
  public String toString() {
    return "DiskErrorLogEntry{" + "archiveId=" + archiveId + ", exceptionCounter="
        + exceptionCounter
        + ", serialNumber='" + serialNumber + ", deviceName='" + deviceName + '}';
  }
}
