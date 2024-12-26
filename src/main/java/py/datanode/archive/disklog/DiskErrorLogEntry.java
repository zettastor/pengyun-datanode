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
