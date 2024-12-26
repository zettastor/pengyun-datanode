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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.archive.ArchiveStatus;
import py.archive.disklog.DiskErrorLogManager;
import py.datanode.archive.RawArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.exception.StorageException;
import py.storage.Storage;
import py.storage.StorageExceptionHandler;

/**
 * Increase the number of exceptions for an archive, and persist the error log to log file.
 */
public class StorageExceptionPersister implements StorageExceptionHandler {
  private static final Logger logger = LoggerFactory.getLogger(StorageExceptionPersister.class);
  private final DiskErrorLogManager diskErrorLogManager;
  private final RawArchiveManager rawAchiveManager;
  private final DataNodeConfiguration config;

  public StorageExceptionPersister(DiskErrorLogManager diskLog, RawArchiveManager rawAchiveManager,
      DataNodeConfiguration cfg) {
    this.diskErrorLogManager = diskLog;
    this.rawAchiveManager = rawAchiveManager;
    this.config = cfg;
  }

  @Override
  public void handle(Storage storage, StorageException exception) {
    logger.error("Caught a StorageException. Ready to persist it to error log", exception);
    if (!exception.isIoException()) {
      return;
    }

    for (Archive rawArchive : rawAchiveManager.getArchives()) {
      if (!storage.equals(rawArchive.getStorage())) {
        continue;
      }
      recordError(rawArchive, exception);
      return;
    }
    logger.warn("can not find the storage={}", storage);
  }

  private void recordError(Archive archive, StorageException exception) {
    ArchiveStatus status = archive.getArchiveMetadata().getStatus();
    if (ArchiveStatus.isEjected(status) || status == ArchiveStatus.BROKEN) {
      logger.warn("archive={} is no used, do not record", archive.getArchiveMetadata());
      return;
    }

    logger.warn("exception storage: {}", archive.getStorage());
    diskErrorLogManager.recordError(archive, exception);
    return;
  }
}
