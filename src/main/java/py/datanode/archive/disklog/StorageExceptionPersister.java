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
