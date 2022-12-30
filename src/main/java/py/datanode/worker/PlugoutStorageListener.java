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

package py.datanode.worker;

import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyException;
import net.contentobjects.jnotify.JNotifyListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.archive.PluginPlugoutManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.exception.ArchiveNotFoundException;
import py.exception.ArchiveTypeNotSupportException;
import py.exception.StorageException;
import py.storage.Storage;
import py.storage.impl.AsynchronousFileChannelStorageFactory;

public class PlugoutStorageListener implements JNotifyListener {
  private static final Logger logger = LoggerFactory.getLogger(PlugoutStorageListener.class);
  private final Archive archive;
  private final DataNodeConfiguration cfg;
  private PluginPlugoutManager pluginPlugoutManager;
  private int watchId;

  public PlugoutStorageListener(Archive archive, PluginPlugoutManager pluginPlugoutManager,
      DataNodeConfiguration cfg) {
    this.archive = archive;
    this.pluginPlugoutManager = pluginPlugoutManager;
    this.cfg = cfg;
  }

  public int getWatchId() {
    return watchId;
  }

  public void setWatchId(int watchId) {
    this.watchId = watchId;
  }

  @Override
  public void fileCreated(int i, String s, String s1) {
    logger.warn("file created, i={}, s={}, s1={}", i, s, s1);
  }

  @Override
  public void fileDeleted(int i, String s, String s1) {
    logger.warn("file deleted, i={}, s={}, s1={}", i, s, s1);
    try {
      Storage storage = archive.getStorage();

      String cmd = cfg.getArchiveConfiguration().getPlugOutArchiveScriptPath() + " " + archive
          .getArchiveMetadata().getSerialNumber();
      logger.warn("execute cmd {} to check whether all the disks in data node is changed", cmd);

      logger.warn("plug out the archive {}", archive);
      try {
        pluginPlugoutManager.plugout(archive.getArchiveMetadata().getDeviceName(),
            archive.getArchiveMetadata().getSerialNumber());
        removeListener(watchId);
      } catch (ArchiveTypeNotSupportException e) {
        logger.warn("plug out archive error", e);
      } catch (ArchiveNotFoundException e) {
        logger.warn("plug out archive error", e);
      } catch (Exception e) {
        logger.warn("caught an exception when plug out archive={}", archive, e);
      }

      try {
        storage.close();
        AsynchronousFileChannelStorageFactory.getInstance().close(archive.getStorage());
      } catch (StorageException e) {
        logger.warn("the archive {} close faild", archive);
      }

      try {
        Process pid = Runtime.getRuntime().exec(cmd);
      } catch (Exception e) {
        logger.warn("caught an exception, {}", e.getCause());
      }
    } catch (Exception e) {
      logger.warn("catch an exception ", e);
    }
  }

  @Override
  public void fileModified(int i, String s, String s1) {
    logger.warn("file modify, i={}, s={}, s1={}", i, s, s1);
  }

  @Override
  public void fileRenamed(int i, String s, String s1, String s2) {
    logger.warn("file rename, i={}, s={}, s1={}", i, s, s1);
  }

  public void removeListener(int watchId) {
    try {
      JNotify.removeWatch(watchId);
    } catch (JNotifyException e) {
      logger.warn("caught an exception when removing the watch for watch id={}", watchId, e);
    }
  }

}
