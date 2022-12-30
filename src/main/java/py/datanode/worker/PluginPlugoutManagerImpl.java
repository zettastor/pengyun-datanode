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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyException;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.archive.ArchiveType;
import py.archive.PluginPlugoutManager;
import py.datanode.archive.RawArchiveManager;
import py.datanode.archive.UnsettledArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.engine.DelayedTask;
import py.engine.DelayedTaskEngine;
import py.engine.Result;
import py.engine.ResultImpl;
import py.exception.ArchiveIsNotCleanedException;
import py.exception.ArchiveNotFoundException;
import py.exception.ArchiveStatusException;
import py.exception.ArchiveTypeNotSupportException;
import py.exception.InvalidInputException;
import py.exception.JnotifyAddListerException;
import py.thrift.share.ArchiveIsUsingExceptionThrift;

public class PluginPlugoutManagerImpl implements PluginPlugoutManager {
  private static final Logger logger = LoggerFactory.getLogger(PluginPlugoutManagerImpl.class);
  private static int PLUG_IN_DELAY_TIME = 5000;
  private final Map<Long, PlugoutStorageListener> mapArchiveToListener;
  private final DataNodeConfiguration cfg;
  private final DelayedTaskEngine plugInOutEngine;
  private RawArchiveManager rawArchiveManager;
  private UnsettledArchiveManager unsettledArchiveManager;
  private Map<String, DelayedTask> mapPlugInOutTask;

  public PluginPlugoutManagerImpl(DataNodeConfiguration cfg) {
    this.mapArchiveToListener = new ConcurrentHashMap<>();
    mapPlugInOutTask = new ConcurrentHashMap<>();
    this.cfg = cfg;

    DelayedTaskEngine delayedTaskEngine = new DelayedTaskEngine();
    delayedTaskEngine.setPrefix("Plug in and Plug out the archive");
    delayedTaskEngine.setDaemon(true);
    plugInOutEngine = delayedTaskEngine;
  }

  public void setRawArchiveManager(RawArchiveManager rawArchiveManager) {
    this.rawArchiveManager = rawArchiveManager;
  }

  public UnsettledArchiveManager getUnsettledArchiveManager() {
    return unsettledArchiveManager;
  }

  public void setUnsettledArchiveManager(UnsettledArchiveManager unsettledArchiveManager) {
    this.unsettledArchiveManager = unsettledArchiveManager;
  }

  public void init() throws JnotifyAddListerException {
    if (rawArchiveManager == null) {
      logger.warn("there is no raw archive manager");
    } else {
      addListener(rawArchiveManager);
    }

    if (unsettledArchiveManager == null) {
      logger.warn("there is not unsettled archive");
    } else {
      addListener(unsettledArchiveManager);
    }
    plugInOutEngine.start();
  }

  public void addListener(Archive archive, PluginPlugoutManager manager)
      throws JnotifyAddListerException {
    String devName = archive.getArchiveMetadata().getDeviceName();
    logger.warn("add listener for archive={}", archive.getArchiveMetadata());

    PlugoutStorageListener listener = new PlugoutStorageListener(archive, manager, cfg) {
      @Override
      public void removeListener(int watchId) {
        super.removeListener(watchId);
        if (mapArchiveToListener.remove(archive.getArchiveMetadata().getArchiveId()) == null) {
          logger.warn("can not remove the listener of archive={}", archive);
        }
      }
    };

    try {
      int watchId = JNotify.addWatch(devName, JNotify.FILE_DELETED, false, listener);
      listener.setWatchId(watchId);
    } catch (JNotifyException e) {
      logger.warn("caught an exception", e);
      throw new JnotifyAddListerException();
    }
    mapArchiveToListener.put(archive.getArchiveMetadata().getArchiveId(), listener);
  }

  public void addListener(PluginPlugoutManager manager) throws JnotifyAddListerException {
    for (Archive archive : manager.getArchives()) {
      String devName = archive.getArchiveMetadata().getDeviceName();
      if (devName == null) {
        logger.error("devName is null");
        return;
      }
      logger.warn("add listener for archive={}", archive.getArchiveMetadata());

      PlugoutStorageListener listener = new PlugoutStorageListener(archive, manager, cfg) {
        @Override
        public void removeListener(int watchId) {
          super.removeListener(watchId);
          if (mapArchiveToListener.remove(archive.getArchiveMetadata().getArchiveId()) == null) {
            logger.warn("can not remove the listener of archive={}", archive);
          }
        }
      };

      try {
        int watchId = JNotify.addWatch(devName, JNotify.FILE_DELETED, false, listener);
        listener.setWatchId(watchId);
      } catch (JNotifyException e) {
        logger.warn("caught an exception", e);
        throw new JnotifyAddListerException();
      }
      mapArchiveToListener.put(archive.getArchiveMetadata().getArchiveId(), listener);
    }
  }

  public Map<Long, PlugoutStorageListener> getMapArchiveToListener() {
    return mapArchiveToListener;
  }

  @Override
  public void plugin(Archive archive)
      throws ArchiveTypeNotSupportException, InvalidInputException, ArchiveIsUsingExceptionThrift,
      ArchiveStatusException, JnotifyAddListerException {
    PluginPlugoutManager manager = null;
    if (archive.getArchiveMetadata().getArchiveType() == ArchiveType.RAW_DISK) {
      manager = rawArchiveManager;
    } else if (archive.getArchiveMetadata().getArchiveType() == ArchiveType.UNSETTLED_DISK) {
      manager = unsettledArchiveManager;
    } else {
      throw new NotImplementedException("not support the archive=" + archive);
    }
    PluginTask task = new PluginTask(0, archive, manager);
    plugInOutEngine.drive(task);
  }

  private PluginPlugoutManager getManager(String serialNumber) {
    if (find(rawArchiveManager, serialNumber)) {
      return rawArchiveManager;
    }
    return null;
  }

  private boolean find(PluginPlugoutManager pluginPlugoutManager, String serialNumber) {
    if (pluginPlugoutManager == null) {
      logger.warn("there is no such manager", new Exception());
      return false;
    }

    for (Archive archive : pluginPlugoutManager.getArchives()) {
      if (archive.getArchiveMetadata().getSerialNumber().equals(serialNumber)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Archive plugout(String devName, String serialNumber)
      throws ArchiveTypeNotSupportException, ArchiveNotFoundException, InterruptedException {
    PluginPlugoutManager manager = getManager(serialNumber);
    if (manager == null) {
      logger.warn("already not exist: {},{} in all managers", devName, serialNumber);
      throw new ArchiveNotFoundException();
    }
    PlugoutTask task = new PlugoutTask(0, serialNumber, devName, manager);
    plugInOutEngine.drive(task);
    return null;
  }

  @Override
  public void hasPlugoutFinished(Archive archive) throws ArchiveIsNotCleanedException {
    throw new NotImplementedException("archive=" + archive);
  }

  @Override
  public List<Archive> getArchives() {
    List<Archive> archives = new ArrayList<>();
    if (rawArchiveManager != null) {
      archives.addAll(rawArchiveManager.getArchives());
    }
    return archives;
  }

  @Override
  public Archive getArchive(long archiveId) {
    throw new NotImplementedException("get archvie by id not implemented by PluginPlugoutManager");
  }

  private class PluginTask extends DelayedTask {
    private final Archive archive;
    private final PluginPlugoutManager manager;

    public PluginTask(int delayMs, Archive archive, PluginPlugoutManager manager) {
      super(delayMs);
      this.archive = archive;
      this.manager = manager;
    }

    @Override
    public Result work() {
      try {
        DelayedTask delayedTask = mapPlugInOutTask
            .get(archive.getArchiveMetadata().getSerialNumber());
        if (delayedTask != null) {
          if (delayedTask instanceof PlugoutTask) {
            logger.warn("i have an plug out task");
            mapPlugInOutTask.remove(archive.getArchiveMetadata().getSerialNumber());
            return ResultImpl.DEFAULT;
          }
        } else {
          mapPlugInOutTask.put(archive.getArchiveMetadata().getSerialNumber(), this);
        }
        unsettledArchiveManager.removeArchive(archive.getArchiveMetadata().getArchiveId());
        manager.plugin(archive);
        PlugoutStorageListener plugoutStorageListener = null;
        if (null != (plugoutStorageListener = mapArchiveToListener
            .get(archive.getArchiveMetadata().getArchiveId()))) {
          plugoutStorageListener.removeListener(plugoutStorageListener.getWatchId());
        }
        addListener(archive, manager);
      } catch (ArchiveIsNotCleanedException del) {
        this.updateDelay(PLUG_IN_DELAY_TIME);
        plugInOutEngine.drive(this);
        logger.warn("archive is deleting {}", archive);
      } catch (Exception e) {
       
        mapPlugInOutTask.remove(archive.getArchiveMetadata().getSerialNumber());
        logger.warn("plug in the archive {} error", archive, e);
      }
      return ResultImpl.DEFAULT;
    }
  }

  private class PlugoutTask extends DelayedTask {
    private final String serialNumber;
    private final String devName;
    private final PluginPlugoutManager manager;

    public PlugoutTask(int delayMs, String serialNumber, String devName,
        PluginPlugoutManager manager) {
      super(delayMs);
      this.serialNumber = serialNumber;
      this.devName = devName;
      this.manager = manager;
    }

    @Override
    public Result work() {
      try {
        DelayedTask delayedTask = mapPlugInOutTask.get(serialNumber);
        if (delayedTask != null) {
          mapPlugInOutTask.remove(serialNumber);
          return ResultImpl.DEFAULT;
        }
        try {
          Archive archive = manager.plugout(devName, serialNumber);
          if (archive == null) {
            logger.warn("fail to plugout the archive: {},{} from {}", devName, serialNumber,
                manager.getClass().getSimpleName());
            return ResultImpl.DEFAULT;
          }

          String identifier = archive.getStorage().identifier();
          try {
           
            File file = new File(identifier);
            logger.warn("delete the file={},archive={}", identifier, archive);
            if (file.exists() && !file.delete()) {
              logger.warn("can't delete file: {}", identifier);
            }
          } catch (Exception e) {
            logger.warn("delete link file:{} failure", identifier, e);
            throw e;
          }

        } catch (Exception e) {
          logger.warn("Plug Out Task error ", e);
        }
      } finally {
        return ResultImpl.DEFAULT;
      }
    }
  }

}
