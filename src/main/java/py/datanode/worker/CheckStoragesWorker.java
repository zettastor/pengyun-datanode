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

import io.netty.util.HashedWheelTimer;
import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractArchiveBuilder;
import py.archive.Archive;
import py.archive.ArchiveOptions;
import py.archive.ArchiveType;
import py.archive.PluginPlugoutManager;
import py.datanode.archive.ArchiveUtils;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.DataNodeConfigurationUtils;
import py.exception.NotSupportedException;
import py.exception.StorageException;
import py.periodic.Worker;
import py.storage.Storage;
import py.storage.StorageExceptionHandlerChain;
import py.storage.impl.AsyncStorage;
import py.storage.impl.AsynchronousFileChannelStorageFactory;
import py.storage.impl.PriorityStorageImpl;

/**
 * check broken disks periodic if it is removed. if so, i will remove disks information.
 *
 *
 */
public class CheckStoragesWorker implements Worker {
  private static final Logger logger = LoggerFactory.getLogger(CheckStoragesWorker.class);
  private static Pattern pluginPattern;
  private static Pattern plugoutPattern;
  private static Pattern ejectedPattern;

  private final StorageExceptionHandlerChain storageExceptionHandlerChain;
  private final DataNodeConfiguration cfg;
  private final PluginPlugoutManager pluginPlugoutManager;
  private final CheckStoragesWorkerFactory.ProcessWaiter processWaiter;
  private final HashedWheelTimer hashedWheelTimer;

  public CheckStoragesWorker(PluginPlugoutManager pluginPlugoutManager,
      CheckStoragesWorkerFactory.ProcessWaiter processWaiter, DataNodeConfiguration cfg,
      StorageExceptionHandlerChain storageExceptionHandlerChain,
      HashedWheelTimer hashedWheelTimer) {
    this.pluginPlugoutManager = pluginPlugoutManager;
    this.cfg = cfg;
    this.processWaiter = processWaiter;
    this.storageExceptionHandlerChain = storageExceptionHandlerChain;
    this.hashedWheelTimer = hashedWheelTimer;
  }

  private boolean checkPlugin(String line) {
    if (pluginPattern == null) {
      String pattern = cfg.getArchiveConfiguration().getArchivePluginMatcher()
          .replace("%s", "(.+)");
      logger.warn("plugin pattern={}", pattern);
      pluginPattern = Pattern.compile(pattern);
    }

    Matcher matcher = pluginPattern.matcher(line);
    boolean matcherResult = matcher.matches();
    if (!matcherResult) {
      return false;
    }

    if (matcher.groupCount() != 2) {
      throw new IllegalArgumentException("group count=" + matcher.groupCount());
    }

    String linkName = matcher.group(1);
    String dirName = matcher.group(2);

    File file = new File(cfg.getArchiveConfiguration().getPersistRootDir(), dirName);
    file = new File(file, linkName);
    logger.warn("plugin: link name={}, dir name={}, full path={}", linkName, dirName,
        file.getAbsolutePath());
    Storage newStorage;
    try {
      newStorage = new PriorityStorageImpl(
          (AsyncStorage) AsynchronousFileChannelStorageFactory.getInstance()
              .generate(file.getAbsolutePath()),
          cfg.getWriteTokenCount(), storageExceptionHandlerChain, hashedWheelTimer,
          cfg.getStorageIoTimeout(), (int) ArchiveOptions.PAGE_PHYSICAL_SIZE);
      newStorage.open();
    } catch (Exception e) {
      logger.warn("caught an exception when try to open new storage:{}", file.getAbsolutePath(), e);
      return matcherResult;
    }

    ArchiveType archiveType;
    try {
      archiveType = DataNodeConfigurationUtils.getArchiveTypeByDirName(cfg, dirName);
    } catch (NotSupportedException e) {
      logger.warn("caught an exception", e);
      return matcherResult;
    }

    AbstractArchiveBuilder builder = ArchiveUtils.getArchiveBuilder(cfg, newStorage, archiveType);
    Archive archive = null;
    try {
      archive = builder.build();
      pluginPlugoutManager.plugin(archive);
    } catch (Exception e) {
      logger.error("the archive={} can not be managed", archive, e);
    }

    return matcherResult;
  }

  private boolean checkPlugout(String line) {
    if (plugoutPattern == null) {
      String pattern = cfg.getArchiveConfiguration().getArchivePlugoutMatcher()
          .replace("%s", "(.+)");
      logger.warn("plugout pattern={}", pattern);
      plugoutPattern = Pattern.compile(pattern);
    }

    Matcher matcher = plugoutPattern.matcher(line);
    boolean matcherResult = matcher.matches();
    if (matcherResult) {
      if (matcher.groupCount() != 2) {
        throw new IllegalArgumentException("group count=" + matcher.groupCount());
      }
      String devName = matcher.group(1);
      String serialNumber = matcher.group(2);

      logger.warn("plugout: dev name={}, serialNumber={}", devName, serialNumber);
      Archive archive = null;
      try {
        pluginPlugoutManager.plugout(devName, serialNumber);
      } catch (Exception e) {
        logger.error("the archive={} can not be managed", archive, e);
      }
    }

    return matcherResult;
  }

  @Override
  public void doWork() throws Exception {
    try {
     
     
     
     

      String cmd = cfg.getArchiveConfiguration().getCheckArchiveScriptPath();
      logger.info("execute cmd {} to check whether all the disks in data node is changed", cmd);
      Process pid;
      try {
        pid = Runtime.getRuntime().exec(cmd);
      } catch (Exception e) {
        logger.warn("caught an exception, {}", e.getCause());
        return;
      }

      List<String> results = processWaiter.wait(pid);
      for (String line : results) {
        logger.warn("current line: {}", line);
        if (checkPlugin(line)) {
          continue;
        }
      }
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }

  private void checkStoragePlugoutAndCloseStorage() {
   
   
    String cmd = "perl " + cfg.getArchiveConfiguration().getCheckStoragePlugoutScriptPath();
    logger.info("execute cmd {} to check whether all the disks in data node is changed", cmd);
    Process pid;
    try {
      pid = Runtime.getRuntime().exec(cmd);
    } catch (Exception e) {
      logger.warn("caught an exception, {}", e.getCause());
      return;
    }

    List<String> results = processWaiter.wait(pid);
    for (String line : results) {
      logger.warn("line : {}", line);
      try {
        processPlugOutDisks(line);
      } catch (Exception e) {
        logger.warn("caught an exception", e);
      }
    }
  }

  private void processPlugOutDisks(String line) throws StorageException {
    if (ejectedPattern == null) {
      String pattern = cfg.getArchiveConfiguration().getArchiveEjectedMatcher()
          .replace("%s", "(.+)");
      logger.warn("ejected pattern={}", pattern);
      ejectedPattern = Pattern.compile(pattern);
    }

    Matcher matcher = ejectedPattern.matcher(line);
    boolean matcherResult = matcher.matches();
    if (!matcherResult) {
      return;
    }
    if (matcher.groupCount() != 2) {
      throw new IllegalArgumentException("group count=" + matcher.groupCount());
    }
    String rawName = matcher.group(1);
    String serialNumber = matcher.group(2);
    logger.warn("ejected rawName={}, serial number={}", rawName, serialNumber);
    List<Archive> archives = pluginPlugoutManager.getArchives();
    for (Archive archive : archives) {
      if (!archive.getArchiveMetadata().getSerialNumber().equals(serialNumber)) {
        continue;
      }

      logger.warn("find the archive={}", archive);
     
      String deviceName = archive.getStorage().identifier();
      deviceName = deviceName.substring(deviceName.lastIndexOf('/') + 1);
     
      String reg = "[a-zA-Z]";
      deviceName = deviceName.replaceAll(reg, "");
      int ndevice = Integer.valueOf(deviceName);

      rawName = rawName.replaceAll(reg, "");
      int ndisk = Integer.valueOf(rawName);
      if (ndevice == ndisk) {
        String identifier = archive.getStorage().identifier();
       
        try {
          File file = new File(identifier);
          if (file.exists() && !file.delete()) {
            logger.warn("can't delete file: {}", identifier);
          }
        } catch (Exception e) {
          logger.warn("delete link file:{} failure", identifier, e);
        }

        logger.warn("archive is plugout: {}, identifer {}, plug disk {}", archive,
            archive.getStorage().identifier(), deviceName);
        archive.getStorage().close();
       
        AsynchronousFileChannelStorageFactory.getInstance().close(archive.getStorage());
      }
    }
  }
}
