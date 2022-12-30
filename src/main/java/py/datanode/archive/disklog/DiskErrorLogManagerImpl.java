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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.archive.ArchiveStatus;
import py.archive.disklog.DiskErrorLogManager;
import py.datanode.archive.RawArchive;
import py.datanode.configuration.DataNodeConfiguration;
import py.engine.DelayedTask;
import py.engine.DelayedTaskEngine;
import py.engine.Result;
import py.engine.ResultImpl;
import py.exception.DiskBrokenException;
import py.exception.DiskDegradeException;
import py.exception.StorageException;

public class DiskErrorLogManagerImpl implements DiskErrorLogManager {
  private static final Logger logger = LoggerFactory.getLogger(DiskErrorLogManagerImpl.class);
  private static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS");
  private static DelayedTaskEngine delayEngine = new DelayedTaskEngine();

  static {
   
    delayEngine.setDaemon(true);
    delayEngine.setPrefix("record-error");
    delayEngine.start();
  }

  private final Map<Long, DiskErrorLogEntry> diskLogEntries = new HashMap<>();
  private final DataNodeConfiguration config;
  private final File logDir;

  private DiskErrorLogManagerImpl(DataNodeConfiguration config, File logDir) {
    this.config = config;
    this.logDir = logDir;

    if (logDir.exists()) {
      if (!logDir.isDirectory()) {
        throw new RuntimeException("Disk log dir exists but is not a directory");
      }
    } else {
      logDir.mkdir();
    }
  }

  private static String getNewDateStringForLogFile() {
    return format.format(new Date(System.currentTimeMillis()));
  }

  private static Date parseDateFromDiskLogFileName(String filename) {
    try {
      return format.parse(filename.split("\\.")[1]);
    } catch (Exception e) {
      logger.warn("can't parse filename: {}", filename, e);
      return new Date(0L);
    }
  }

  public static DiskErrorLogManagerImpl load(DataNodeConfiguration config, File persistenceRoot,
      String disklogSubdir) {
    if (persistenceRoot == null || !persistenceRoot.exists() || !persistenceRoot.isDirectory()) {
      throw new RuntimeException("Persistence root does not exists or not a directory");
    }

    File disklogDir = new File(persistenceRoot, disklogSubdir);
    if (disklogDir.exists()) {
      if (!disklogDir.isDirectory()) {
        throw new RuntimeException("Disk log dir exists but is not a directory");
      }
    } else {
      disklogDir.mkdir();
    }

    SortedSet<File> latestFiles = new TreeSet<File>(new Comparator<File>() {
      public int compare(File file1, File file2) {
        Date date1 = parseDateFromDiskLogFileName(file1.getName());
        Date date2 = parseDateFromDiskLogFileName(file2.getName());

        if (date1.getTime() > date2.getTime()) {
          return -1;
        }
        return 1;
      }
    });

    for (File file : disklogDir.listFiles()) {
      latestFiles.add(file);
    }

    DiskErrorLogManagerImpl diskLog = null;

    ObjectMapper mapper = new ObjectMapper();
    DiskErrorLogManagerImpl tempLog = new DiskErrorLogManagerImpl(config, disklogDir);
    BufferedReader reader = null;
    String line = null;
    for (File file : latestFiles) {
      try {
        reader = new BufferedReader(new FileReader(file));
        while ((line = reader.readLine()) != null) {
          DiskErrorLogEntry entry = mapper.readValue(line, DiskErrorLogEntry.class);
          tempLog.diskLogEntries.put(entry.getArchiveId(), entry);
        }

        if (!tempLog.diskLogEntries.isEmpty()) {
          diskLog = tempLog;
          break;
        }
      } catch (Exception e) {
        logger.warn(
            "Caught an exception when reading disklog. Line: {}, skipping and deleting file: {}",
            line,
            file, e);
        file.delete();
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException e) {
            logger.warn("can't close file: {}", file, e);
          }
        }
      }
    }

    return diskLog != null ? diskLog : new DiskErrorLogManagerImpl(config, disklogDir);
  }

  public synchronized void putArchive(Archive archive)
      throws DiskDegradeException, DiskBrokenException {
    long archiveId = archive.getArchiveMetadata().getArchiveId();

    String serialNumber = archive.getArchiveMetadata().getSerialNumber();
    String deviceName = getDeviceNameFromArchive(archive);

    logger.debug("archive id {}, serialNumber {}, deviceName {}", archiveId, serialNumber,
        deviceName);

    DiskErrorLogEntry entry = diskLogEntries.get(archiveId);
    if (entry == null) {
     
      entry = new DiskErrorLogEntry(deviceName, archiveId, 0L, serialNumber);
      diskLogEntries.put(archiveId, entry);
      delayEngine.drive(new WriteTask(archive));
      return;
    }

    if (!deviceName.equals(entry.getDeviceName())) {
     
     
     
     
     
      logger.warn(
          "device name does not match logged name for the disk" 
              + " with this archiveId: {} deviceName {} loggedName {}",
          archiveId, deviceName, entry.getDeviceName());
      entry.setDeviceName(deviceName);
    }

    long exceptionCounter = entry.getExceptionCounter();

    if (exceptionCounter > config.getDegradeDiskStorageExceptionThreshold()
        && exceptionCounter <= config.getBrokenDiskStorageExceptionThreshold()) {
      logger.warn(
          "disk has occured I/O exception, but not reach broken threshold {}. Error count {}",
          config.getBrokenDiskStorageExceptionThreshold(), exceptionCounter);
      throw new DiskDegradeException(deviceName, archiveId, exceptionCounter);
    }

    if (entry.getExceptionCounter() > config.getBrokenDiskStorageExceptionThreshold()) {
      logger.warn("disk has occured I/O exception, reach broken threshold{}, error count {}");
      throw new DiskBrokenException(deviceName, archiveId, entry.getExceptionCounter());
    }
  }

  private void write() throws IOException, InterruptedException {
    PrintStream output = null;
    ObjectMapper mapper = new ObjectMapper();
    try {
      output = new PrintStream(new FileOutputStream(getNextFile()));
      for (DiskErrorLogEntry entry : diskLogEntries.values()) {
        String jsonErrorLog = mapper.writeValueAsString(entry);
        output.println(jsonErrorLog);
      }
      output.flush();
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }

  public void recordError(Archive archive, StorageException error) {
    delayEngine.drive(new IoErrorTask(archive, config.getDelayRecordStorageExceptionMs()));
  }

  public void rawArchiveBroken(Archive archive) {
    if (archive == null) {
      return;
    }
    if (!(archive instanceof RawArchive)) {
      logger.warn("this operation is not support for:{}", archive);
      throw new UnsupportedOperationException();
    }
    delayEngine.drive(new DiskBrokenTask((RawArchive) archive,
        config.getDelayRecordStorageExceptionMs()));
  }

    private File getNextFile() throws InterruptedException {
    File file = new File(logDir, "disklog." + getNewDateStringForLogFile());
    while (file.exists()) {
      Thread.sleep(1);
      file = new File(logDir, "disklog." + getNewDateStringForLogFile());
    }
    return file;
  }

  private String getDeviceNameFromArchive(Archive archive) {
    String deviceName = archive.getStorage().identifier();
    if (deviceName.contains("/")) {
      deviceName = deviceName.substring(deviceName.lastIndexOf('/') + 1);
    }
    return deviceName;
  }

  @Override
  public long getErrorCount(Archive archive) {
    DiskErrorLogEntry entry = diskLogEntries.get(archive.getArchiveMetadata().getArchiveId());
    return (entry == null) ? 0 : entry.getExceptionCounter();
  }

  @Override
  public void removeArchive(Archive archive) {
    delayEngine.drive(new RemoveTask(archive));
  }

  private class WriteTask extends DelayedTask {
    private Archive archive;

    public WriteTask(Archive archive) {
      super(0);
      this.archive = archive;
    }

    @Override
    public Result work() {
     
      try {
        write();
      } catch (IOException | InterruptedException e) {
        logger.error("clean error get an exception ", e);
      }
      return ResultImpl.DEFAULT;
    }
  }

  private class RemoveTask extends DelayedTask {
    private Archive archive;
    private CountDownLatch latch = new CountDownLatch(1);

    public RemoveTask(Archive archive) {
      super(0);
      this.archive = archive;
    }

    @Override
    public Result work() {
     
      try {
        if (null == diskLogEntries.remove(archive.getArchiveMetadata().getArchiveId())) {
          logger.warn("There is no entry in the diskErrorLogManager for archive={}",
              archive.getArchiveMetadata());
          return ResultImpl.DEFAULT;
        }
        write();
      } catch (IOException | InterruptedException e) {
        logger.error("clean error get an exception ", e);
      } finally {
        latch.countDown();
      }
      return ResultImpl.DEFAULT;
    }
  }

  private class IoErrorTask extends DelayedTask {
    public final Archive archive;

    public IoErrorTask(Archive archive, int delayMs) {
      super(delayMs);
      this.archive = archive;
    }

    @Override
    public Result work() {
      ArchiveStatus status = archive.getArchiveMetadata().getStatus();
      if (ArchiveStatus.isEjected(status) || status == ArchiveStatus.BROKEN) {
        logger.warn("the archive={} is ejected, no need recording", archive);
        return ResultImpl.DEFAULT;
      }

      long archiveId = archive.getArchiveMetadata().getArchiveId();
      DiskErrorLogEntry entry = diskLogEntries.get(archiveId);
      boolean needUpdate = false;

      if (entry == null) {
        logger.warn("there is no entry for this archive={}", archive.getArchiveMetadata());
      } else {
        String deviceName = getDeviceNameFromArchive(archive);
        String serialNumber = archive.getArchiveMetadata().getSerialNumber();
        long counter = entry.getExceptionCounter() + 1;
        entry = new DiskErrorLogEntry(deviceName, archiveId, counter, serialNumber);
        diskLogEntries.put(archiveId, entry);
        if (counter > config.getDegradeDiskStorageExceptionThreshold()) {
          needUpdate = true;
        }
      }

      if (needUpdate) {
        long counter = entry.getExceptionCounter();
        ArchiveStatus newStatus = (counter > config.getBrokenDiskStorageExceptionThreshold()) 
            ? ArchiveStatus.BROKEN :
            ArchiveStatus.DEGRADED;
        logger.warn("counter is {}, archive is {}, status={}", counter, archive, newStatus);
        try {
          archive.setArchiveStatus(newStatus);
        } catch (Exception ex) {
          logger.error("Caught an exception that set archive to status {}", newStatus, ex);
        }

        try {
         
          write();
        } catch (Exception e) {
          logger.error("Caught an exception while writing disk log, archive={}",
              archive.getArchiveMetadata(),
              e);
        }
      }
      return ResultImpl.DEFAULT;
    }
  }

  private class DiskBrokenTask extends DelayedTask {

    public final RawArchive archive;

    public DiskBrokenTask(RawArchive archive, int delayMs) {
      super(delayMs);
      this.archive = archive;
    }

    @Override
    public Result work() {
      try {
        archive.setArchiveStatus(ArchiveStatus.BROKEN);
      } catch (Exception ex) {
        logger.error("Caught an exception that set archive to status {}",
            ArchiveStatus.BROKEN, ex);
      }
      return ResultImpl.DEFAULT;
    }
  }

}
