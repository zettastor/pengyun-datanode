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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownStatusDecider {
  public static final String DATANODE_START_TIME_FILE_NAME = "DataNodeStartTime";
  public static final String DATANODE_SHUTDOWN_TIME_FILE_NAME = "DataNodeCleanShutdownTime";
  private static final Logger logger = LoggerFactory.getLogger(ShutdownStatusDecider.class);
  private final File startTimeFile;
  private final File shutdownTimeFile;
  private final File procUpTimeFile;

  public ShutdownStatusDecider(File persistenceRoot) {
    this(persistenceRoot, "/proc/uptime");
  }

  public ShutdownStatusDecider(File persistenceRoot, String fileNameToProcUpTime) {
    this.procUpTimeFile = new File(fileNameToProcUpTime);
    startTimeFile = new File(persistenceRoot, DATANODE_START_TIME_FILE_NAME);
    shutdownTimeFile = new File(persistenceRoot, DATANODE_SHUTDOWN_TIME_FILE_NAME);
  }

  private static long readTimeFromFile(File file) throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      String line = reader.readLine();
      long time = Long.parseLong(line);
      logger
          .info("read {} from time file: {} and parse it to time: {} ", line, file, new Date(time));
      return time;
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  public static void persistTimeToFile(File file, long time) throws IOException {
    PrintWriter writer = null;
    BufferedReader reader = null;
    List<String> lines = new ArrayList<String>();
    String line;
    try {
      reader = new BufferedReader(new FileReader(file));
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    } catch (IOException e) {
      logger.info("file not found, maybe the first time to start");
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    try {
      writer = new PrintWriter(new FileWriter(file));
      writer.println(String.valueOf(time));
      writer.println(new Date(time));
      if (lines.size() > 0) {
        for (String eachLine : lines) {
          writer.println(eachLine);
        }
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  public void persistStartTime() throws IOException {
    persistTimeToFile(startTimeFile, System.currentTimeMillis());
  }

  public void persistShutdownTime() throws IOException {
    persistTimeToFile(shutdownTimeFile, System.currentTimeMillis());
  }

  /**
   * Create an shutdown_time file with 0 in it when the service is started. By doing so, we can tell
   * if the JVM has crashed last time.
   */
  public void createEmptyShutdownTimeFile() throws IOException {
    if (!shutdownTimeFile.exists()) {
      persistTimeToFile(shutdownTimeFile, 0L);
    }
  }

  public DataNodeShutdownStatus getStatus() {
    try {
     
      if (!startTimeFile.exists()) {
        return DataNodeShutdownStatus.HOST_CRASH;
      }

      long jvmStartTime = readTimeFromFile(startTimeFile);
      long systemStartTime = getSystemStartupTime();

      if (systemStartTime > jvmStartTime) {
        return DataNodeShutdownStatus.HOST_CRASH;
      }

      if (!shutdownTimeFile.exists()) {
        return DataNodeShutdownStatus.JVM_CRASH;
      }

      long jvmShutdownTime = readTimeFromFile(shutdownTimeFile);
     
     
     
      if (jvmShutdownTime > jvmStartTime) {
        return DataNodeShutdownStatus.CLEAN;
      } else {
        return DataNodeShutdownStatus.JVM_CRASH;
      }

    } catch (Exception e) {
      logger.error("caught an exception", e);
      return DataNodeShutdownStatus.HOST_CRASH;
    }
  }

  protected long getSystemStartupTime() throws Exception {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(procUpTimeFile));
      String line = reader.readLine();
      String[] parts = line.split(" ");
      double uptime = Double.parseDouble(parts[0]);
      if (uptime <= 0) {
        throw new IllegalArgumentException(
            "Invalid system uptime. Read from file:" + procUpTimeFile.getName());
      }
      long systemStartTime = System.currentTimeMillis() - (long) (uptime * 1000);
      logger.info("{} read from {} is parsed to {} ", line, procUpTimeFile.getName(),
          new Date(systemStartTime));
      return systemStartTime;
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  public enum DataNodeShutdownStatus {
    CLEAN, JVM_CRASH, HOST_CRASH,
  }
}
