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

package py.datanode;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.nio.ByteBuffer;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import py.common.LoggerConfiguration;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.storage.impl.PageAlignedStorageFactory;
import py.exception.StorageException;
import py.storage.Storage;

@Configuration
@Import(DataNodeConfiguration.class)
public class StorageDetect {
  private static final Logger logger = LoggerFactory.getLogger(StorageDetect.class);
  private static int EXIT_FOR_NORMAL = 100;
  private static int EXIT_FOR_SSD = 101;
  private static int EXIT_FOR_ERROR = -1;

  public static void main(String[] args) {
    LoggerConfiguration.getInstance().appendFile("logs/storagedetect.log", Level.DEBUG);

    CommandLineArgs cliArgs = new CommandLineArgs();
    JCommander commander = new JCommander(cliArgs, args);
    File file = new File(cliArgs.storagePath);
    if (!file.exists()) {
      logger.warn("file: {} is not exist", cliArgs.storagePath);
      commander.usage();
      System.exit(EXIT_FOR_ERROR);
    }

    if (cliArgs.timeSecond <= 0) {
      logger.warn("invalid time: {}", cliArgs.timeSecond);
      commander.usage();
      System.exit(EXIT_FOR_ERROR);
    }

    logger.info("configuration: {}", cliArgs);

    try {
      for (int i = 0; i < cliArgs.count; i++) {
        int result = storageDetect(cliArgs.storagePath, cliArgs.blockSize, cliArgs.timeSecond,
            cliArgs.threshold);
        if (result == EXIT_FOR_SSD) {
          logger.warn("the storage {} final result is ssd", cliArgs.storagePath);
          System.exit(EXIT_FOR_SSD);
        }
      }

      logger.warn("the storage {} final result is hdd", cliArgs.storagePath);
      System.exit(EXIT_FOR_NORMAL);
    } catch (StorageException e) {
      logger.error("can not initalize the storage: {}", cliArgs.storagePath, e);
      System.exit(EXIT_FOR_ERROR);
    }

  }

  private static int storageDetect(String storagePath, long blockSize, long timeSecond,
      long threshold) throws StorageException {
   
   
   
   
    PageAlignedStorageFactory storageFactory = new PageAlignedStorageFactory();
    storageFactory.setMode("r");
    Storage storage = storageFactory.setFile(new File(storagePath)).generate(storagePath);
    RandomDataGenerator random = new RandomDataGenerator();
    long maxRange = storage.size() / blockSize;
    logger.warn("storage: {} size: {}, maxRange: {}", storagePath, storage.size(), maxRange);
    byte[] buffer = new byte[(int) blockSize];
    long byteCount = 0;
    long times = 0;
    long startTime = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - startTime >= timeSecond * 1000) {
        break;
      }

      long position = (random.nextLong(0, maxRange - 1)) * blockSize;
      try {
        storage.read(position, ByteBuffer.wrap(buffer));
      } catch (Exception e) {
        logger.error("caught an exception when read offset: {}", position, e);
        throw e;
      }

      byteCount += blockSize;
      times++;
    }

    double speed = (double) byteCount / timeSecond;
    long iops = times / timeSecond;
    logger.warn("the storage speed: {}KB per second, IOPS: {}", speed / 1024, iops);
    if (iops <= threshold) {
      return EXIT_FOR_NORMAL;
    } else {
      return EXIT_FOR_SSD;
    }
  }

  private static class CommandLineArgs {
    public static final String STORAGE = "--storagepath";
    public static final String BLOCKSIZE = "--blocksize";
    public static final String TIMESECOND = "--timesecond";
    public static final String COUNT = "--count";
    public static final String THRESHOLD = "--threshold";
    @Parameter(names = STORAGE, description = "path to the file where a " 
        + "storage will be created.", required = true)
    public String storagePath;
    @Parameter(names = BLOCKSIZE, description = "block size for reading", required = false)
    public long blockSize = 4 * 1024L;
    @Parameter(names = TIMESECOND, description = "the last time for testing the" 
        + " speed of specified storage every count, unit: second", required = false)
    public long timeSecond = 3;
    @Parameter(names = COUNT, description = "the count for testing the speed of" 
        + " specified storage, unit: second", required = false)
    public int count = 3;
    @Parameter(names = THRESHOLD, description = "when the iops value of testing " 
        + "the speed of the storage is less than the threshold, it will return 100," 
        + " which stands for normal disk; when the iops value is larger than the threshold, " 
        + "it will return 101, which stands for ssd")
    public long threshold = 1000;

    @Parameter(names = "--help", help = true)
    private boolean help;

    @Override
    public String toString() {
      return "CommandLineArgs [storagePath=" + storagePath + ", blockSize=" + blockSize
          + ", timeSecond="
          + timeSecond + ", threshold=" + threshold + ", help=" + help + "]";
    }
  }
}
