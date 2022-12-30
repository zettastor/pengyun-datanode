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
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveType;
import py.archive.BaseArchiveBuilder;
import py.archive.StorageType;
import py.common.LoggerConfiguration;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.DataNodeConfigurationUtils;
import py.datanode.storage.impl.PageAlignedStorageFactory;
import py.exception.StorageException;
import py.storage.Storage;

@Configuration
@Import(DataNodeConfiguration.class)
public class ArchiveTypeQueryAndLink {
  private static final Logger logger = LoggerFactory.getLogger(ArchiveTypeQueryAndLink.class);
  private static boolean enableSystemExist = true;
  private static DataNodeConfiguration dataNodeConfiguration;

  private static int EXIT_FOR_FORMAT_NOK = 201;
  private static int EXIT_FOR_FORMAT_EXIST = 200;

  public static void setEnableSystemExist(boolean existEn) {
    enableSystemExist = existEn;
  }

  private static int cmdRun(String[] cmd) {
    logger.info("the cmd running is: {}", Arrays.asList(cmd));

    BufferedReader reader = null;
    BufferedReader error = null;
    try {
      Process linkCmd = Runtime.getRuntime().exec(cmd);
      reader = new BufferedReader(new InputStreamReader(linkCmd.getInputStream()));
      error = new BufferedReader(new InputStreamReader(linkCmd.getErrorStream()));
      try {
        linkCmd.waitFor();
      } catch (Exception e) {
        return -1;
      }

      String line;
      while ((line = reader.readLine()) != null) {
        logger.debug("read line=" + line);
      }

      while ((line = error.readLine()) != null) {
        logger.error("error line=" + line);
      }

      return linkCmd.exitValue();
    } catch (IOException e) {
      logger.warn("caught an exception", e);
      return -1;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("", e);
        }
      }

      if (error != null) {
        try {
          error.close();
        } catch (Exception e) {
          logger.error("", e);
        }
      }
    }
  }

  private static void exitWithErrorCode(int returnCode) throws Exception {
    if (enableSystemExist) {
      System.exit(returnCode);
    } else {
      logger.warn("process will return and  return Code is {}", returnCode);
      if (returnCode == EXIT_FOR_FORMAT_NOK) {
        logger.warn("Storage format or link failed.");
      }
    }
  }

  public static boolean linkDataNodeByAppType(ArchiveType archiveType, String storageType,
      String rawPath)
      throws Exception {
    String subDir = DataNodeConfigurationUtils
        .getDirNameByArchiveType(dataNodeConfiguration, archiveType);
    storageType = storageType.toLowerCase();

    logger.info("The subDir is  {}", subDir);

    String rawNum = rawPath.replaceAll(".*[^\\d](?=(\\d+))", "");
    rawNum = rawNum.trim();

    File fileDir = new File(dataNodeConfiguration.getArchiveConfiguration().getPersistRootDir(),
        subDir);
    logger.warn("the raw number is: {}, sub directory: {}, {}", rawNum, subDir, fileDir);

    if (!fileDir.exists()) {
      fileDir.mkdirs();
      logger.info("sub directory created successfully: {}", fileDir.getPath());
    }

    File file = new File(fileDir, storageType + rawNum);
    logger.warn("not expected the file={}", file.getAbsolutePath());
    if (file.exists()) {
     
      String cmdStr = "ls -la " + fileDir.getAbsolutePath();
      String[] cmdQuery = {"/bin/sh", "-c", cmdStr};
      cmdRun(cmdQuery);
      return false;
    }

    String cmdStr =
        "ln -s " + rawPath + " " + fileDir.getAbsolutePath() + "/" + storageType + rawNum;
    String[] cmdLink = {"/bin/sh", "-c", cmdStr};
    logger.warn("running the command={}", Arrays.asList(cmdLink));
    return cmdRun(cmdLink) == 0;
  }

  public static void main(String[] args) throws Exception {
    LoggerConfiguration.getInstance().appendFile("logs/storageTypeQuery.log", Level.INFO);
    CommandLineArgs cliArgs = new CommandLineArgs();
    JCommander commander = new JCommander(cliArgs, args);

    if (cliArgs.needLink && StringUtils.isEmpty(cliArgs.storageType)) {
      System.err.println("if need link disk, you must input storageType");
      System.exit(1);
    }

    File file = new File(cliArgs.storagePath);
    if (!file.exists()) {
      logger.warn("file: {} is not exist", cliArgs.storagePath);
      commander.usage();
      exitWithErrorCode(EXIT_FOR_FORMAT_NOK);
      return;
    }

    TlsfByteBufferManagerFactory.init(512, 1024 * 1024, true);

    String storageType = StringUtils.strip(cliArgs.storageType);
    if (storageType == null) {
      storageType = StorageType.SATA.toString();
    }

    ApplicationContext ctx = new AnnotationConfigApplicationContext(DataNodeConfiguration.class);
    dataNodeConfiguration = ctx.getBean(DataNodeConfiguration.class);

    logger.info("configuration: {}", cliArgs);

    PageAlignedStorageFactory storageFactory = new PageAlignedStorageFactory();
    storageFactory.setMode("r");

    Storage storage;
    try {
      storage = storageFactory.setFile(new File(cliArgs.storagePath)).generate(cliArgs.storagePath);
    } catch (StorageException e) {
      logger.warn("caught an exception when building storage", e);
      exitWithErrorCode(EXIT_FOR_FORMAT_NOK);
      return;
    }

    logger.info("The storage as {}", storage);

    BaseArchiveBuilder build = new BaseArchiveBuilder(null, storage);
    Archive archive;
    try {
      archive = build.build();
    } catch (Exception e) {
      logger.warn("caught an exception when building the archive", e);
      exitWithErrorCode(EXIT_FOR_FORMAT_NOK);
      return;
    }

    logger.info("The Archive as {}", archive);

    ArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
    if (archiveMetadata == null) {
      logger.warn("archive.getArchiveMetadata failed");
      exitWithErrorCode(EXIT_FOR_FORMAT_NOK);
    }

    ArchiveType archiveTypeFromStorage = archiveMetadata.getArchiveType();
    String newStorageType = storageType;

    if (archiveTypeFromStorage.equals(ArchiveType.RAW_DISK)
        || archiveTypeFromStorage.equals(ArchiveType.UNSETTLED_DISK)) {
      if (!cliArgs.needLink) {
        exitWithErrorCode(EXIT_FOR_FORMAT_EXIST);
        return;
      }

      if (linkDataNodeByAppType(archiveTypeFromStorage, newStorageType,
          cliArgs.storagePath.trim())) {
        exitWithErrorCode(EXIT_FOR_FORMAT_EXIST);
      } else {
        exitWithErrorCode(EXIT_FOR_FORMAT_NOK);
      }
    } else {
      logger.warn("not support the archive type, maybe not format={}", archiveTypeFromStorage);
      exitWithErrorCode(EXIT_FOR_FORMAT_NOK);
    }
  }

  private static class CommandLineArgs {
    public static final String STORAGE = "--storagePath";
   
   
    public static final String STORAGETYPE = "--storageType";
    public static final String NEED_LINK = "--needLink";
    @Parameter(names = STORAGE, description = "path to the file where a storage will be created. " 
        + "for example: /dev/raw/raw1", required = true)
    public String storagePath;
    @Parameter(names = STORAGETYPE, description = "storage type for linking when the " 
        + "archive is formatted. for example: ssd/pcie/raw")
    public String storageType;
    @Parameter(names = NEED_LINK, description = "<true|false>. specifies whether need link disk, " 
        + "default true. Optional", arity = 1)
    public boolean needLink = true;

    @Parameter(names = "--help", help = true)
    private boolean help;

    @Override
    public String toString() {
      return "CommandLineArgs{" 
         + "storagePath='" + storagePath + '\'' 
         + ", storageType='" + storageType + '\'' 
         + ", needLink=" + needLink 
         + ", help=" + help 
         + '}';
    }
  }
}
