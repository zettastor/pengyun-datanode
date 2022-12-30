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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import py.archive.AbstractArchiveBuilder;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.BaseArchiveBuilder;
import py.common.LoggerConfiguration;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.archive.ArchiveUtils;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.storage.impl.PageAlignedStorageFactory;
import py.exception.StorageException;
import py.storage.Storage;

@Configuration
@Import(DataNodeConfiguration.class)
public class QuerySerialNumber {
  private static final Logger logger = LoggerFactory.getLogger(QuerySerialNumber.class);
  private static DataNodeConfiguration dataNodeConfiguration;

  private static int EXIT_WITHOUT_SERIAL_NUMBER = -1;
  private static int EXIT_WITH_SERIAL_NUMBER = 0;

  public static void main(String[] args) {
    LoggerConfiguration.getInstance().appendFile("logs/QuerySerialNumber.log", Level.INFO);
    QuerySerialNumber.CommandLineArgs cliArgs = new QuerySerialNumber.CommandLineArgs();
    JCommander commander = new JCommander(cliArgs, args);

    File file = new File(cliArgs.storagePath);
    if (!file.exists()) {
      logger.warn("file: {} is not exist", cliArgs.storagePath);
      commander.usage();
      System.exit(EXIT_WITHOUT_SERIAL_NUMBER);
      return;
    }

    ApplicationContext ctx = new AnnotationConfigApplicationContext(DataNodeConfiguration.class);
    dataNodeConfiguration = ctx.getBean(DataNodeConfiguration.class);

    TlsfByteBufferManagerFactory.init(512, 1024 * 1024, true);

    logger.info("configuration: {}", cliArgs);

    PageAlignedStorageFactory storageFactory = new PageAlignedStorageFactory();
    storageFactory.setMode("r");

    Storage storage;
    try {
      storage = storageFactory.setFile(new File(cliArgs.storagePath)).generate(cliArgs.storagePath);
    } catch (StorageException e) {
      logger.warn("caught an exception when building storage", e);
      System.exit(EXIT_WITHOUT_SERIAL_NUMBER);
      return;
    }

    logger.info("The storage as {}", storage);

    BaseArchiveBuilder build = new BaseArchiveBuilder(null, storage);
    Archive archive;
    try {
      archive = build.build();
    } catch (Exception e) {
      logger.warn("caught an exception when building the archive", e);
      System.exit(EXIT_WITHOUT_SERIAL_NUMBER);
      return;
    }

    logger.info("The Archive as {}", archive);

    ArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
    if (archiveMetadata == null) {
      logger.warn("archive.getArchiveMetadata failed, archiveMetadata is null.");
      System.exit(EXIT_WITHOUT_SERIAL_NUMBER);
    }

    AbstractArchiveBuilder abstractArchiveBuilder = ArchiveUtils
        .getArchiveBuilder(dataNodeConfiguration, storage,
            archive.getArchiveMetadata().getArchiveType());
    ArchiveMetadata currentArchiveMetadata = null;
    try {
      currentArchiveMetadata = abstractArchiveBuilder.loadArchiveMetadata();
    } catch (Exception e) {
      logger.warn("Query serial number failed, device:[{}].", cliArgs.storagePath);
      System.exit(EXIT_WITHOUT_SERIAL_NUMBER);
      return;
    }
    String serialNumber = currentArchiveMetadata.getSerialNumber();

    if (StringUtils.isNotBlank(serialNumber)) {
      logger.info("Query serial number success, serial number:[{}] device:[{}].", serialNumber,
          cliArgs.storagePath);
      System.out.println(serialNumber);
      System.exit(EXIT_WITH_SERIAL_NUMBER);
    } else {
      logger.warn("Query serial number failed, device:[{}].", cliArgs.storagePath);
      System.exit(EXIT_WITHOUT_SERIAL_NUMBER);
    }
  }

  private static class CommandLineArgs {
    public static final String STORAGE = "--storagePath";
    @Parameter(names = STORAGE, description = "path to the " 
        + "file where a storage will be created. for example: /dev/raw/raw1", required = true)
    public String storagePath;

    @Override
    public String toString() {
      return "CommandLineArgs{" + "storagePath='" + storagePath + '\'' + '}';
    }
  }
}
