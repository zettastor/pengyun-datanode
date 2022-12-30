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

package py.datanode.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import py.archive.ArchiveOptions;
import py.archive.RawArchiveMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.recurring.DefaultSegmentUnitTaskCallback;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.storage.impl.PageAlignedStorageFactory;
import py.datanode.storage.impl.StorageBuilder;
import py.instance.InstanceId;
import py.storage.Storage;

@Configuration
@Import(DataNodeConfiguration.class)
public class ArchivesReader {
  @Autowired
  private DataNodeConfiguration cfg;

  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: ArchivesReader rawDevicePath pageSize segmentSize");
      System.exit(-1);
    }

    String strRawDevPath = args[0];
    try {
      int pageSize = Integer.parseInt(args[1]);
      long segmentSize = Long.parseLong(args[2]);

      ApplicationContext ctx = new AnnotationConfigApplicationContext(ArchivesReader.class);
      ArchivesReader archivesReader = ctx.getBean(ArchivesReader.class);
      try {
        System.out.println(archivesReader.read(strRawDevPath, pageSize, segmentSize));
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.exit(0);
    } catch (Exception e) {
      System.err.println(e);
      System.err.println("Usage: ArchivesReader rawDevicePath pageSize segmentSize");
      System.exit(-1);
    }
  }

  public String read(String strRawDevPath, int pageSize, long segmentSize) throws Exception {
    int pageSizeByte = pageSize;
    long segSizeByte = segmentSize;

    System.out.println("page size: " + pageSizeByte + ", segment size: " + segSizeByte);
    ArchiveOptions.initContants(pageSizeByte, segSizeByte, ArchiveOptions.SECTOR_SIZE, (double) 0,
        ArchiveOptions.DEFAULT_MAX_FLEXIBLE_COUNT,
        ArchiveOptions.PAGE_METADATA_NEED_FLUSH_DISK);

    TlsfByteBufferManagerFactory.init(512, 16 * 1024 * 1024, true);

    List<Storage> rawStorages = new LinkedList<Storage>();
    File rawDevPath = new File(strRawDevPath);
    if (rawDevPath.isDirectory()) {
      for (Storage storage : StorageBuilder
          .getFileStorage(rawDevPath, new PageAlignedStorageFactory())) {
        rawStorages.add(storage);
      }
    } else {
      rawStorages
          .add(StorageBuilder.getSingleFileStorage(rawDevPath, new PageAlignedStorageFactory()));
    }

    System.out.println("Begin setting up the segment manager");

    StringBuilder builderAbs = new StringBuilder();
    StringBuilder builderContent = new StringBuilder();
    String titleCutOffRule = "\r\n---------------------------------------" 
        + "---------------------------------------------------------------";
    String titleStr = "\r\nSigId   Status               PrimaryId           (S)econdaries       " 
        + "  (S)econdaries/(TaskEngine)rbiter";

    StringBuilder builder = new StringBuilder();
    ObjectMapper mapper = new ObjectMapper();
    try {
      for (Storage storage : rawStorages) {
        RawArchive archive = null;

        try {
          RawArchiveBuilder archiveBuilder = new RawArchiveBuilder(cfg, storage)
              .setSegmentUnitTaskCallback(new DefaultSegmentUnitTaskCallback());
          SyncLogTaskExecutor syncLogTaskExecutor = new SyncLogTaskExecutor(1, "reader", null, null,
              cfg);
          archiveBuilder.setSyncLogTaskExecutor(syncLogTaskExecutor);
          archive = (RawArchive) archiveBuilder.build();
        } catch (Exception se) {
          System.err.println(
              "StorageException encountered on RawArchive initialization, " 
                  + "segment units on it will be unavailable"
                  + " storage: " + storage);
          se.printStackTrace();
          continue;
        }

        RawArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
        builder.append("\n===============================\n");
        builder.append("\n" + storage.identifier() + "\n");
        builder.append(
            "archive: " + mapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(archiveMetadata));
        builder.append("\nsegment units:");

        String contentStr = null;
        contentStr = String
            .format("\r\n %s\r\narchiveId: %-20d    status:%s", storage.identifier().toString(),
                archiveMetadata.getArchiveId(), archiveMetadata.getStatus().toString());
        builderAbs.append(contentStr);

        for (SegmentUnit segmentUnit : archive.getSegmentUnits()) {
          builder.append("\n------------------------------\n");

          SegmentUnitMetadata segmentUnitMetaData = segmentUnit.getSegmentUnitMetadata();

          builder.append("SegmentUnit: " + mapper.writerWithDefaultPrettyPrinter()
              .writeValueAsString(segmentUnitMetaData));

          contentStr = String
              .format("\r\n%-5d %-20s %-20s ", segmentUnitMetaData.getSegId().getIndex(),
                  segmentUnitMetaData.getStatus().name(),
                  segmentUnitMetaData.getMembership().getPrimary().toString());

          Iterator<InstanceId> secondaryIterator = segmentUnitMetaData.getMembership()
              .getSecondaries()
              .iterator();
          while (secondaryIterator.hasNext()) {
            contentStr += String.format("(S)%-20s", secondaryIterator.next().toString());
          }

          Iterator<InstanceId> arbitersIterator = segmentUnitMetaData.getMembership().getArbiters()
              .iterator();
          while (arbitersIterator.hasNext()) {
            contentStr += String.format("(TaskEngine)%-20s", arbitersIterator.next().toString());
          }

          builderContent.append(contentStr);
         
         
        }

        builderAbs.append(titleCutOffRule);
        builderAbs.append(titleStr);
        builderAbs.append(titleCutOffRule);
        builderAbs.append(builderContent.toString());
      }

    } catch (Exception e) {
      System.err.println(
          "caught an exception when reading archive metadata" 
              + " and segment unit metadata. Quit launching datanode");
      e.printStackTrace();
      throw e;
    }

    builderAbs.append(builder.toString());
    return builderAbs.toString();
  }
}
