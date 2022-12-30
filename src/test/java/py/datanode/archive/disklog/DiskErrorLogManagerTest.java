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

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import py.archive.Archive;
import py.archive.RawArchiveMetadata;
import py.datanode.archive.ArchiveInitializer;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.test.DataNodeConfigurationForTest;
import py.storage.Storage;
import py.test.TestBase;

public class DiskErrorLogManagerTest extends TestBase {
  private static final int PAGE_SIZE = 128 * 1024;
  private static final int SEG_UNIT_SIZE = 16 * 1024 * 1024;
  private static final int ARCHIVE_SIZE = 3 * SEG_UNIT_SIZE;
  private DataNodeConfiguration config;
  private List<Storage> storages = new ArrayList<Storage>();

  private void initArchive(ArchiveInitializer initializer, long archiveId, int pageSize,
      long segmentSize, long archiveSize, Storage storage) throws Exception {
    RawArchiveMetadata archiveMetadata = new RawArchiveMetadata();
    archiveMetadata.setArchiveId(archiveId);
    archiveMetadata.setPageSize(pageSize);
    archiveMetadata.setSegmentUnitSize(segmentSize);
    initArchive(initializer, archiveMetadata, archiveSize, storage);
  }

  private void initArchive(ArchiveInitializer initializer, RawArchiveMetadata archiveMetadata,
      long archiveSize, Storage storage) throws Exception {
    Archive archive = null;
    storages.add(storage);
  }

  @Before
  public void init() {
    config = new DataNodeConfigurationForTest();
    config.setPageSize(PAGE_SIZE);
    config.setSegmentUnitSize(SEG_UNIT_SIZE);
  }
}
