/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
