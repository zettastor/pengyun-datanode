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

package py.datanode.page.impl;

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;
import py.archive.ArchiveOptions;
import py.archive.page.PageAddressImpl;
import py.archive.segment.SegId;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.PageAddressNotConsistentException;
import py.datanode.page.PageMetadata;
import py.exception.ChecksumMismatchedException;
import py.exception.StorageException;
import py.storage.Storage;
import py.test.TestBase;

public class PageMetadataImplTest extends TestBase {
  DataNodeConfiguration option;
  Storage testStorage = null;
  private SegId segId = new SegId(1L, 0);

  @Before
  public void inite() throws StorageException {
    option = new DataNodeConfiguration();
    ArchiveOptions.initContants(option.getPageSize(), option.getSegmentUnitSize(),
        option.getFlexibleCountLimitInOneArchive());
  }

  @Test
  public void testPageMetadata() throws PageAddressNotConsistentException {
    byte[] src = new byte[(int) option.getPhysicalPageSize()];
    int dataOffset = ArchiveOptions.PAGE_METADATA_LENGTH;
    for (int i = 0; i < option.getPhysicalPageSize(); i++) {
      if (i < dataOffset) {
        src[i] = 0;
      } else {
        src[i] = 10;
      }
    }

    PageAddressImpl pageAddress = new PageAddressImpl(segId, 0, option.getPhysicalPageSize(), null);

    PageMetadata pageMetadata = PageMetadataImpl.fromBuffer(ByteBuffer.wrap(src));
    pageMetadata.updateAddress(pageAddress);

    if (!pageMetadata.getAddress().equals(pageAddress)) {
      fail();
    }
  }
}
