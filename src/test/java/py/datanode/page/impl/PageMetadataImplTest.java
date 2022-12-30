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
