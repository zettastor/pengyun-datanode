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

import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.mockito.Mock;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.datanode.archive.RawArchive;
import py.datanode.segment.SegmentUnit;

public class PageContentUtilTest extends BasePageManagerImplTest {
  protected final SegId segId = new SegId(RequestIdBuilder.get(), 0);
  protected final int storageOriginalPageNum = 110;
  @Mock
  SegmentUnit segUnit;
  @Mock
  SegmentUnitMetadata segUnitMetadata;
  @Mock
  RawArchive archive;
  private int numL1Pages = 5;
  private int pageSize = 512;
  private int blockSize = 1024;
  private int numL2Blocks = 2;

  public PageContentUtilTest() throws Exception {
    super();
  }

  protected ByteBuffer getByteBuffer(int delta, int size) {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.clear();
    for (int i = 0; i < buffer.capacity(); i++) {
      Validate.isTrue(buffer.hasRemaining());
      buffer.put((byte) (delta == 0 ? 0 : i + delta));
    }
    buffer.clear();
    return buffer;
  }
}
