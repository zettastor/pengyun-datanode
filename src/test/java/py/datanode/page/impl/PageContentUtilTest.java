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
