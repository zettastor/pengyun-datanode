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

import static org.junit.Assert.assertTrue;

import org.apache.log4j.Level;
import org.junit.Test;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageHelper;
import py.datanode.page.context.AsyncPageContextWaiter;
import py.datanode.page.context.AsyncShadowPageContextImpl;
import py.utils.DataConsistencyUtils;

public class PageShadowTest extends BasePageManagerImplTest {
  private static final int DEFAULT_SSD_BLOCK_COUNT = 4;

  private static final int DEFAULT_PAGE_COUNT = 8;

  public PageShadowTest() {
  }

  @Test
  public void shadowAndRead() throws Exception {
    setLogLevel(Level.DEBUG);
    try {
      generatePageManager(DEFAULT_PAGE_COUNT, DEFAULT_SSD_BLOCK_COUNT,
          DEFAULT_RAW_STORAGE_TIMES_OF_PAGE);
      PageAddress originalAddress = PageAddressGenerator
          .generate(new SegId(1L, 2), 0, cfg.getPhysicalPageSize(),
              rawStorage);
      writePage(originalAddress, DataConsistencyUtils.generateDataByOffset(cfg.getPageSize(), 0));
      flushPage(originalAddress);
      AsyncPageContextWaiter<Page> waiter = new AsyncPageContextWaiter<>();
      PageAddress shadowAddress = PageAddressGenerator.generate(new SegId(1L, 2), 0,
          10 * cfg.getPhysicalPageSize(), rawStorage);
      PageContext<Page> context = contextFactory
          .generateAsyncShadowPageContext(originalAddress, shadowAddress,
              waiter, cfg.getDefaultPageRequestTimeoutMs());
      waiter.increment();
      pageManager.checkout(context);
      context = waiter.take();
      assertTrue(context instanceof AsyncShadowPageContextImpl);
      assertTrue(context.isSuccess());
      try {
        PageHelper.shadow((AsyncShadowPageContextImpl<Page>) context);
      } catch (Exception e) {
        logger.warn("caught an exception", e);
      }

      PageHelper.checkIn(pageManager, context, originalAddress.getSegId());

      DataConsistencyUtils.checkDataByOffset(readPage(shadowAddress, true), 0);
    } finally {
      pageManager.close();
    }
  }
}
