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

package py.datanode.cache;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddressImpl;
import py.archive.segment.SegId;
import py.common.Utils;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.impl.BasePageManagerImplTest;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.PageManagerImplWithoutL2Test;
import py.utils.DataConsistencyUtils;

public class PageManagerWithExternalTest extends BasePageManagerImplTest {
  private static final Logger logger = LoggerFactory.getLogger(PageManagerImplWithoutL2Test.class);

  public PageManagerWithExternalTest() throws Exception {
  }

  @BeforeClass
  public static void beforeClass() {
    setLogLevel(Level.DEBUG);
  }

  @AfterClass
  public static void afterClass() {
    setLogLevel(Level.DEBUG);
  }

  @Test
  public void testExternal() throws Exception {
    cfg.setStartFlusher(true);
    generatePageManager(1000);
    final int pageCount = 1000;
    int maxIndexTmp = (int) (rawStorage.size() / cfg.getPhysicalPageSize()) - 1;
    final int maxIndex = maxIndexTmp > 1500 ? 1500 : maxIndexTmp;
    logger.warn("the max count is {}", maxIndex);
    CountDownLatch countDownLatch = new CountDownLatch(6);
    Thread externalWrite = new Thread(() -> {
      RandomDataGenerator dataGenerator = new RandomDataGenerator();
      for (int i = 0; i < pageCount; i++) {
        try {
          PageAddressImpl address = new PageAddressImpl(new SegId(100L, 1), 0,
              dataGenerator.nextInt(0, maxIndex) * cfg.getPhysicalPageSize(), rawStorage);
          PageContext<Page> pageContext = pageManager.checkoutForExternalWrite(address);
          pageContext.waitFor();
          logger.warn("generate an page {}", pageContext.getPageAddressForIo());
          if (!pageContext.isSuccess()) {
            logger.error("the page is not success", pageContext.getCause());
            return;
          }
          pageContext.getPage()
              .write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
          pageManager.checkin(pageContext);
        } catch (Exception e) {
          logger.error(" ", e);
        }

      }
      countDownLatch.countDown();
    });
    externalWrite.start();

    Thread internalWrite = new Thread(() -> {
      RandomDataGenerator dataGenerator = new RandomDataGenerator();
      for (int i = 0; i < pageCount; i++) {
        try {
          PageAddressImpl address = new PageAddressImpl(new SegId(100L, 1), 0,
              dataGenerator.nextInt(0, maxIndex) * cfg.getPhysicalPageSize(), rawStorage);
          PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(address);
          pageContext.waitFor();
          logger.warn("generate an page {}", pageContext.getPageAddressForIo());
          if (!pageContext.isSuccess()) {
            logger.error("the page is not success", pageContext.getCause());
            return;
          }
          pageContext.getPage()
              .write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
          pageManager.checkin(pageContext);
        } catch (Exception e) {
          logger.error(" ", e);
        }

      }
      countDownLatch.countDown();
    });
    internalWrite.start();

    Thread externalCorrectionThread = new Thread(() -> {
      RandomDataGenerator dataGenerator = new RandomDataGenerator();
      for (int i = 0; i < pageCount; i++) {
        try {
          PageAddressImpl address = new PageAddressImpl(new SegId(100L, 1), 0,
              dataGenerator.nextInt(0, maxIndex) * cfg.getPhysicalPageSize(), rawStorage);
          PageContext<Page> pageContext = pageManager.checkoutForExternalCorrection(address);
          pageContext.waitFor();
          logger.warn("generate an page {}", pageContext.getPageAddressForIo());
          if (!pageContext.isSuccess()) {
            logger.error("the page is not success", pageContext.getCause());
            return;
          }
          pageContext.getPage()
              .write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
          pageContext.getPage().setPageLoaded(true);
          pageManager.checkin(pageContext);
        } catch (Exception e) {
          logger.error(" ", e);
        }

      }
      countDownLatch.countDown();
    });
    externalCorrectionThread.start();

    Thread internalCorrectionThread = new Thread(() -> {
      RandomDataGenerator dataGenerator = new RandomDataGenerator();
      for (int i = 0; i < pageCount; i++) {
        try {
          PageAddressImpl address = new PageAddressImpl(new SegId(100L, 1), 0,
              dataGenerator.nextInt(0, maxIndex) * cfg.getPhysicalPageSize(), rawStorage);
          PageContext<Page> pageContext = pageManager.checkoutForInternalCorrection(address);
          pageContext.waitFor();
          logger.warn("generate an page {}", pageContext.getPageAddressForIo());
          if (!pageContext.isSuccess()) {
            logger.error("the page is not success", pageContext.getCause());
            return;
          }
          pageContext.getPage()
              .write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
          pageContext.getPage().setPageLoaded(true);
          pageManager.checkin(pageContext);
        } catch (Exception e) {
          logger.error(" ", e);
        }

      }
      countDownLatch.countDown();
    });
    internalCorrectionThread.start();

    Thread externalReadThread = new Thread(() -> {
      RandomDataGenerator dataGenerator = new RandomDataGenerator();
      for (int i = 0; i < pageCount; i++) {
        try {
          PageAddressImpl address = new PageAddressImpl(new SegId(100L, 1), 0,
              dataGenerator.nextInt(0, maxIndex) * cfg.getPhysicalPageSize(), rawStorage);
          PageContext<Page> pageContext = pageManager.checkoutForExternalRead(address);
          pageContext.waitFor();
          logger.warn("generate an page {}", pageContext.getPageAddressForIo());
          if (!pageContext.isSuccess()) {
            logger.error("the page is not success", pageContext.getCause());
            return;
          }
          pageManager.checkin(pageContext);
        } catch (Exception e) {
          logger.error(" ", e);
        }

      }
      countDownLatch.countDown();
    });
    externalReadThread.start();

    Thread internalReadThread = new Thread(() -> {
      RandomDataGenerator dataGenerator = new RandomDataGenerator();
      for (int i = 0; i < pageCount; i++) {
        try {
          PageAddressImpl address = new PageAddressImpl(new SegId(100L, 1), 0,
              dataGenerator.nextInt(0, maxIndex) * cfg.getPhysicalPageSize(), rawStorage);
          PageContext<Page> pageContext = pageManager.checkoutForRead(address);
          pageContext.waitFor();
          logger.warn("generate an page {}", pageContext.getPageAddressForIo());
          if (!pageContext.isSuccess()) {
            logger.error("the page is not success", pageContext.getCause());
            return;
          }
          pageManager.checkin(pageContext);
        } catch (Exception e) {
          logger.error(" ", e);
        }

      }
      countDownLatch.countDown();
    });
    internalReadThread.start();
    countDownLatch.await(600, TimeUnit.SECONDS);
    Utils.waitUntilConditionMatches(10000, () -> {
      return pageManager.getDirtyPageCount() == 0;
    });

    pageManager.close();
    int counterExternal = counterExternalWrite.get() + counterExternalHit.get();
    if (counterExternal != pageCount * 2) {
      logger.warn("write {}, read{}, hit is {} ", counterExternalWrite.get(),
          counterExternalHit.get());
      org.apache.commons.lang.Validate.isTrue(false);
    }
  }
}
