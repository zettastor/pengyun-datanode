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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import py.archive.page.PageAddressImpl;
import py.archive.segment.SegId;
import py.datanode.exception.TimedOutWaitingForAvailablePageException;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.TaskType;
import py.datanode.page.context.AsyncPageContextWaiter;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.context.PageContextWrapper;
import py.datanode.page.context.SinglePageContextFactory;
import py.storage.Storage;
import py.storage.impl.StorageUtils;
import py.utils.DataConsistencyUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StorageUtils.class)
public class PageManagerImplWithoutL2Test extends BasePageManagerImplTest {
  private static final Logger logger = LoggerFactory.getLogger(PageManagerImplWithoutL2Test.class);

  public PageManagerImplWithoutL2Test() throws Exception {
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
  public void syncReadAndWrite() throws Exception {
    generatePageManager(2);
    try {
      PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForRead(pageAddress);
      logger.info("page context: {}", pageContext);
      assertTrue(pageContext.isSuccess());
      pageManager.checkin(pageContext);

      pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      pageContext = pageManager.checkoutForInternalWrite(pageAddress);
      logger.info("page context: {}", pageContext);
      assertTrue(pageContext.isSuccess());
      byte value = 10;
      pageContext.getPage()
          .write(0,
              ByteBuffer.wrap(DataConsistencyUtils.generateFixedData(cfg.getPageSize(), value)));
      pageManager.checkin(pageContext);

      pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      pageContext = pageManager.checkoutForRead(pageAddress);
      logger.info("page context: {}", pageContext);
      assertTrue(pageContext.isSuccess());
      DataConsistencyUtils.checkFixedData(pageContext.getPage().getReadOnlyView(), value);
      pageManager.checkin(pageContext);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void wrapperMultiWriteInOneContextBySsd() throws Exception {
    cfg.setDefaultPageRequestTimeoutMs(20000);
    cfg.setStartFlusher(true);
    try {
      PowerMockito.mockStatic(StorageUtils.class);
    } catch (UnsatisfiedLinkError e) {
      logger.warn("can not mock jnotify", e);
    }

    PowerMockito.doReturn(true)
        .when(StorageUtils.class, "isSsd", ArgumentMatchers.any(Storage.class));

    generatePageManager(6);
    try {
      PageContextFactory<Page> factory = SinglePageContextFactory.getInstance();
      int pageCount = 3;
      ByteBuffer totalBuffer = ByteBuffer.allocateDirect(cfg.getPageSize());
      List<PageContext<Page>> pageContexts = new ArrayList<PageContext<Page>>();
      AsyncPageContextWaiter<Page> waiter = new AsyncPageContextWaiter<Page>();

      CountDownLatch latch = new CountDownLatch(pageCount);

      logger.debug("read page count: {}", pageCount);
      for (int i = 0; i < pageCount; i++) {
        PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0,
            i * cfg.getPhysicalPageSize(),
            rawStorage);
        PageContext<Page> context = factory
            .generateAsyncCheckoutContext(address, TaskType.CHECK_OUT_FOR_INTERNAL_WRITE, waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        waiter.increment();
        pageContexts.add(context);
        logger.info("context: " + context);
      }

      pageManager.checkout(new PageContextWrapper<Page>(pageContexts));
      while (true) {
        PageContext<Page> context = waiter.take();
        if (context == null) {
          logger.warn("exit the waiter");
          break;
        }

        try {
          if (!context.isSuccess()) {
            logger.error("get a page for read failure", context.getCause());
            Validate.isTrue(false);
          }
          logger.info("page: " + context);
          long kk = context.getPage().getAddress().getOffsetInSegment() / cfg.getPhysicalPageSize();
          totalBuffer.putInt((int) kk);
          totalBuffer.clear();
          context.getPage().write(0, totalBuffer);
          context.getPage().setDirty(true);
          pageManager.checkin(context);
        } catch (Exception e) {
          logger.error("caught an exception", e);
        }
      }

      while (true) {
        for (PageContext<Page> context : pageContexts) {
          if (context.getTaskType() != TaskType.FLUSHED_TO_DISK) {
            Thread.sleep(1000);
            continue;
          }
        }
        logger.debug("all page have flushed");
        break;

      }
      logger.warn("flush");
      pageContexts.clear();
      for (int i = 3; i < pageCount + 3; i++) {
        PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0,
            i * cfg.getPhysicalPageSize(),
            rawStorage);
        PageContext<Page> context = factory
            .generateAsyncCheckoutContext(address, TaskType.CHECK_OUT_FOR_INTERNAL_WRITE, waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        waiter.increment();
        pageContexts.add(context);
        logger.info("context: " + context);
      }

      pageManager.checkout(new PageContextWrapper<Page>(pageContexts));
      while (true) {
        PageContext<Page> context = waiter.take();
        if (context == null) {
          logger.warn("exit the waiter");
          break;
        }

        try {
          if (!context.isSuccess()) {
            logger.error("get a page for read failure", context.getCause());
            Validate.isTrue(false);
          }
          logger.info("page: " + context);
          long kk = context.getPage().getAddress().getOffsetInSegment() / cfg.getPhysicalPageSize();
          totalBuffer.putInt((int) kk);
          totalBuffer.clear();
          context.getPage().write(0, totalBuffer);
          pageManager.checkin(context);
        } catch (Exception e) {
          logger.error("caught an exception", e);
        }
      }

      Thread.sleep(1000);
      AtomicInteger count = new AtomicInteger(pageCount);
      logger.warn("check");
      for (int i = 0; i < pageCount; i++) {
        PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0,
            i * cfg.getPhysicalPageSize(),
            rawStorage);
        PageContext<Page> context = factory
            .generateAsyncCheckoutContext(address, TaskType.CHECK_OUT_FOR_READ, waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        pageManager.checkout(context);
        waiter.increment();
        logger.info("context: " + context);
      }
      while (true) {
        PageContext<Page> context = waiter.take();
        if (context == null && count.get() == 0) {
          break;
        }
        if (context == null && count.get() != 0) {
          continue;
        }

        try {
          if (!context.isSuccess()) {
            throw context.getCause();
          }
          ByteBuffer byteBuffer = context.getPage().getIoBuffer();
          byteBuffer.position(512);
          int kk = byteBuffer.getInt();
          if (kk != context.getPageAddressForIo().getOffsetInSegment() / cfg
              .getPhysicalPageSize()) {
            logger.error("the int is {}, the address is {}", kk, context.getPageAddressForIo());
            Validate.isTrue(false);
          }
          logger.info("page: " + context.getPage());
          pageManager.checkin(context);
          count.decrementAndGet();
        } catch (Exception e) {
          logger.error("caught an exception", e);
          throw e;
        }
      }

      Validate.isTrue(count.get() == 0);

    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void wrapperMultiReadInOneContext() throws Exception {
    cfg.setDefaultPageRequestTimeoutMs(20000);
    generatePageManager(5);
    try {
      PageContextFactory<Page> factory = SinglePageContextFactory.getInstance();
      int pageCount = 1024;

      List<PageContext<Page>> pageContexts = new ArrayList<PageContext<Page>>();
      AsyncPageContextWaiter<Page> waiter = new AsyncPageContextWaiter<Page>();
      logger.debug("read page count: {}", pageCount);
      for (int i = 0; i < pageCount; i++) {
        PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0,
            i * cfg.getPhysicalPageSize(),
            rawStorage);
        PageContext<Page> context = factory
            .generateAsyncCheckoutContext(address, TaskType.CHECK_OUT_FOR_READ, waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        waiter.increment();
        pageContexts.add(context);
        logger.info("context: " + context);
      }

      pageManager.checkout(new PageContextWrapper<Page>(pageContexts));
      int count = 0;
      while (true) {
        PageContext<Page> context = waiter.take();
        if (context == null) {
          logger.info("exit the waiter");
          break;
        }

        try {
          if (!context.isSuccess()) {
            logger.error("get a page for read failure", context.getCause());
            Validate.isTrue(false);
          }
          logger.info("page: " + context);
          count++;
          pageManager.checkin(context);
        } catch (Exception e) {
          logger.error("caught an exception", e);
        }
      }

      assertTrue("count: " + count + ", expected: " + pageCount, count == pageCount);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void wrapperMultiWriteInOneContext() throws Exception {
    cfg.setDefaultPageRequestTimeoutMs(20000);

    generatePageManager(5);
    try {
      PageContextFactory<Page> factory = SinglePageContextFactory.getInstance();
      int pageCount = 1024;

      List<PageContext<Page>> pageContexts = new ArrayList<PageContext<Page>>();
      AsyncPageContextWaiter<Page> waiter = new AsyncPageContextWaiter<Page>();
      logger.debug("read page count: {}", pageCount);
      for (int i = 0; i < pageCount; i++) {
        PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0,
            i * cfg.getPhysicalPageSize(),
            rawStorage);
        PageContext<Page> context = factory
            .generateAsyncCheckoutContext(address, TaskType.CHECK_OUT_FOR_INTERNAL_WRITE, waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        waiter.increment();
        pageContexts.add(context);
        logger.info("context: " + context);
      }

      pageManager.checkout(new PageContextWrapper<Page>(pageContexts));
      int count = 0;
      while (true) {
        PageContext<Page> context = waiter.take();
        if (context == null) {
          logger.info("exit the waiter");
          break;
        }

        try {
          if (!context.isSuccess()) {
            logger.error("get a page for read failure", context.getCause());
            Validate.isTrue(false);
          }
          logger.info("page: " + context);
          count++;
          pageManager.checkin(context);
        } catch (Exception e) {
          logger.error("caught an exception", e);
        }
      }

      assertTrue("count: " + count + ", expected: " + pageCount, count == pageCount);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void asyncMultiRead() throws Exception {
    cfg.setDefaultPageRequestTimeoutMs(20000);
    generatePageManager(5);
    try {
      PageContextFactory<Page> factory = SinglePageContextFactory.getInstance();
      int pageCount = 1024;

      AsyncPageContextWaiter<Page> waiter = new AsyncPageContextWaiter<Page>();
      for (int i = 0; i < pageCount; i++) {
        PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0,
            i * cfg.getPhysicalPageSize(),
            rawStorage);
        PageContext<Page> context = factory
            .generateAsyncCheckoutContext(address, TaskType.CHECK_OUT_FOR_READ, waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        waiter.increment();
        pageManager.checkout(context);
        logger.info("context: " + context);
      }

      int count = 0;
      while (true) {
        PageContext<Page> context = waiter.take();
        if (context == null) {
          break;
        }

        count++;
        try {
          assertTrue(context.isSuccess());
          logger.info("page: " + context.getPage());
          pageManager.checkin(context);
        } catch (Exception e) {
          logger.error("caught an exception", e);
        }
      }

      assertTrue("count: " + count + ", expected: " + pageCount, count == pageCount);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void syncMultiWrite() throws Exception {
    cfg.setFlushPagesIntervalMs(200);
    cfg.setStartFlusher(true);
    cfg.setDefaultPageRequestTimeoutMs(200000);

    final int numPages = 3;
    generatePageManager(numPages);
    try {
      final int pageAddressCount = 4;
      final List<PageAddressImpl> pageAddresses = Collections
          .synchronizedList(new ArrayList<PageAddressImpl>());
      for (int i = 0; i < pageAddressCount; i++) {
        long pageOffset = i * cfg.getPhysicalPageSize();
        pageAddresses.add(new PageAddressImpl(new SegId(10000L, i), 0, pageOffset, rawStorage));
      }

      int threadCount = 1000;
      final CountDownLatch waitLatch = new CountDownLatch(1);
      final CountDownLatch mainLatch = new CountDownLatch(threadCount);
      final Random random = new Random(System.currentTimeMillis());
      final Set<Integer> dirtyPageIndexes = new HashSet<>();
      final List<Exception> exceptions = new LinkedList<Exception>();

      for (int i = 0; i < threadCount; i++) {
        Thread thread = new Thread() {
          public void run() {
            try {
              waitLatch.await();
              int index = random.nextInt(pageAddressCount);
              PageAddressImpl pageAddress = pageAddresses.get(index);
              dirtyPageIndexes.add(index);

              PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(pageAddress);
              if (!pageContext.isSuccess()) {
                throw pageContext.getCause();
              }
              pageContext.getPage().write(0, ByteBuffer.allocate(10));
              pageManager.checkin(pageContext);
            } catch (Exception e) {
              logger.error("caught an exception", e);
              exceptions.add(e);
            } finally {
              logger.info("countdown value: {}", mainLatch.getCount());
              mainLatch.countDown();
            }
          }
        };
        thread.start();
      }

      waitLatch.countDown();
      mainLatch.await();

      long waitTime = dirtyPageIndexes.size() * cfg.getFlushPagesIntervalMs() + 20000;
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < waitTime) {
        if (pageManager.getDirtyPageCount() == 0) {
          break;
        }

        logger.debug("there are dirty pages: " + pageManager.getDirtyPageCount());
        Thread.sleep(2000);
      }

      assertEquals(0, pageManager.getDirtyPageCount());
      if (!exceptions.isEmpty()) {
        logger.error("caught exceptions({}) ", exceptions.size(), exceptions.get(0));
        throw exceptions.get(0);
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail("caught an exception: " + e);
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void correctionAndRead() throws Exception {
    generatePageManager(2);
    try {
      PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalCorrection(pageAddress);
      assertTrue("context: " + pageContext, pageContext.isSuccess());
      pageContext.getPage()
          .write(0, DataConsistencyUtils.generateDataByOffset(cfg.getPageSize(), 0L));
      pageContext.getPage().setPageLoaded(true);
      pageManager.checkin(pageContext);

      pageContext = pageManager.checkoutForRead(pageAddress);
      assertTrue("context: " + pageContext, pageContext.isSuccess());
      assertTrue(
          DataConsistencyUtils.checkDataByOffset(pageContext.getPage().getReadOnlyView(), 0L));
      pageManager.checkin(pageContext);

      pageContext = pageManager.checkoutForInternalCorrection(pageAddress);
      assertTrue("context: " + pageContext, pageContext.isSuccess());
      byte number = 12;
      pageContext.getPage()
          .write(0,
              ByteBuffer.wrap(DataConsistencyUtils.generateFixedData(cfg.getPageSize(), number)));
      pageManager.checkin(pageContext);

      pageContext = pageManager.checkoutForRead(pageAddress);
      try {
        assertTrue(
            DataConsistencyUtils.checkFixedData(pageContext.getPage().getReadOnlyView(), number));
      } finally {
        pageManager.checkin(pageContext);
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void multiThreadReadSamePageWhenCorretionSuccess() throws Exception {
    generatePageManager(2);
    try {
      final PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalCorrection(pageAddress);
      assertTrue(pageContext.isSuccess());
      int threadCount = 20;
      final List<Exception> exceptions = Collections.synchronizedList(new LinkedList<Exception>());
      final CountDownLatch mainLatch = new CountDownLatch(threadCount);
      final CountDownLatch readLatch = new CountDownLatch(1);
      final AtomicInteger successCount = new AtomicInteger(0);
      for (int i = 0; i < threadCount; i++) {
        Thread thread = new Thread() {
          public void run() {
            PageContext<Page> pageContext = null;
            try {
              readLatch.await();
              pageContext = pageManager.checkoutForRead(pageAddress);
              assertTrue(pageContext.isSuccess());
              assertTrue(DataConsistencyUtils
                  .checkDataByOffset(pageContext.getPage().getReadOnlyView(), 0L));
              successCount.incrementAndGet();
            } catch (Exception e) {
              logger.error("caught an exception", e);
              exceptions.add(e);
            } finally {
              if (pageContext != null) {
                pageManager.checkin(pageContext);
              }
              mainLatch.countDown();
            }
          }
        };
        thread.start();
      }

      readLatch.countDown();
      pageContext.getPage()
          .write(0, DataConsistencyUtils.generateDataByOffset(cfg.getPageSize(), 0L));
      pageContext.getPage().setPageLoaded(true);
      pageManager.checkin(pageContext);
      mainLatch.await();

      assertTrue(exceptions.isEmpty());
      assertEquals(successCount.get(), threadCount);

    } catch (Exception e) {
      fail("caught an exception" + e);
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void multiThreadReadSamePageWhenCorretionFail() throws Exception {
    cfg.setDefaultPageRequestTimeoutMs(20000);
    cfg.setStartFlusher(true);
    generatePageManager(2);
    int writeCount = 4;
    for (int i = 0; i < writeCount; i++) {
      long offset = i * cfg.getPhysicalPageSize();
      PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, offset, rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(pageAddress);
      assertTrue("context: " + pageContext, pageContext.isSuccess());
      pageContext.getPage()
          .write(0, DataConsistencyUtils.generateDataByOffset(cfg.getPageSize(), offset));
      pageManager.checkin(pageContext);
    }

    try {
      int pageIndex = 1;
      final long offset = pageIndex * cfg.getPhysicalPageSize();
      final PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0,
          pageIndex * cfg.getPhysicalPageSize(), rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalCorrection(pageAddress);

      assertTrue(pageContext.isSuccess());
      try {
        Assert.isTrue(DataConsistencyUtils
            .checkDataByOffset(pageContext.getPage().getReadOnlyView(), offset));
        assertTrue(false);
      } catch (Exception e) {
        logger.warn("ok it should throw the exception", e);
      }

      int threadCount = 20;
      final List<Exception> exceptions = Collections.synchronizedList(new LinkedList<Exception>());
      final CountDownLatch mainLatch = new CountDownLatch(threadCount);
      final AtomicInteger successCount = new AtomicInteger(0);
      for (int i = 0; i < threadCount; i++) {
        Thread thread = new Thread() {
          public void run() {
            PageContext<Page> pageContext = null;
            try {
              pageContext = pageManager.checkoutForRead(pageAddress);
              assertTrue(pageContext.isSuccess());
              assertTrue(DataConsistencyUtils
                  .checkDataByOffset(pageContext.getPage().getReadOnlyView(), offset));
              successCount.incrementAndGet();
            } catch (Exception e) {
              logger.warn("caught an exception", e);
              exceptions.add(e);
            }
            pageManager.checkin(pageContext);
            mainLatch.countDown();
          }
        };
        thread.start();
      }

      pageManager.checkin(pageContext);
      mainLatch.await();

      assertEquals(exceptions.size(), 0);
      assertEquals(successCount.get(), threadCount);

    } catch (Exception e) {
      fail("caught an exception" + e);
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void multiThreadReadSamePageWhenWriting() throws Exception {
    generatePageManager(2);
    try {
      final PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(pageAddress);
      assertTrue(pageContext.isSuccess());
      logger.info("page: " + pageContext);

      int threadCount = 10;

      final List<Exception> errors = new LinkedList<Exception>();
      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch mainLatch = new CountDownLatch(10);

      for (int i = 0; i < threadCount; i++) {
        Thread thread = new Thread() {
          public void run() {
            try {
              latch.await();
              PageContext<Page> pageContext = pageManager.checkoutForRead(pageAddress);
              assertTrue(pageContext.isSuccess());
              assertTrue(DataConsistencyUtils
                  .checkDataByOffset(pageContext.getPage().getReadOnlyView(), 0L));
              pageManager.checkin(pageContext);
            } catch (Exception e) {
              logger.error("caught an exception", e);
              errors.add(e);
            } finally {
              mainLatch.countDown();
            }
          }
        };
        thread.start();
      }

      latch.countDown();
      pageContext.getPage()
          .write(0, DataConsistencyUtils.generateDataByOffset(cfg.getPageSize(), 0L));
      pageManager.checkin(pageContext);
      mainLatch.await();

      assertTrue(errors.isEmpty());
    } catch (Exception e) {
      fail("caught an exception" + e);
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test(expected = TimedOutWaitingForAvailablePageException.class)
  public void timeout() throws Exception {
    cfg.setStartFlusher(true);
    cfg.setDefaultPageRequestTimeoutMs(2000);
    generatePageManager(1);
    PageContext<Page> writePageContext = null;
    PageContext<Page> readPageContext = null;
    try {
      PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      writePageContext = pageManager.checkoutForInternalWrite(pageAddress);
      assertTrue(writePageContext.isSuccess());
      logger.info("page: " + writePageContext);
      writePageContext.getPage()
          .write(0, DataConsistencyUtils.generateDataByOffset(cfg.getPageSize(), 0));
      pageAddress = new PageAddressImpl(new SegId(1L, 1), 10, 0, rawStorage);
      readPageContext = pageManager.checkoutForRead(pageAddress);
      assertTrue(!readPageContext.isSuccess());
      throw readPageContext.getCause();
    } finally {
      pageManager.checkin(writePageContext);
      pageManager.checkin(readPageContext);
      pageManager.close();
    }
  }

  @Test
  public void innerFlushedToStorage() throws Exception {
    cfg.setFlushPagesIntervalMs(1000);
    cfg.setStartFlusher(true);
    int pageCount = 3;
    generatePageManager(pageCount);

    try {
     
      for (int i = 0; i < pageCount * 3; i++) {
        long offset = i * cfg.getPhysicalPageSize();
        PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, offset, rawStorage);
        PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(pageAddress);
        logger.info("page: " + pageContext);
        assertTrue(pageContext.isSuccess());
        pageContext.getPage()
            .write(0, DataConsistencyUtils.generateDataByOffset(cfg.getPageSize(), offset));
        pageManager.checkin(pageContext);
      }

      Thread.sleep(1000);
     
      long waitTime = 2 * cfg.getFlushPagesIntervalMs() + 200000;
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < waitTime) {
        if (pageManager.getDirtyPageCount() == 0) {
          break;
        }

        logger.debug("there are dirty pages: " + pageManager.getDirtyPageCount());
        Thread.sleep(2000);
      }
      assertEquals(0, pageManager.getDirtyPageCount());

      for (int i = 0; i < pageCount * 3; i++) {
        long offset = i * cfg.getPhysicalPageSize();
        PageAddressImpl pageAddress = new PageAddressImpl(new SegId(1L, 0), 0, offset, rawStorage);
        PageContext<Page> pageContext = pageManager.checkoutForRead(pageAddress);
        logger.info("page context: {}", pageContext);
        assertTrue(pageContext.isSuccess());
        DataConsistencyUtils.checkDataByOffset(pageContext.getPage().getReadOnlyView(), offset);
        pageManager.checkin(pageContext);
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void syncReadAndWriteInMultiThread() throws Exception {
    cfg.setStartFlusher(true);
    cfg.setFlushPagesIntervalMs(100);
    generatePageManager(1);

    try {
      final int maxIndex = (int) (rawStorage.size() / cfg.getPhysicalPageSize());
      int threadCount = 10;
      final int ioCount = 1000;
      final CountDownLatch mainLatch = new CountDownLatch(threadCount);
      final Random random = new Random();
      final AtomicInteger counter = new AtomicInteger(0);

      for (int i = 0; i < threadCount; i++) {
        Thread thread = new Thread() {
          public void run() {
            while (true) {
              PageContext<Page> pageContext = null;
              boolean isRead = true;
              try {
                if (counter.getAndIncrement() >= ioCount) {
                  break;
                }

                int index = random.nextInt(maxIndex);
                if (index % 2 == 0) {
                  isRead = true;
                } else {
                  isRead = false;
                }
                PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0,
                    index * cfg.getPhysicalPageSize(), rawStorage);
                if (isRead) {
                  pageContext = pageManager.checkoutForRead(address);
                } else {
                  pageContext = pageManager.checkoutForInternalWrite(address);
                  pageContext.getPage().write(0, ByteBuffer.allocate(10));
                }

              } catch (Exception e) {
                logger.info("caught an exception", e);
              } finally {
                if (pageContext != null) {
                  pageManager.checkin(pageContext);
                }
              }
              mainLatch.countDown();
            }
          }
        };
        thread.start();
      }
      mainLatch.await();
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

  @Test
  public void failToLoadPageFromStorage() throws Exception {
    final int numPages = 10;
    cfg.setStartFlusher(false);

    try {
      generatePageManager(numPages);
      PageAddressImpl address = new PageAddressImpl(new SegId(1L, 0), 0, 0, rawStorage);
      PageContext<Page> context = pageManager.checkoutForExternalRead(address);
      assertTrue(context.isSuccess());
      pageManager.checkin(context);

      rawStorage.needReadThrowException = true;
      rawStorage.hasException = false;

      address = new PageAddressImpl(new SegId(1L, 0), 0, cfg.getPhysicalPageSize(), rawStorage);
      context = pageManager.checkoutForRead(address);
      assertTrue(!context.isSuccess());
      assertTrue(rawStorage.hasException);
    } finally {
      if (pageManager != null) {
        pageManager.close();
      }
    }

  }

  @Test
  public void failToFlushPageToStorage() throws Exception {
    generatePageManager(10);
    try {
      PageAddressImpl address = new PageAddressImpl(new SegId(10000L, 1), 0,
          cfg.getPhysicalPageSize(),
          rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(address);
      assertTrue(pageContext.isSuccess());
      pageContext.getPage().write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
      pageManager.checkin(pageContext);

      rawStorage.needWriteThrowException = true;
      rawStorage.hasException = false;
      CountDownLatch latch = new CountDownLatch(1);
      PageContext<Page> context = contextFactory.generatePageFlushContext(address, latch);
      pageManager.flushPage(context);
      context.waitFor();
      assertTrue(!context.isSuccess());
      assertTrue(rawStorage.hasException);
      rawStorage.needWriteThrowException = false;

    } finally {
      pageManager.close();
    }
  }

  @Test
  public void checkoutAndFlushPage() throws Exception {
    final int numPages = 10;
    generatePageManager(numPages);
    try {
      PageAddressImpl address = new PageAddressImpl(new SegId(10000L, 1), 0,
          cfg.getPhysicalPageSize(),
          rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(address);
      assertTrue(pageContext.isSuccess());
      pageContext.getPage().write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
      assertEquals(counterFlushToStorage.get(), 0);
      CountDownLatch latch = new CountDownLatch(1);
      PageContext<Page> context = contextFactory.generatePageFlushContext(address, latch);
      pageManager.flushPage(context);
      Validate.isTrue(!latch.await(2, TimeUnit.SECONDS));

      assertEquals(counterFlushToStorage.get(), 0);
      pageManager.checkin(pageContext);
      latch.await();
      assertEquals(counterFlushToStorage.get(), 1);
    } finally {
      pageManager.close();
    }
  }

  @Test
  public void flushClearPageAndGetThePageForWrite() throws Exception {
    generatePageManager(2);
    Random random = new Random();
    try {
     
      PageAddressImpl address = new PageAddressImpl(new SegId(100L, 1), 0,
          cfg.getPhysicalPageSize(), rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(address);
      assertTrue(pageContext.isSuccess());
      Page dirtyPage = pageContext.getPage();
      pageContext.getPage().write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
      pageManager.checkin(pageContext);

      Validate.isTrue(dirtyPage.isDirty());
      flushPage(address);
      long expiredTime = System.currentTimeMillis() + 3000;
      while (System.currentTimeMillis() < expiredTime) {
        if (!dirtyPage.isDirty() && !dirtyPage.isFlushing()) {
          break;
        }
        Thread.sleep(100);
      }

      Validate.isTrue(!dirtyPage.isDirty());
      Validate.isTrue(dirtyPage.canFlush());

      PageAddressImpl address1 = new PageAddressImpl(new SegId(100L, 1), 0, 0, rawStorage);
      AsyncPageContextWaiter<Page> waiter = new AsyncPageContextWaiter<>();
      PageContext<Page> writeContext = null;
      if (random.nextBoolean()) {
        writeContext = contextFactory
            .generateAsyncCheckoutContext(address1, TaskType.CHECK_OUT_FOR_INTERNAL_WRITE, waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        logger.warn("check out for write");
      } else {
        writeContext = contextFactory
            .generateAsyncCheckoutContext(address1, TaskType.CHECK_OUT_FOR_INTERNAL_CORRECTION,
                waiter,
                cfg.getDefaultPageRequestTimeoutMs());
        logger.warn("check out for correction");
      }
      waiter.increment();
      pageManager.checkout(writeContext);

      expiredTime = System.currentTimeMillis() + 2000;
      while (System.currentTimeMillis() < expiredTime) {
        Validate.isTrue(waiter.getPendingTaskCount() == 1);
        Thread.sleep(100);
      }

      Validate.isTrue(waiter.getPendingTaskCount() == 1);
      dirtyPage.setCanbeFlushed();

      expiredTime = System.currentTimeMillis() + 3000;
      while (System.currentTimeMillis() < expiredTime) {
        if (waiter.getPendingTaskCount() == 0) {
          break;
        }
        Thread.sleep(500);
      }

      PageContext<Page> context = waiter.take();
      Validate.isTrue(context.isSuccess());
      pageManager.checkin(context);
    } finally {
      pageManager.close();
    }
  }

  @Test
  public void flushPageTest() throws Exception {
    final int numPages = 10;
    generatePageManager(numPages);
    try {
      PageAddressImpl address = new PageAddressImpl(new SegId(10000L, 1), 0,
          cfg.getPhysicalPageSize(),
          rawStorage);
      PageContext<Page> pageContext = pageManager.checkoutForInternalWrite(address);
      assertTrue(pageContext.isSuccess());
      pageContext.getPage().write(0, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
      pageManager.checkin(pageContext);
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 10000) {
        if (pageManager.getDirtyPageCount() == 1) {
          break;
        }
        Thread.sleep(100);
      }
      assertEquals(pageManager.getDirtyPageCount(), 1);
      assertEquals(counterFlushToStorage.get(), 0);
      CountDownLatch latch = new CountDownLatch(1);
      PageContext<Page> context = contextFactory.generatePageFlushContext(address, latch);
      pageManager.flushPage(context);
      context.waitFor();
      int timeWaitForFlush = 10000;
      while (timeWaitForFlush > 0) {
        if (pageManager.getDirtyPageCount() == 0) {
          break;
        }
        timeWaitForFlush -= 100;
        Thread.sleep(100);
      }
      if (timeWaitForFlush == 0) {
        fail("time out!");
      }
      assertEquals(counterFlushToStorage.get(), 1);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail("caught an exception: " + e);
    } finally {
      try {
        pageManager.close();
      } catch (InterruptedException e) {
        logger.error("close page manager failure", e);
        fail();
      }
    }
  }

}
