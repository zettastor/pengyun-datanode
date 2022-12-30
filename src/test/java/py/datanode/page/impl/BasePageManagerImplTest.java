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

import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.Validate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;
import py.archive.ArchiveOptions;
import py.archive.ArchiveType;
import py.archive.page.PageAddress;
import py.common.DirectAlignedBufferAllocator;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.archive.RawArchive;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.TaskType;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.context.SinglePageContextFactory;
import py.datanode.utils.DummyAsyncStorageFactory;
import py.exception.StorageException;
import py.storage.impl.AsyncStorage;
import py.test.TestBase;
import py.utils.DataConsistencyUtils;

public class BasePageManagerImplTest extends TestBase {
  protected static final int DEFAULT_RAW_STORAGE_TIMES_OF_PAGE = 2048;
  protected PageContextFactory<Page> contextFactory = SinglePageContextFactory.getInstance();

  protected DummyAsyncStorageFactory factory = new DummyAsyncStorageFactory();
  protected DataNodeConfiguration cfg;

  protected PageManagerImpl pageManager = null;

  protected File ssdFile = new File("/tmp/test-ssdStorage.py.");
  protected File rawFile = new File("/tmp/test-rawStorage.py.");
  protected AsyncStorageTest rawStorage;
  protected AsyncStorageTest ssdStorage;
  protected AsyncStorageTest l2WriteCacheStorage;
  protected AtomicInteger counterLoadFromL2 = new AtomicInteger(0);
  protected AtomicInteger counterLoadFromStorage = new AtomicInteger(0);
  protected AtomicInteger counterFlushToL2 = new AtomicInteger(0);
  protected AtomicInteger counterFlushToStorage = new AtomicInteger(0);
  protected AtomicInteger counterNotEnoughL2Page = new AtomicInteger(0);
  protected AtomicInteger counterCancelled = new AtomicInteger(0);

  protected AtomicInteger counterExternalWrite = new AtomicInteger(0);
  protected AtomicInteger counterExternalRead = new AtomicInteger(0);
  protected AtomicInteger counterExternalHit = new AtomicInteger(0);

  {
    TlsfByteBufferManagerFactory.init(512, 1024 * 1024 * 100, true);
  }

  public BasePageManagerImplTest() {
    cfg = new DataNodeConfiguration();
  }

  @Before
  public void beforeMethod() throws Exception {
    if (ssdFile.exists()) {
      ssdFile.delete();
    }

    if (rawFile.exists()) {
      rawFile.delete();
    }

    cfg.setStartFlusher(false);
    cfg.setDefaultPageRequestTimeoutMs(5000);
    cfg.setFlushWaitMs(1000);
    cfg.setPageSize(8 * 1024);
    cfg.setSegmentUnitSize(cfg.getPhysicalPageSize() * 1024);

    ArchiveOptions
        .initContants(cfg.getPageSize(), cfg.getSegmentUnitSize(),
            cfg.getFlexibleCountLimitInOneArchive());

    counterLoadFromL2.set(0);
    counterLoadFromStorage.set(0);
    counterFlushToL2.set(0);
    counterFlushToStorage.set(0);
    counterNotEnoughL2Page.set(0);
    counterCancelled.set(0);
  }

  @After
  public void afterMethod() {
    logger.info("after method");
    if (pageManager != null) {
      try {
        pageManager.close();
        pageManager = null;
      } catch (Exception e) {
        logger.warn("caught an exception", e);
      }
    }

    factory.delete();
    if (ssdFile.exists()) {
      ssdFile.delete();
    }

    if (rawFile.exists()) {
      rawFile.delete();
    }
  }

  public void generatePageManager(int numPages) throws Exception {
    generatePageManager(numPages, DEFAULT_RAW_STORAGE_TIMES_OF_PAGE, null);
  }

  @SuppressWarnings("unchecked")
  public void generatePageManager(int numPages, int numRawPages, AsyncStorageTest ssdStorage)
      throws Exception {
    rawStorage = new AsyncStorageTest(
        (AsyncStorage) factory.setSize(numRawPages * cfg.getPhysicalPageSize())
            .generate(rawFile.getAbsolutePath()));

    ArrayList<Page> pages = new ArrayList<Page>();
    ByteBuffer byteBuffer = DirectAlignedBufferAllocator
        .allocateAlignedByteBuffer((int) cfg.getPhysicalPageSize() * numPages);
    for (int i = 0; i < numPages; i++) {
      byteBuffer.position(i * (int) cfg.getPhysicalPageSize());
      byteBuffer.limit(byteBuffer.position() + (int) cfg.getPhysicalPageSize());
      pages.add(new MemoryPageImpl(byteBuffer.slice()));
    }

    l2WriteCacheStorage = new AsyncStorageTest(
        (AsyncStorage) factory.setSize(numRawPages * cfg.getPhysicalPageSize())
            .generate(rawFile.getAbsolutePath()));

    pageManager = new PageManagerImplTest(pages,
        cfg, "L1");
    RawArchive rawArchive = Mockito.mock(RawArchive.class);
   
    when(rawArchive.getStorage()).thenReturn(rawStorage);
   
   
    pageManager.start();
  }

  @SuppressWarnings("unchecked")
  public void generatePageManager(int numPages, int numL2Blocks, int numRawPages) throws Exception {
    ssdStorage = new AsyncStorageTest((AsyncStorage) factory.setSize(
        ArchiveType
            .getArchiveHeaderLength()).generate(ssdFile.getAbsolutePath()));
    Assert.assertTrue(ssdStorage.size() != 0);
    generatePageManager(numPages, numRawPages, ssdStorage);
  }

  public void correctPage(PageAddress pageAddress, ByteBuffer byteBuffer) throws Exception {
    PageContext<Page> context = contextFactory
        .generateCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_INTERNAL_CORRECTION,
            cfg.getDefaultPageRequestTimeoutMs());
    pageManager.checkout(context);
    context.waitFor();
    Assert.assertTrue(context.isSuccess());
    Page page = context.getPage();
    page.write(0, byteBuffer);
    page.setPageLoaded(true);
    pageManager.checkin(context);
  }

  public void failToCorrectPage(PageAddress pageAddress) throws Exception {
    PageContext<Page> context = contextFactory
        .generateCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_INTERNAL_CORRECTION,
            cfg.getDefaultPageRequestTimeoutMs());
    pageManager.checkout(context);
    context.waitFor();
    Assert.assertTrue(context.isSuccess());
    pageManager.checkin(context);
  }

  public void writePage(PageAddress pageAddress) throws Exception {
    writePage(pageAddress, DataConsistencyUtils.generateDataByTime(cfg.getPageSize()));
  }

  public void writePage(PageAddress pageAddress, ByteBuffer buffer) throws Exception {
    PageContext<Page> context = contextFactory
        .generateCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_INTERNAL_WRITE,
            cfg.getDefaultPageRequestTimeoutMs());
    pageManager.checkout(context);
    context.waitFor();
    Assert.assertTrue(context.isSuccess());
    context.getPage().write(0, buffer);
    pageManager.checkin(context);
  }

  public ByteBuffer readPage(PageAddress pageAddress, boolean external) throws Exception {
    PageContext<Page> context = null;
    try {
      if (external) {
        context = contextFactory
            .generateCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_EXTERNAL_READ,
                cfg.getDefaultPageRequestTimeoutMs());
      } else {
        context = contextFactory.generateCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_READ,
            cfg.getDefaultPageRequestTimeoutMs());
      }
      pageManager.checkout(context);
      context.waitFor();
      Assert.assertTrue(context.isSuccess());
      ByteBuffer buffer = ByteBuffer.allocate(cfg.getPageSize());
      context.getPage().getData(0, buffer.duplicate());
      return buffer;
    } finally {
      pageManager.checkin(context);
    }
  }

  public void readPage(PageAddress pageAddress, boolean external, boolean needCheck)
      throws Exception {
    PageContext<Page> context = null;
    try {
      if (external) {
        context = contextFactory
            .generateCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_EXTERNAL_READ,
                cfg.getDefaultPageRequestTimeoutMs());
      } else {
        context = contextFactory.generateCheckoutContext(pageAddress, TaskType.CHECK_OUT_FOR_READ,
            cfg.getDefaultPageRequestTimeoutMs());
      }
      pageManager.checkout(context);
      context.waitFor();
      Assert.assertTrue(context.isSuccess());
      if (needCheck) {
        Validate.isTrue(DataConsistencyUtils.checkDataByTime(context.getPage().getReadOnlyView()));
      }
    } finally {
      pageManager.checkin(context);
    }
  }

  public void flushPage(PageAddress address) throws Exception {
    PageContext<Page> context = contextFactory
        .generatePageFlushContext(address, new CountDownLatch(1));
    pageManager.flushPage(context);
    context.waitFor();
  }

  public class PageManagerImplTest extends PageManagerImpl {
    public PageManagerImplTest(List<Page> pages,
        DataNodeConfiguration cfg,
        String tag) {
      super(pages, cfg, tag, null);
    }

    public PageManagerImplTest(List<Page> pages,
        DataNodeConfiguration cfg,
        String tag, StorageIoDispatcher storageIoDispatcher) {
      super(pages, cfg, tag, storageIoDispatcher);
    }

    @Override
    public void loadedFromStorage(PageContext<Page> context) {
      counterLoadFromStorage.incrementAndGet();
      super.loadedFromStorage(context);
      logger.warn("loadedFromStorage");
    }

    @Override
    public void flushedToStorage(PageContext<Page> context) {
      counterFlushToStorage.incrementAndGet();
      super.flushedToStorage(context);
      logger.warn("flushedToStorage");
    }
  }

  public class AsyncStorageTest extends AsyncStorage {
    public volatile boolean needWriteThrowException = false;
    public volatile boolean needReadThrowException = false;
    public volatile boolean hasException = false;
    public volatile int sleepTimeMs = 0;
    private AsyncStorage storage;

    public AsyncStorageTest(AsyncStorage storage) {
      super(storage.identifier());
      this.storage = storage;
      this.ioDepth = storage.getIoDepth();
    }

    private void needSleep() {
      try {
        if (sleepTimeMs > 0) {
          Thread.sleep(sleepTimeMs);
        }
      } catch (InterruptedException e) {
        logger.error("", e);
      }
    }

    @Override
    public void close() throws StorageException {
      storage.close();
    }

    @Override
    public void open() throws StorageException {
      super.open();
    }

    @Override
    public void read(long pos, byte[] dstBuf, int off, int len) throws StorageException {
      if (needReadThrowException) {
        hasException = true;
        throw new StorageException("read 1 offset: " + pos);
      } else {
        needSleep();
        storage.read(pos, dstBuf, off, len);
      }
    }

    @Override
    public <A> void read(ByteBuffer buffer, long pos, A attachment,
        CompletionHandler<Integer, ? super A> handler) throws StorageException {
      if (needReadThrowException) {
        hasException = true;
        handler.failed(new StorageException("read failure"), attachment);
      } else {
        needSleep();
        storage.read(buffer, pos, attachment, handler);
      }
    }

    @Override
    public void read(long pos, ByteBuffer buffer) throws StorageException {
      if (needReadThrowException) {
        hasException = true;
        throw new StorageException("read 2 offset: " + pos);
      } else {
        needSleep();
        storage.read(pos, buffer);
      }
    }

    @Override
    public void write(long pos, ByteBuffer buffer) throws StorageException {
      if (needWriteThrowException) {
        hasException = true;
        throw new StorageException("write 2 offset: " + pos);
      } else {
        needSleep();
        storage.write(pos, buffer);
      }
    }

    @Override
    public <A> void write(ByteBuffer buffer, long pos, A attachment,
        CompletionHandler<Integer, ? super A> handler) throws StorageException {
      if (needWriteThrowException) {
        hasException = true;
        handler.failed(new StorageException("write failure"), attachment);
      } else {
        needSleep();
        storage.write(buffer, pos, attachment, handler);
      }
    }

    @Override
    public void write(long pos, byte[] buf, int off, int len) throws StorageException {
      if (needWriteThrowException) {
        hasException = true;
        throw new StorageException("write 1 offset: " + pos);
      } else {
        needSleep();
        storage.write(pos, buf, off, len);
      }
    }

    @Override
    public long size() {
      return storage.size();
    }

  }
}
