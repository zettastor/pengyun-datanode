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

package py.datanode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyListener;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import py.RequestResponseHelper;
import py.app.context.DummyInstanceIdStore;
import py.archive.ArchiveOptions;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitType;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.connection.pool.udp.UDPEchoServer;
import py.connection.pool.udp.UdpServer;
import py.datanode.archive.ArchiveInitializer;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveBuilder;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.configuration.RocksDbPathConfig;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;
import py.datanode.segment.datalog.persist.LogFileSystem;
import py.datanode.segment.datalog.persist.LogStorageSystem;
import py.datanode.service.DataNodeServiceImpl;
import py.datanode.storage.impl.RandomAccessFileStorageFactory;
import py.datanode.storage.impl.StorageBuilder;
import py.datanode.test.DataNodeConfigurationForTest;
import py.exception.ChecksumMismatchedException;
import py.exception.StorageException;
import py.icshare.GroupFileStore;
import py.instance.DummyInstanceStore;
import py.instance.Group;
import py.instance.InstanceId;
import py.instance.PortType;
import py.netty.core.NetworkDetect;
import py.netty.memory.SimplePooledByteBufAllocator;
import py.storage.Storage;
import py.test.TestBase;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.DataNodeService;
import py.volume.CacheType;
import py.volume.VolumeType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JNotify.class, NetworkDetect.class, UDPEchoServer.class})
@SuppressStaticInitializationFor({"net.contentobjects.jnotify.JNotify",
    "py.connection.pool.udp.UDPEchoServer"})
public class DataNodeAppEngineTest extends TestBase {
  private static AtomicInteger ioPort = new AtomicInteger(55002);
  private SegId segId = new SegId(1L, 0);
  private String persistentRoot = "/tmp/datanode_app_test/var/storage";
  private int port = 8001;
  private long instanceId1 = 11111;
  private DataNodeConfiguration cfg;

  public DataNodeAppEngineTest() throws Exception {
    try {
      PowerMockito.mockStatic(JNotify.class);
    } catch (UnsatisfiedLinkError e) {
      logger.warn("can not mock jnotify", e);
    }

    try {
      PowerMockito.mockStatic(UDPEchoServer.class);
    } catch (UnsatisfiedLinkError e) {
      logger.warn("can not mock udp server", e);
    }

    Random random = new Random();
    PowerMockito.doReturn(random.nextInt(255))
        .when(JNotify.class, "addWatch", any(String.class), anyInt(), anyBoolean(),
            any(JNotifyListener.class));
    PowerMockito.mockStatic(NetworkDetect.class);
    PowerMockito.doReturn(true).when(NetworkDetect.class, "myselfAlive", any(String.class));
    super.init();
    TlsfByteBufferManagerFactory.init(512, 1024 * 1024 * 2, true);
  }

  public static RawArchive generateRawArchive(DataNodeConfiguration cfg, Storage storage,
      boolean firstStart,
      boolean justLoadingExistingArchive, boolean overwrite, boolean runInRealTime)
      throws StorageException, IOException, ChecksumMismatchedException, Exception {
    TlsfByteBufferManagerFactory.init(512, 1024 * 1024 * 10, true);

    RawArchiveBuilder builder = new RawArchiveBuilder(cfg, storage);
    builder.setJustloadingExistArchive(justLoadingExistingArchive);
   
   
    builder.setFirstTimeStart(firstStart);
   
    builder.setRunInRealTime(runInRealTime);
    builder.setArchiveType(ArchiveType.RAW_DISK);
    builder.setDevName("dev" + System.currentTimeMillis());
    builder.setStorageType(StorageType.PCIE);
    builder.setStoragePath(storage.identifier());
    builder.setSerialNumber(String.valueOf(System.currentTimeMillis()));
    String user = System.getProperty("user.name");
    builder.setCurrentUser(user != null ? user : "unknown");

    builder.setOverwrite(overwrite);

    logger.warn("initialization archive builder={}", builder);
    RawArchive archive = (RawArchive) builder.build();
    archive.setStoragePoolId(TestBase.DEFAULT_STORAGE_POOL_ID);
    return archive;
  }

  @Before
  public void beforeMethod() throws IOException {
    File dir = new File(persistentRoot);
    if (dir.exists()) {
      try {
        FileUtils.deleteDirectory(dir);
      } catch (IOException e) {
        logger.error("caught an exception", e);
      }
    }

    dir = new File("/tmp/datanode_app_test");
    if (dir.exists()) {
      try {
        FileUtils.deleteDirectory(dir);
      } catch (IOException e) {
        logger.error("caught an exception", e);
      }
    }

    cfg = new DataNodeConfigurationForTest();
    LogPersistRocksDbConfiguration logPersistRocksDbConfiguration = 
        new LogPersistRocksDbConfiguration();
    RocksDbPathConfig rocksDbPathConfig = new RocksDbPathConfig(cfg,
        logPersistRocksDbConfiguration);
    cfg.setRocksDbPathConfig(rocksDbPathConfig);
    cfg.getRocksDbPathConfig().setWcRocksDbPath("/tmp/datanode_app_test/wc");
    cfg.getRocksDbPathConfig().setIndexerRocksDbPath("/tmp/datanode_app_test/indexer");
    cfg.getRocksDbPathConfig().setLogPersistRocksDbPath("/tmp/datanode_app_test/log");
    cfg.getRocksDbPathConfig().setPartitionDir("/tmp/datanode_app_test");
    cfg.getArchiveConfiguration().setPersistRootDir(persistentRoot);

    createUnsetDir(cfg, persistentRoot);
  }

  @Test
  public void testGroupsAreAllSameWithFileStore() throws Exception {
    List<Storage> storages = mockStorages(cfg, 2);
    assertTrue(storages.size() == 2);

    long archiveId = 1111111L;
    long instanceId = 123456L;

    writeArchiveMetadata(cfg, storages.get(0), "raw0", archiveId, new InstanceId(instanceId),
        cfg.getSegmentUnitSize(), cfg.getPageSize(), null);
    writeArchiveMetadata(cfg, storages.get(1), "raw1", archiveId + 1, new InstanceId(instanceId),
        cfg.getSegmentUnitSize(), cfg.getPageSize(), null);
    DataNodeAppEngine engine = null;

    try {
      engine = startDataNodeEngine(cfg, instanceId1, null);
    } catch (Exception e) {
      logger.info("caught an exception ", e);
      fail("start datanode engine failure" + e);
    }

    assertEquals(2, engine.getRawArchiveManager().getRawArchives().size());
    for (RawArchive archive : engine.getRawArchiveManager().getRawArchives()) {
      assertEquals(null, archive.getArchiveMetadata().getGroup());
    }
    assertEquals(null, engine.getContext().getGroup());
    engine.stop();
  }

  @Test
  public void testGroupsAreSameButFileStoreHasValue() throws Exception {
    List<Storage> storages = mockStorages(cfg, 2);
    assertTrue(storages.size() == 2);

    long archiveId = 1111111L;
    long instanceId = 123456L;

    writeArchiveMetadata(cfg, storages.get(0), "raw0", archiveId, new InstanceId(instanceId),
        cfg.getSegmentUnitSize(), cfg.getPageSize(), null);
    writeArchiveMetadata(cfg, storages.get(1), "raw1", archiveId + 1, new InstanceId(instanceId),
        cfg.getSegmentUnitSize(), cfg.getPageSize(), null);
    DataNodeAppEngine engine = null;
    Group group = new Group(1);
    try {
      engine = startDataNodeEngine(cfg, instanceId1, group);
    } catch (Exception e) {
      logger.info("caught an exception ", e);
      fail("start datanode engine failure" + e);
    }

    try {
      assertEquals(engine.getRawArchiveManager().getRawArchives().size(), 2);
      for (RawArchive archive : engine.getRawArchiveManager().getRawArchives()) {
        assertEquals(null, archive.getArchiveMetadata().getGroup());
      }
      assertEquals(group, engine.getContext().getGroup());
    } finally {
      engine.stop();
    }

  }

  @Test
  public void startAndStopDataNodeAndTryCreateSegmentUnit() throws Exception {
   
    initRawDisksAndMmbp(false);

    DataNodeAppEngine engine = startDataNodeEngine(cfg, instanceId1, null);
    try {
      engine.setAllocator(SimplePooledByteBufAllocator.DEFAULT);
      DataNodeServiceImpl dataNodeService = engine.getDataNodeService();
      assertEquals(0, dataNodeService.getSegmentUnitManager().get().size());
      CreateSegmentUnitRequest request = RequestResponseHelper
          .buildCreateSegmentUnitRequest(segId, VolumeType.REGULAR, CacheType.NONE,
              instanceId1,
              TestBase.DEFAULT_STORAGE_POOL_ID, SegmentUnitType.Normal, instanceId1 + 1,
              instanceId1 + 2);
      DataNodeService.Iface client = GenericThriftClientFactory.create(DataNodeService.Iface.class)
          .generateSyncClient(engine.getContext().getMainEndPoint(), 100000);
      try {
        client.createSegmentUnit(request);
      } catch (Exception e) {
        logger.error("caught an exception", e);
        fail();
      }

      assertEquals(1, dataNodeService.getSegmentUnitManager().get().size());
      engine.stop();
      Thread.sleep(2000);

      engine.start();
     
      assertEquals(1, dataNodeService.getSegmentUnitManager().get().size());
      dataNodeService.shutdown();
    } finally {
      engine.stop();
    }

    logger.info("stop all thread");
  }

  @Test
  public void testFileStoreSameWithOneArchive() throws Exception {
    List<Storage> storages = mockStorages(cfg, 2);
    assertTrue(storages.size() == 2);

    long archiveId = 1111111L;
    long instanceId = 123456;

    writeArchiveMetadata(cfg, storages.get(0), "raw0", archiveId, new InstanceId(instanceId),
        cfg.getSegmentUnitSize(), cfg.getPageSize(), new Group(1));
    writeArchiveMetadata(cfg, storages.get(1), "raw1", archiveId + 1, new InstanceId(instanceId),
        cfg.getSegmentUnitSize(), cfg.getPageSize(), new Group(2));
    DataNodeAppEngine engine = null;
    try {
      engine = startDataNodeEngine(cfg, instanceId1, new Group(1));
    } catch (Exception e) {
      logger.info("caught an exception ", e);
      fail("start datanode engine failure" + e);
    }

    try {
      assertTrue(engine.getRawArchiveManager().getRawArchives().size() == 2);
      for (RawArchive archive : engine.getRawArchiveManager().getRawArchives()) {
        if (archive.getArchiveId() == archiveId) {
          assertEquals(new Group(1), archive.getArchiveMetadata().getGroup());
        } else if (archive.getArchiveId() == archiveId + 1) {
          assertEquals(new Group(2), archive.getArchiveMetadata().getGroup());
        }
      }
      assertEquals(new Group(1), engine.getContext().getGroup());
    } finally {
      engine.stop();
    }

  }

  @Test
  public void startAndStopDataNodeAndTryCreateSegmentUnitWithSsd() throws Exception {
    final int ramDiskSize = 1 * 1024 * 1024;
    cfg.setStartFlusher(true);
    cfg.setPageSystemMemoryCacheSize(Integer.toString(ramDiskSize));
    
   
    initRawDisksAndMmbp(true);

    DataNodeAppEngine engine = startDataNodeEngine(cfg, instanceId1, null);
    engine.setAllocator(SimplePooledByteBufAllocator.DEFAULT);
    DataNodeServiceImpl dataNodeService = engine.getDataNodeService();

    assertEquals(0, dataNodeService.getSegmentUnitManager().get().size());
    CreateSegmentUnitRequest request = RequestResponseHelper
        .buildCreateSegmentUnitRequest(segId, VolumeType.REGULAR, CacheType.NONE,
            instanceId1,
            TestBase.DEFAULT_STORAGE_POOL_ID, SegmentUnitType.Normal, instanceId1 + 1,
            instanceId1 + 2);

    DataNodeService.Iface client = GenericThriftClientFactory.create(DataNodeService.Iface.class)
        .generateSyncClient(engine.getContext().getEndPointByServiceName(PortType.CONTROL), 100000);
    try {
      client.createSegmentUnit(request);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      fail();
    }

    Thread.sleep(2000);
    assertEquals(1, dataNodeService.getSegmentUnitManager().get().size());
   

    engine.stop();

    cfg.setPageSystemMemoryCacheSize(Integer.toString(ramDiskSize * 4));

    engine.start();

    int totalPageCount = ramDiskSize * 4 / (int) cfg.getPhysicalPageSize();
   

    assertEquals(1, dataNodeService.getSegmentUnitManager().get().size());

    engine.stop();
  }

  @Test
  public void testReadLog() {
    LogPersistingConfiguration configuration = new LogPersistingConfiguration();
    configuration.setRootDirName("/tmp/datalog");
    try {
      LogStorageSystem dataLogStorageSystem = new LogFileSystem(configuration,
          new MutationLogReaderWriterFactory(configuration));
      logger.info("log storage system: {}", dataLogStorageSystem);
    } catch (IOException e) {
      logger.error("Caught an exception", e);
      assertTrue(false);
    } catch (RuntimeException e) {
      logger.error("Caught an exception", e);
      assertTrue(false);
    }
  }

  public List<Storage> mockStorages(DataNodeConfiguration cfg, int count) throws Exception {
   
    int diskSize = (int) (ArchiveOptions.SEGMENTUNIT_METADATA_REGION_OFFSET
        + ArchiveOptions.ALL_FLEXIBLE_LENGTH + cfg.getSegmentUnitSize() * 3);
    for (int i = 0; i < count; i++) {
      createRamAndRawDisk(cfg, persistentRoot, "raw" + i, diskSize, false);
    }

    List<Storage> storages = StorageBuilder.getFileStorage(
        FileSystems.getDefault()
            .getPath(persistentRoot, cfg.getArchiveConfiguration().getDataArchiveDir())
            .toFile(), new RandomAccessFileStorageFactory());

    return storages;
  }

  public DataNodeAppEngine startDataNodeEngine(DataNodeConfiguration cfg, long instanceId,
      Group group)
      throws Exception {
    LogPersistingConfiguration logPersistingConfiguration = new LogPersistingConfiguration();
    LogPersistRocksDbConfiguration logPersistRocksDbConfiguration = 
        new LogPersistRocksDbConfiguration();
    logPersistingConfiguration.setRootDirName(persistentRoot + "/dataLog");

    logPersistRocksDbConfiguration.setRocksDbPathConfig(cfg.getRocksDbPathConfig());
    DummyInstanceIdStore instanceIdStore = new DummyInstanceIdStore();
    instanceIdStore.setInstanceId(new InstanceId(instanceId));

    DataNodeContext context = new DataNodeContext("testing");
    context.putEndPoint(PortType.CONTROL, new EndPoint(null, port));
    context.putEndPoint(PortType.IO, new EndPoint(null, ioPort.incrementAndGet()));
    logger.info("io endpoint: {}", context.getEndPointByServiceName(PortType.IO));
    context.setPersistenceRoot(persistentRoot);
    context.setInstanceIdStore(instanceIdStore);
    GroupFileStore groupStore = new GroupFileStore(persistentRoot, "DataNode");
    groupStore.persistGroup(group);
    context.setGroupStore(groupStore);

    DataNodeServiceImpl dataNodeService = new DataNodeServiceImpl();
    DataNodeAppEngine engine = new DataNodeAppEngine(dataNodeService);
    engine.setAllocator(SimplePooledByteBufAllocator.DEFAULT);
    engine.setContext(context);
    engine.setDataNodeCfg(cfg);
    engine.setDataNodeLogPersistingCfg(logPersistingConfiguration);
    engine.setDataNodeLogPersistingRocksDbCfg(logPersistRocksDbConfiguration);
    engine.setInstanceStore(new DummyInstanceStore());
    engine.setUdpServer(mock(UdpServer.class));

    try {
      engine.start();
    } catch (Exception e) {
      logger.warn("caught an exception", e);
      throw new Exception();
    }
    engine.getRawArchiveManager().clearArchiveStatusChangeFinishListener();
   
    for (RawArchive archiveBase : engine.getRawArchiveManager().getRawArchives()) {
      if (archiveBase.getArchiveStatus() == ArchiveStatus.CONFIG_MISMATCH) {
        continue;
      }
      archiveBase.setArchiveStatus(ArchiveStatus.GOOD);
      archiveBase.setStoragePoolId(TestBase.DEFAULT_STORAGE_POOL_ID);
      archiveBase.persistMetadata();
    }

    return engine;
  }

  private void createRamAndRawDisk(DataNodeConfiguration config, String persistentRoot,
      String devName,
      int storageSize, boolean isSsd) throws Exception {
    Path path = null;
    path = FileSystems.getDefault()
        .getPath(persistentRoot, config.getArchiveConfiguration().getDataArchiveDir(), devName);

    byte[] data = new byte[storageSize];
    FileUtils.writeByteArrayToFile(path.toFile(), data);
  }

  private void createUnsetDir(DataNodeConfiguration config, String persistentRoot)
      throws IOException {
    Path path = FileSystems.getDefault()
        .getPath(persistentRoot, config.getArchiveConfiguration().getUnsettledArchiveDir());

    FileUtils.forceMkdir(path.toFile());
  }

  private void writeArchiveMetadata(DataNodeConfiguration config, Storage storage, String devName,
      Long archiveId,
      InstanceId instanceId, long segmentSize, int pageSize, Group group) throws Exception {
   
    ArchiveInitializer archiveInitializer = null;

    config.setPageSize(pageSize);
    config.setSegmentUnitSize(segmentSize);

    RawArchive archive = generateRawArchive(config, storage, true, false, true, false);
    RawArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
    archiveMetadata.setArchiveId(archiveId);
    archiveMetadata.setDeviceName(devName);
    archiveMetadata.setInstanceId(instanceId);
    archiveMetadata.setGroup(group);
    RawArchiveBuilder.initArchiveMetadataWithStorageSizeNewVersion(archiveMetadata.getArchiveType(),
        storage.size(), archiveMetadata);
    archive.persistMetadata();
  }

  private void initRawDisksAndMmbp(boolean enableSsd) throws Exception {
    Path rawDiskPath = FileSystems.getDefault()
        .getPath(persistentRoot, cfg.getArchiveConfiguration().getDataArchiveDir(), "raw1");
    int pageCount = (int) (cfg.getSegmentUnitSize() * 20 / cfg.getPageSize());
    for (int i = 0; i < pageCount; i++) {
      FileUtils
          .writeByteArrayToFile(rawDiskPath.toFile(), new byte[(int) cfg.getPhysicalPageSize()],
              true);
    }

    ArchiveInitializer archiveInitializer = null;
    Collection<Storage> storages = StorageBuilder.getFileStorage(
        FileSystems.getDefault()
            .getPath(persistentRoot, cfg.getArchiveConfiguration().getDataArchiveDir())
            .toFile(), new RandomAccessFileStorageFactory());
    assertEquals(1, storages.size());

    Storage storageEm = null;
    ObjectMapper objectMapper = new ObjectMapper();

    storageEm = storages.iterator().next();
    
    RawArchive archive = generateRawArchive(cfg, storageEm, true, false, true, false);
    RawArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
    archiveMetadata.setArchiveId(11111L);
    archiveMetadata.setDeviceName("raw1");
    archiveMetadata.setInstanceId(new InstanceId(11111L));
    archiveMetadata.setPageSize(cfg.getPageSize());
    archiveMetadata.setSegmentUnitSize(cfg.getSegmentUnitSize());
    archiveMetadata.setStoragePoolId(TestBase.DEFAULT_STORAGE_POOL_ID);
    RawArchiveBuilder.initArchiveMetadataWithStorageSizeNewVersion(archiveMetadata.getArchiveType(),
        storageEm.size(), archiveMetadata);
   
  }
}
