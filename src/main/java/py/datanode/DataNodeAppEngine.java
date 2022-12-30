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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.app.thrift.ThriftAppEngine;
import py.archive.ArchiveOptions;
import py.archive.ArchiveStatus;
import py.archive.PluginPlugoutManager;
import py.archive.disklog.DiskErrorLogManager;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.recurring.SegmentUnitProcessorFactory;
import py.archive.segment.recurring.SegmentUnitTaskCallback;
import py.archive.segment.recurring.SegmentUnitTaskContextFactory;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.archive.segment.recurring.SegmentUnitTaskExecutorImpl;
import py.archive.segment.recurring.SegmentUnitTaskExecutorWrapper;
import py.archive.segment.recurring.SegmentUnitTaskType;
import py.client.thrift.GenericThriftClientFactory;
import py.common.FastBufferManagerProxy;
import py.common.NamedThreadFactory;
import py.common.PrimaryTlsfFastBufferManager;
import py.common.SyncLogTlsfFastBufferManager;
import py.common.TlsfFileBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.connection.pool.udp.UdpServer;
import py.connection.pool.udp.detection.DefaultDetectionTimeoutPolicyFactory;
import py.connection.pool.udp.detection.NetworkIoHealthChecker;
import py.connection.pool.udp.detection.UdpDetectorImpl;
import py.datanode.alarm.AlarmReporterImpl;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveManager;
import py.datanode.archive.RawArchiveManagerImpl;
import py.datanode.archive.UnsettledArchive;
import py.datanode.archive.UnsettledArchiveBuild;
import py.datanode.archive.UnsettledArchiveManager;
import py.datanode.archive.UnsettledArchiveManagerImpl;
import py.datanode.archive.disklog.DiskErrorLogManagerImpl;
import py.datanode.archive.disklog.StorageExceptionPersister;
import py.datanode.checksecondaryinactive.CheckSecondaryInactiveByTime;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.page.MultiChunkAddressHelper;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.page.impl.StorageIoDispatcher;
import py.datanode.page.impl.WrappedPageManagerFactory;
import py.datanode.segment.PersistDataToDiskEngineImpl;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitCanDeletingCheckImpl;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.SegmentUnitManagerImpl;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.MutationLogEntrySaveProxy;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.MutationLogManagerImpl;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.broadcast.NotInsertedLogs;
import py.datanode.segment.datalog.persist.LogPersister;
import py.datanode.segment.datalog.persist.LogPersisterAndReaderFactory;
import py.datanode.segment.datalog.persist.LogStorageReader;
import py.datanode.segment.datalog.persist.full.impl.TempFileLogPersister;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.segment.datalog.sync.log.reduce.AbstractPbSyncLogReduceBuilderFactory;
import py.datanode.segment.datalog.sync.log.reduce.BackwardSyncLogRequestReduceBuilderFactory;
import py.datanode.segment.datalog.sync.log.reduce.BackwardSyncLogResponseReduceBuilderFactory;
import py.datanode.segment.datalog.sync.log.reduce.PbAsyncLogsBatchRequestReduceBuilderFactory;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogMessageType;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollector;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollectorImpl;
import py.datanode.segment.datalogbak.catchup.CatchupLogContextFactory;
import py.datanode.segment.datalogbak.catchup.PlalDriver;
import py.datanode.segment.datalogbak.catchup.SegmentUnitDataLogProcessorFactory;
import py.datanode.segment.heartbeat.HostHeartbeat;
import py.datanode.segment.heartbeat.HostHeartbeatImpl;
import py.datanode.segment.membership.statemachine.StateProcessingContextFactory;
import py.datanode.segment.membership.statemachine.StateProcessorFactory;
import py.datanode.service.DataNodeServiceImpl;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.datanode.service.io.throttle.IoThrottleManagerImpl;
import py.datanode.service.worker.MembershipPusher;
import py.datanode.service.worker.PersistSegUnitMetadataWorker;
import py.datanode.storage.impl.StorageBuilder;
import py.datanode.utils.ShutdownStatusDecider;
import py.datanode.utils.ShutdownStatusDecider.DataNodeShutdownStatus;
import py.datanode.worker.CheckLogSpaceWorker;
import py.datanode.worker.CheckStoragesWorkerFactory;
import py.datanode.worker.JustCreatedSegUnitsReporter;
import py.datanode.worker.NetworkHealthChecker;
import py.datanode.worker.PluginPlugoutManagerImpl;
import py.dih.client.DihClientFactory;
import py.engine.TaskEngine;
import py.exception.ArchiveTypeMismatchException;
import py.exception.InternalErrorException;
import py.exception.StorageException;
import py.icshare.BackupDbReporter;
import py.icshare.BackupDbReporterImpl;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.InstanceStore;
import py.monitor.jmx.server.JmxAgent;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.periodic.UnableToStartException;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogResponseUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.querylog.filehandle.FileHandleManagerImpl;
import py.storage.Storage;
import py.storage.StorageExceptionHandlerChain;
import py.storage.impl.AsyncStorage;
import py.storage.impl.AsynchronousFileChannelStorageFactory;
import py.storage.impl.PriorityStorageImpl;
import py.thrift.datanode.service.DataNodeService;
import py.token.controller.TokenControllerCenter;

public class DataNodeAppEngine extends ThriftAppEngine {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeAppEngine.class);
  private static final int DEFAULT_POSSIBLE_SEGMENT_UNIT_COUNT = 6000;
 
  private final DataNodeServiceImpl dataNodeService;
 
  private StorageExceptionHandlerChain storageExceptionHandlerChain =
      new StorageExceptionHandlerChain();
  private SegmentUnitManager segMetadataManager;
  private ShutdownStatusDecider shutdownStatusDecider;
  private PageManager<Page> memoryPageManager;
  private FastBufferManagerProxy fastBufferManagerProxy;
  private FastBufferManagerProxy fastBufferManagerProxyForSecondary;
  private FastBufferManagerProxy fastBufferManagerProxyForSyncLog;
  private FastBufferManagerProxy fileBufferManagerProxy;
  private UnsettledArchiveManager unsettledArchiveManager;

  private Storage ssdStorage;
  private RawArchiveManager rawArchiveManager;
  private InformationReporter informationReporter;
  private MutationLogManager mutationLogManager;
  private LogPersister logPersister;
  private LogStorageReader logReader;
  private LogPersisterAndReaderFactory logPersisterAndReaderFactory;
  private SegmentUnitTaskExecutor catchupLogEngine;
  private SegmentUnitTaskExecutor stateProcessingEngine;
  private GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory;
  private GenericThriftClientFactory<DataNodeService.Iface> heartBeatDataNodeSyncClientFactory;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory;
  private PeriodicWorkExecutorImpl checkDiskExecutor;
  private PeriodicWorkExecutorImpl checkLogSpaceExecutor;
  private PeriodicWorkExecutorImpl checkSlowDiskAwaitExecutor;
  private PeriodicWorkExecutorImpl checkPendingIoWorkExecutor;
  private PeriodicWorkExecutorImpl persistSegUnitMetadataExecutor;
  private PeriodicWorkExecutorImpl checkNetworkHealthExecutor;
  private PeriodicWorkExecutorImpl logCounter;
  private JustCreatedSegUnitsReporter justCreatedSegUnitsReporter;
  private HostHeartbeat hostHeartbeat;
  private TempFileLogPersister tempFileLogPersister;
  private IoThrottleManager ioThrottleManager;
  private AlarmReporterImpl alarmReporter;

  private InstanceStore instanceStore;
  private DataNodeConfiguration dataNodeCfg;
  private LogPersistingConfiguration dataNodeLogPersistingCfg;
  private LogPersistRocksDbConfiguration dataNodeLogPersistingRocksDbCfg;

  private InformationCenterClientFactory inforCenterClientFactory = null;
  private DihClientFactory dihClientFactory = null;
  private StorageExceptionPersister persister = null;
  private PlalEngine plalEngine = null;
  private TaskEngine copyPageTaskEngine;
  private MutationLogEntrySaveProxy saveLogProxy;
  private JmxAgent jmxAgent;
  private BackupDbReporter backupDbReporter;
  private ByteBufAllocator allocator = null;

  private HashedWheelTimer priorityStorageHashedWheelTimer = null;
  private UdpServer udpServer = null;
  private PluginPlugoutManagerImpl pluginPlugoutManagerImpl;
  private SegmentUnitCanDeletingCheckImpl segmentUnitCanDeletingCheck;
  private boolean arbiterOnly = false;

  private Timer reduceTimer;
  private SyncLogReduceCollector<PbBackwardSyncLogRequestUnit,
      PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest> backwardSyncLogRequestReduceCollector;
  private SyncLogReduceCollector<PbBackwardSyncLogResponseUnit,
      PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest> backwardSyncLogResponseReduceCollector;
  private SyncLogReduceCollector<PbAsyncSyncLogBatchUnit,
      PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest> syncLogBatchRequestReduceCollector;
 
  private SyncLogTaskExecutor syncLogTaskExecutor;

  public DataNodeAppEngine(DataNodeServiceImpl dataNodeService) {
    super(dataNodeService);
    this.dataNodeService = dataNodeService;
  }

  public static void processAllLogsExit(MutationLogManager mutationLogManager,
      SegmentUnitManager segMetadataManager,
      PlalEngine plalEngine, int timeoutSec) {
    if (mutationLogManager == null) {
      logger.warn("mutationLogManager is null, should check it");
      return;
    }

    long waitTimeForLogDriverMs = timeoutSec * 1000;
    long beginTime = System.currentTimeMillis();
    List<SegmentLogMetadata> segmentLogMetadatas = mutationLogManager.getSegments(null);
    do {
      int finishedCount = 0;
      for (SegmentLogMetadata logMetadata : segmentLogMetadatas) {
        SegId segId = logMetadata.getSegId();
        SegmentUnit segUnit = segMetadataManager.get(segId);
       
       
       
       
       
       

        MigrationStatus migrationStatus = segUnit.getSegmentUnitMetadata().getMigrationStatus();
        if (migrationStatus.isMigrating()) {
          logger.warn("it isn't migrating for segId: {}", segId);
          finishedCount++;
          continue;
        }

        ArchiveStatus status = segUnit.getArchive().getArchiveStatus();
        if (status == ArchiveStatus.GOOD || status == ArchiveStatus.DEGRADED) {
          int driveCount = PlalDriver.drive(logMetadata, segUnit, plalEngine);
          LogImage logImage = logMetadata.getLogImageIncludeLogsAfterPcl();
          logger.warn("For segment {}, pl, al and cl are not equal: logImage {}, drive={} ", segId,
              logImage,
              driveCount);
          if (driveCount == 0 && logImage.allPointerEqual()) {
            finishedCount++;
            continue;
          }
        } else {
          logger.warn("For segment {}, archive={} is not right", logMetadata.getSegId(),
              segUnit.getArchive());
          finishedCount++;
        }
      }

      if (finishedCount == segmentLogMetadatas.size()) {
        logger.warn("cost time={} for shutdown", System.currentTimeMillis() - beginTime);
        break;
      }

      try {
        Thread.sleep(2000);
      } catch (Exception e) {
        logger.warn("fail to sleep");
      }
    } while (beginTime + waitTimeForLogDriverMs > System.currentTimeMillis());
  }

  public static boolean saveLogsIfNeccessary(MutationLogManager mutationLogManager,
      SegmentUnitManager segMetadataManager, MutationLogEntrySaveProxy proxy) {
    
    List<SegmentLogMetadata> segmentLogMetadatas = mutationLogManager.getSegments(null);
    boolean success = true;
    for (SegmentLogMetadata logMetadata : segmentLogMetadatas) {
      SegId segId = logMetadata.getSegId();
      SegmentUnit segUnit = segMetadataManager.get(segId);

      MigrationStatus migrationStatus = segUnit.getSegmentUnitMetadata().getMigrationStatus();
      if (migrationStatus.isMigrating()) {
        logger.warn("it isn't migrating for segId: {}", logMetadata.getSegId());
        continue;
      }

      List<MutationLogEntry> logsAfterPpl = logMetadata.getLogsAfterPpl();
      logger.warn("@@shutdown segId={} log image: {}", segId,
          logMetadata.getLogImageIncludeLogsAfterPcl());
      if (!logsAfterPpl.isEmpty()) {
        try {
          proxy.saveLogs(segId, logsAfterPpl);
        } catch (IOException e) {
          logger.error("failed to save logs: {} at segId: {}", logsAfterPpl, segId, e);
          success = false;
        }
      }

      List<MutationLogEntry> logsWithNoLogId = Lists
          .newArrayList(logMetadata.getMapLogUuidToLogsWithoutLogId().values());
      if (!logsWithNoLogId.isEmpty()) {
        try {
          proxy.saveLogs(segId, logsWithNoLogId);
        } catch (IOException e) {
          logger.warn("failed to save logs with no logId: {} at segId: {}", logsWithNoLogId, segId);
        }
      }
    }

    return success;
  }

  public void setUdpServer(UdpServer udpServer) {
    this.udpServer = udpServer;
  }

  public void start() throws Exception {
    if (dataNodeCfg.isDisableSystemOutput()) {
      System.setErr(new PrintStream(new OutputStream() {
        public void write(int b) {
        }
      }));
      System.setOut(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) {
        }
      }));
    }
   
   
   
    logger.warn("Allocating {}MB heap from Os", dataNodeCfg.getMemoryInHeapSizeMb());
    int alignedSize = 512;
    TlsfByteBufferManagerFactory
        .init(alignedSize, dataNodeCfg.getMemoryInHeapSizeMb() * 1024 * 1024, true);

    logger.warn("The datanode configuration that we are using is {}", dataNodeCfg);
    if (allocator == null) {
     
      allocator = PooledByteBufAllocatorWrapper.INSTANCE;
    }

    if (!arbiterOnly) {
      CheckSecondaryInactiveByTime
          .initMissCount(dataNodeCfg.getThresholdOfMissingPagesToRequestForNewMember(),
              dataNodeCfg.getThresholdOfMissingLogsToRequestForNewMember());
    }

    final DataNodeContext dataNodeContext = (DataNodeContext) getContext();

    logger.info("Configuring an async client factory to connect to other data nodes");
    logger.warn("datanode  Max Network Frame Size is {}", getMaxNetworkFrameSize());
    dataNodeAsyncClientFactory = GenericThriftClientFactory.create(DataNodeService.AsyncIface.class)
        .withMaxConnectionsPerEndpoint(dataNodeCfg.getMaxConnectionsPerAsyncDataNodeEndPoint())
        .withDefaultSocketTimeout(dataNodeCfg.getDataNodeRequestTimeoutMs())
        .setMaxNetworkFrameSize(getMaxNetworkFrameSize())
        .withMaxChannelPendingSizeMb(dataNodeCfg.getMaxChannelPendingSizeMb());
    heartBeatDataNodeSyncClientFactory = GenericThriftClientFactory
        .create(DataNodeService.Iface.class)
        .withMaxConnectionsPerEndpoint(dataNodeCfg.getMaxConnectionsPerSyncDataNodeEndPoint())
        .withDefaultSocketTimeout(dataNodeCfg.getDataNodeRequestTimeoutMs())
        .setMaxNetworkFrameSize(getMaxNetworkFrameSize())
        .withMaxChannelPendingSizeMb(dataNodeCfg.getMaxChannelPendingSizeMb());
    dataNodeSyncClientFactory = GenericThriftClientFactory.create(DataNodeService.Iface.class)
        .withMaxConnectionsPerEndpoint(dataNodeCfg.getMaxConnectionsPerSyncDataNodeEndPoint())
        .withDefaultSocketTimeout(dataNodeCfg.getDataNodeRequestTimeoutMs())
        .setMaxNetworkFrameSize(getMaxNetworkFrameSize())
        .withMaxChannelPendingSizeMb(dataNodeCfg.getMaxChannelPendingSizeMb());
    dataNodeSyncClientFactory.setNeedCache();

    logger.warn("Preparing all internal structures. AppContext:{}", dataNodeContext);
    File root = new File(dataNodeCfg.getArchiveConfiguration().getPersistRootDir());

    shutdownStatusDecider = new ShutdownStatusDecider(root);
    DataNodeShutdownStatus shutdownStatus = shutdownStatusDecider.getStatus();
    logger.warn("Shutdown status of last run:{}", shutdownStatus);

    MultiChunkAddressHelper.initChunkLength(dataNodeCfg.getPageCountInRawChunk());
    ArchiveOptions.initContants(dataNodeCfg.getPageSize(), dataNodeCfg.getSegmentUnitSize(),
        dataNodeCfg.getPageMetadataSize(), dataNodeCfg.getArchiveReservedRatio(),
        dataNodeCfg.getFlexibleCountLimitInOneArchive(),
        dataNodeCfg.isPageMetadataNeedFlushToDisk());

    hostHeartbeat = new HostHeartbeatImpl(instanceStore, dataNodeCfg,
        heartBeatDataNodeSyncClientFactory);

    logger.info("Begin setting up the segment manager");
    SegmentUnitManagerImpl segmentUnitManagerImpl = new SegmentUnitManagerImpl();
    this.segMetadataManager = segmentUnitManagerImpl;

    TokenControllerCenter.getInstance().start();

    int numPossibleSegments = DEFAULT_POSSIBLE_SEGMENT_UNIT_COUNT;
    logger.info("Start to setup the storages.");
    this.priorityStorageHashedWheelTimer = createHashedWheelTimer();

    AsynchronousFileChannelStorageFactory factory = AsynchronousFileChannelStorageFactory
        .getInstance()
        .setMaxThreadpoolSizePerStorage(dataNodeCfg.getMaxThreadpoolSizePerStorage())
        .setMaxThreadpoolSizePerSsd(dataNodeCfg.getMaxThreadpoolSizePerSsd())
        .setMaxIoDepthHdd(dataNodeCfg.getMaxIoDepthPerHddStorage())
        .setMaxIoDepthSsd(dataNodeCfg.getMaxIoDepthPerSsdStorage());

    syncLogTaskExecutor = new SyncLogTaskExecutor(dataNodeCfg.getSyncLogTaskExecutorThreads(),
        "sync-log-task", getContext(), instanceStore, dataNodeCfg);
    syncLogTaskExecutor.start();

    if (!arbiterOnly) {
      logger.info(" Start to load the disk log");
      DiskErrorLogManager diskErrorLogManager = null;
      try {
        diskErrorLogManager = DiskErrorLogManagerImpl
            .load(dataNodeCfg, root, dataNodeCfg.getArchiveConfiguration().getDiskLogDir());
      } catch (Exception e) {
        logger.warn("error loading disk log. persistenceRoot: {}", root.getAbsolutePath());
      }

      List<UnsettledArchive> unsettledArchives = new ArrayList<>();
      File unsettledStorageDir = new File(root,
          dataNodeCfg.getArchiveConfiguration().getUnsettledArchiveDir());
      List<Storage> unsettledStorages = StorageBuilder.getFileStorage(unsettledStorageDir, factory);
      for (Storage storage : unsettledStorages) {
        storage.open();
        Storage unsettledStorage = new PriorityStorageImpl((AsyncStorage) storage,
            dataNodeCfg.getWriteTokenCount(), storageExceptionHandlerChain,
            priorityStorageHashedWheelTimer,
            dataNodeCfg.getStorageIoTimeout(), (int) ArchiveOptions.PAGE_PHYSICAL_SIZE);
        unsettledStorage.open();
        UnsettledArchiveBuild build = new UnsettledArchiveBuild(unsettledStorage, dataNodeCfg);
        try {
          UnsettledArchive archive = (UnsettledArchive) build.build();
          unsettledArchives.add(archive);
          logger.warn("Initializing the unsettled archive {} for unsettled", archive);
        } catch (ArchiveTypeMismatchException e) {
          logger.warn("unsettle disk type mismatch, relink disk.", e);
          unsettledStorage.close();
          String linkName = FilenameUtils.getName(unsettledStorage.identifier());
          relinkUnsettleDisk(linkName);
        }
      }

      File storageDir = new File(root, dataNodeCfg.getArchiveConfiguration().getDataArchiveDir());
      logger.info("data archive path: {}", storageDir);
      List<Storage> rawStorages = new LinkedList<>();
      for (Storage storage : StorageBuilder.getFileStorage(storageDir, factory)) {
        storage.open();
        Storage asyncStorage = new PriorityStorageImpl((AsyncStorage) storage,
            dataNodeCfg.getWriteTokenCount(),
            storageExceptionHandlerChain, priorityStorageHashedWheelTimer,
            dataNodeCfg.getStorageIoTimeout(), (int) ArchiveOptions.PAGE_PHYSICAL_SIZE);
        asyncStorage.open();
        rawStorages.add(asyncStorage);
      }

      unsettledArchiveManager = new UnsettledArchiveManagerImpl(dataNodeContext.getInstanceId(),
          dataNodeCfg,
          storageExceptionHandlerChain, unsettledArchives);
      

      saveLogProxy = new MutationLogEntrySaveProxy(dataNodeCfg.getShutdownSaveLogDir());

      segmentUnitCanDeletingCheck = new SegmentUnitCanDeletingCheckImpl(segMetadataManager,
          instanceStore,
          getContext().getInstanceId(), dataNodeSyncClientFactory,
          dataNodeCfg.getRateOfSegmentUnitDeleterDalayMs());

      logger.info("begin to create plal engine");
      plalEngine = new PlalEngine();
      logger.info("init membership pusher");
      MembershipPusher
          .init(new DataNodeServiceAsyncClientWrapper(dataNodeAsyncClientFactory), instanceStore,
              this.segMetadataManager, getContext().getInstanceId());

      long memorySizeBytes = dataNodeCfg.getMemorySizeForDataLogsMb() * 1024 * 1024;

      logger.warn("Fast buffer size: {}GB, {}MB, {}KB, {}Byte",
          ((double) memorySizeBytes) / 1024 / 1024 / 1024, memorySizeBytes * 1024 * 1024,
          memorySizeBytes / 1024, memorySizeBytes);

      Validate.isTrue(memorySizeBytes <= 500 * 1024 * 1024, "the fast buffer is too large");
      FastBufferManagerProxy.setSectorBits(dataNodeCfg.getFastBufferAllocateAligned());

      if (dataNodeCfg.getPrimaryFastBufferPercentage() + dataNodeCfg
          .getSecondaryFastBufferPercentage()
          + dataNodeCfg.getSyncLogFastBufferPercentage() > 100) {
        logger.error(
            "wrong config, PrimaryFastBufferPercentage:{}, "
                + "SecondaryFastBufferPercentage:{}, SyncLogFastBufferPercentage:{}",
            dataNodeCfg.getPrimaryFastBufferPercentage(),
            dataNodeCfg.getSecondaryFastBufferPercentage(),
            dataNodeCfg.getSyncLogFastBufferPercentage());
        System.exit(-1);
      }

      int ratio = dataNodeCfg.getPrimaryFastBufferPercentage() + dataNodeCfg
          .getSecondaryFastBufferPercentage();
      fastBufferManagerProxy = new FastBufferManagerProxy(
          new PrimaryTlsfFastBufferManager(dataNodeCfg.getFastBufferAlignmentBytes(),
              memorySizeBytes * ratio / 100), "normal");
     
     
     
      fastBufferManagerProxyForSecondary = fastBufferManagerProxy;
      fastBufferManagerProxyForSyncLog = new FastBufferManagerProxy(
          new SyncLogTlsfFastBufferManager(dataNodeCfg.getFastBufferAlignmentBytes(),
              memorySizeBytes * dataNodeCfg.getSyncLogFastBufferPercentage() / 100),
          "synclog");
      int totalRatio = ratio + dataNodeCfg.getSyncLogFastBufferPercentage();
      if (totalRatio != 100) {
        throw new RuntimeException(
            "fast buffer doesn't use up, ratio={}" + totalRatio + ", expected:100");
      }

      File fileForBufferCache = new File(dataNodeCfg.getArchiveConfiguration().getFileBufferPath());
      if (dataNodeCfg.isEnableFileBuffer() && fileForBufferCache.exists()) {
        logger.warn("initialize file buffer manager to allocate and release buffer, size:{} GB",
            dataNodeCfg.getArchiveConfiguration().getFileBufferSizeGb());

        long skipingSize = 1 * 1024L * 1024L;
       
       
        long reservedSize = 1 * 1024L * 1024L;
        TlsfFileBufferManager fileBufferManager = new TlsfFileBufferManager(
            fileForBufferCache.getAbsolutePath(),
            dataNodeCfg.getArchiveConfiguration().getFileBufferSizeGb() * 1024L * 1024L * 1024L
                - skipingSize - reservedSize, skipingSize);
        fileBufferManagerProxy = new FastBufferManagerProxy(fileBufferManager, "fileBuffer");

        TlsfFileBufferManager.Controller controller = new TlsfFileBufferManager.Controller(
            dataNodeCfg.getArchiveConfiguration().getFileBufferRejectionPercent(),
            dataNodeCfg.getArchiveConfiguration().getFileBufferObservationDuration());
        fileBufferManager.setRefuseController(controller);
      }

      MutationLogEntryFactory
          .init(fastBufferManagerProxy, fileBufferManagerProxy, fastBufferManagerProxyForSecondary,
              fastBufferManagerProxyForSyncLog, dataNodeCfg);

      logger.warn("Staring data log persisting system");
      logPersisterAndReaderFactory = new LogPersisterAndReaderFactory(this.segMetadataManager,
          dataNodeLogPersistingCfg, dataNodeLogPersistingRocksDbCfg);
      File file = new File(dataNodeCfg.getIndexerLogRoot());
      if (!file.exists() || !file.isDirectory()) {
        file.mkdir();
      }
     
      PersistDataToDiskEngineImpl
          .initInstance(dataNodeCfg, instanceStore, dataNodeAsyncClientFactory,
              this.segMetadataManager);

      logger.warn(
          "no matter data node is gracefully shutdown or not, "
              + "need to recover bitmap for segment unit");
      logReader = logPersisterAndReaderFactory.buildLogReader();

      reduceTimer = new HashedWheelTimer(new NamedThreadFactory("sync-log-collect-timer"), 5,
          TimeUnit.MILLISECONDS);
      AbstractPbSyncLogReduceBuilderFactory<PbBackwardSyncLogRequestUnit, PbBackwardSyncLogsRequest>
          reduceBuilderFactory1
          = new BackwardSyncLogRequestReduceBuilderFactory();

      backwardSyncLogRequestReduceCollector =
          new SyncLogReduceCollectorImpl(
              dataNodeCfg.getBackwardSyncLogPackageFrameSize(), getContext(), instanceStore,
              logReader, syncLogTaskExecutor, segMetadataManager, dataNodeCfg,
              SyncLogMessageType.SYNC_LOG_MESSAGE_TYPE_BACKWARD_REQUEST,
              reduceTimer,
              reduceBuilderFactory1);

      AbstractPbSyncLogReduceBuilderFactory
          <PbBackwardSyncLogResponseUnit, PbBackwardSyncLogsResponse>
          reduceBuilderFactory
          = new BackwardSyncLogResponseReduceBuilderFactory();
      backwardSyncLogResponseReduceCollector =
          new SyncLogReduceCollectorImpl(
              dataNodeCfg.getBackwardSyncLogPackageFrameSize(), getContext(), instanceStore,
              logReader, syncLogTaskExecutor, segMetadataManager, dataNodeCfg,
              SyncLogMessageType.SYNC_LOG_MESSAGE_TYPE_BACKWARD_RESPONSE,
              reduceTimer,
              reduceBuilderFactory);
      AbstractPbSyncLogReduceBuilderFactory<PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest>
          abstractPbSyncLogReduceBuilderFactory = new PbAsyncLogsBatchRequestReduceBuilderFactory();
      syncLogBatchRequestReduceCollector =
          new SyncLogReduceCollectorImpl(
              dataNodeCfg.getSyncLogPackageFrameSize(), getContext(), instanceStore,
              logReader, syncLogTaskExecutor, segMetadataManager, dataNodeCfg,
              SyncLogMessageType.SYNC_LOG_MESSAGE_TYPE_SYNC_LOG_BATCH_REQUEST,
              reduceTimer,
              abstractPbSyncLogReduceBuilderFactory);

      syncLogTaskExecutor
          .setBackwardSyncLogRequestReduceCollector(backwardSyncLogRequestReduceCollector);
      syncLogTaskExecutor
          .setBackwardSyncLogResponseReduceCollector(backwardSyncLogResponseReduceCollector);
      syncLogTaskExecutor.setSyncLogBatchRequestReduceCollector(syncLogBatchRequestReduceCollector);

      logger.warn("Initializing the all archives for data, archive count={}", rawStorages.size());
      RawArchiveManagerImpl rawArchiveManagerImpl = new RawArchiveManagerImpl(
          this.segMetadataManager,
          rawStorages, diskErrorLogManager, (SegmentUnitTaskCallback) this.segMetadataManager,
          dataNodeCfg, dataNodeContext, hostHeartbeat,
          syncLogTaskExecutor, segmentUnitCanDeletingCheck);
      rawArchiveManagerImpl.setArbiterManager(segmentUnitManagerImpl);
      this.rawArchiveManager = rawArchiveManagerImpl;

      StorageIoDispatcher storageIoDispatcher = new StorageIoDispatcher();

      logger.warn(
          "Begin to create memory pages and start page "
              + "manager, page count: {}, page size: {}, physical size: {}",
          dataNodeCfg.getNumberOfPages(), dataNodeCfg.getPageSize(),
          dataNodeCfg.getPhysicalPageSize());
      WrappedPageManagerFactory pageManagerFactory = new WrappedPageManagerFactory(dataNodeCfg,
          storageIoDispatcher);
      long startTime = System.currentTimeMillis();
      memoryPageManager = pageManagerFactory
          .build(dataNodeCfg.getPageSystemMemoryCacheSizeBytes(), "L1");
      logger.warn("Finish building the memory page pool, cost time: {} ms",
          System.currentTimeMillis() - startTime);

      this.ioThrottleManager = new IoThrottleManagerImpl(rawArchiveManager, dataNodeCfg);
      if (true) {
        for (RawArchive archive : this.rawArchiveManager.getRawArchives()) {
          archive.setLogStorageReader(logReader);
        }
        this.rawArchiveManager.recoverBitmap(dataNodeLogPersistingCfg);
      }

      logger.warn("Begin setting up the archive manager");
      if (this.rawArchiveManager.getRawArchives().isEmpty()) {
        String errMsg = "there is no available archive in system";
        logger.warn(errMsg);
      }
      if (!dataNodeCfg.getArchiveConfiguration().isOnVms() && dataNodeCfg
          .isStartSlowDiskAwaitChecker()) {
        logger.warn("start mobile slow disk checking.");
        startCheckingSlowDisksByAwait(rawArchiveManager);
      } else {
        logger.warn("don't start mobile slow disk checking.");
      }
     
     
     
      if (diskErrorLogManager != null) {
        persister = new StorageExceptionPersister(diskErrorLogManager, this.rawArchiveManager,
            dataNodeCfg);
        storageExceptionHandlerChain.addHandler(persister);
      }

      @SuppressWarnings("unused")
      UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          abort("unhandled exception encountered, shutting down the JVM", e, "thread",
              String.valueOf(t));
        }
      };

      logger.info("Staring data log persisting system");
      logger.info("Generating mutation log manager");
     
      mutationLogManager = new MutationLogManagerImpl(dataNodeCfg);
      logger.info("Starting data log catching up engines");
      this.rawArchiveManager.setMutationLogManager(mutationLogManager);

      numPossibleSegments = (int) (this.rawArchiveManager.getTotalPhysicalCapacity() / dataNodeCfg
          .getSegmentUnitSize());
      logger.debug("The number of possible segments is {}", numPossibleSegments);
      if (numPossibleSegments <= 0 || numPossibleSegments > 20000) {
        logger.warn("the estimated number of segments is {} doesn't seem right. "
                + "the data node capacity is {} the segment unit size is {} Making it to 6000",
            numPossibleSegments, this.rawArchiveManager.getTotalPhysicalCapacity(),
            dataNodeCfg.getSegmentUnitSize());
       
        numPossibleSegments = DEFAULT_POSSIBLE_SEGMENT_UNIT_COUNT;
      }

      logger.info("start checking the storage plugin plugout");
      pluginPlugoutManagerImpl = new PluginPlugoutManagerImpl(dataNodeCfg);
      pluginPlugoutManagerImpl.setRawArchiveManager(this.rawArchiveManager);
      pluginPlugoutManagerImpl.setUnsettledArchiveManager(this.unsettledArchiveManager);
      pluginPlugoutManagerImpl.init();
      startCheckStorages(pluginPlugoutManagerImpl, dataNodeCfg.getStorageCheckRateMs());

      logPersister = logPersisterAndReaderFactory.buildLogPersister();
      this.rawArchiveManager.setLogPersister(logPersister);
      SegmentUnitProcessorFactory segmentUnitLogProcessorFactory =
          new SegmentUnitDataLogProcessorFactory(
              this.rawArchiveManager, logPersister, memoryPageManager, dataNodeSyncClientFactory,
              instanceStore,
              dataNodeCfg, dataNodeContext.getInstanceId(), plalEngine, segMetadataManager,
              new DataNodeServiceAsyncClientWrapper(dataNodeAsyncClientFactory)
          );

      SegmentUnitTaskContextFactory logContextFactory = new CatchupLogContextFactory(
          this.segMetadataManager);

      int catchUpLogEnginesCount = dataNodeCfg.getCatchUpLogEnginesCount();
      SegmentUnitTaskExecutorImpl[] catchUpLogEngines =
          new SegmentUnitTaskExecutorImpl[catchUpLogEnginesCount];

      int minThreadPoolSizeForSegmentUnitTaskExecutors = dataNodeCfg
          .getMinThreadPoolSizeForSegmentUnitTaskExecutors();
      for (int i = 0; i < catchUpLogEnginesCount; i++) {
        SegmentUnitTaskExecutorImpl singleCatchupLogEngine = new SegmentUnitTaskExecutorImpl(
            segmentUnitLogProcessorFactory, logContextFactory,
            Math.max(minThreadPoolSizeForSegmentUnitTaskExecutors,
                dataNodeCfg.getCorePoolSizeForCatchupLogEnginePcl() / catchUpLogEnginesCount),
            Math.max(minThreadPoolSizeForSegmentUnitTaskExecutors,
                dataNodeCfg.getMaxPoolSizeForCatchupLogEnginePcl() / catchUpLogEnginesCount),
            numPossibleSegments * 3, "LogDriver-" + i);
        singleCatchupLogEngine.addThreadPoolExecutor(SegmentUnitTaskType.PPL,
            Math.max(minThreadPoolSizeForSegmentUnitTaskExecutors,
                dataNodeCfg.getCorePoolSizeForCatchupLogEnginePpl() / catchUpLogEnginesCount),
            Math.max(minThreadPoolSizeForSegmentUnitTaskExecutors,
                dataNodeCfg.getMaxPoolSizeForCatchupLogEnginePpl() / catchUpLogEnginesCount));
        catchUpLogEngines[i] = singleCatchupLogEngine;
      }
      catchupLogEngine = new SegmentUnitTaskExecutorWrapper(catchUpLogEngines);
      catchupLogEngine.pause();

      NotInsertedLogs.startExpiredTaskDriving();

      logger.info("Starting io delay statistic reporter");
      alarmReporter = new AlarmReporterImpl();
      alarmReporter.start();
    } else {
     
      PersistDataToDiskEngineImpl
          .initInstance(dataNodeCfg, instanceStore, dataNodeAsyncClientFactory,
              this.segMetadataManager);

      RawArchiveManagerImpl rawArchiveManagerImpl = new RawArchiveManagerImpl(
          this.segMetadataManager,
          new ArrayList<>(), null, (SegmentUnitTaskCallback) this.segMetadataManager, dataNodeCfg,
          dataNodeContext, hostHeartbeat,
          syncLogTaskExecutor, segmentUnitCanDeletingCheck);
      rawArchiveManagerImpl.setArbiterManager(segmentUnitManagerImpl);
      this.rawArchiveManager = rawArchiveManagerImpl;
      this.mutationLogManager = new MutationLogManagerImpl(dataNodeCfg);
    }

    inforCenterClientFactory = new InformationCenterClientFactory(1);
    inforCenterClientFactory.getGenericClientFactory()
        .setMaxNetworkFrameSize(getMaxNetworkFrameSize());

    logger.info("Starting information reporter");
    inforCenterClientFactory.setInstanceStore(instanceStore);
    this.backupDbReporter = new BackupDbReporterImpl(dataNodeCfg.getBackupDbInfoDir());
    informationReporter = new InformationReporter(this.segMetadataManager, this.rawArchiveManager,
        inforCenterClientFactory, dataNodeContext, dataNodeCfg, this,
        segmentUnitCanDeletingCheck, this.backupDbReporter,
        instanceStore, storageExceptionHandlerChain, dataNodeSyncClientFactory,
        pluginPlugoutManagerImpl,
        ioThrottleManager, priorityStorageHashedWheelTimer);
    informationReporter.setAlarmReporter(alarmReporter);
    informationReporter.setUnsettledArchiveManager(unsettledArchiveManager);
    informationReporter.setArbiterDatanode(arbiterOnly);
    informationReporter.start();

    if (!arbiterOnly) {
     
     
     
     
      for (RawArchive archive : this.rawArchiveManager.getRawArchives()) {
        if (archive.getArchiveStatus() == ArchiveStatus.OFFLINING) {
          try {
            informationReporter.becomeOfflining(archive, ArchiveStatus.OFFLINING);
          } catch (Exception e) {
            logger.warn("can not report the offlining archive: {}", archive, e);
          }
        } else if (archive.getArchiveMetadata().getStatus() == ArchiveStatus.OFFLINED) {
          try {
            informationReporter.becomeOfflined(archive, ArchiveStatus.OFFLINED);
          } catch (Exception e) {
            logger.warn("can not report the offlining archive: {}", archive, e);
          }
        }
      }

      this.rawArchiveManager.addArchiveStatusChangeFinishListener(informationReporter);
    }

    logger.warn(
        "initializing internal structure was done. "
            + "Now begin to start the DataNode service. instanceId:"
            + dataNodeContext);
   
    setMaxNumThreads(dataNodeCfg.getMaxNumWorkerThreads());
    setMinNumThreads(dataNodeCfg.getMinNumWorkerThreads());
    initDataNodeService();
    plalEngine.restartPlalWorker();

    int stateProcessingEnginesCount = dataNodeCfg.getStateProcessingEnginesCount();
    SegmentUnitTaskExecutorImpl[] stateProcessingEngines =
        new SegmentUnitTaskExecutorImpl[stateProcessingEnginesCount];
    int minThreadPoolSizeForSegmentUnitTaskExecutors = dataNodeCfg
        .getMinThreadPoolSizeForSegmentUnitTaskExecutors();

    logger.info("Starting segment unit state processing engine");
    for (int i = 0; i < stateProcessingEnginesCount; i++) {
      StateProcessorFactory segmentUnitStateProcessorFactory = new StateProcessorFactory(
          this.segMetadataManager,
          mutationLogManager, logPersister, this.rawArchiveManager, dataNodeSyncClientFactory,
          dataNodeAsyncClientFactory, dataNodeService.getIoClientFactory(),
          inforCenterClientFactory, dataNodeContext, dataNodeCfg, instanceStore, catchupLogEngine,
          memoryPageManager, plalEngine, ioThrottleManager);

      SegmentUnitTaskContextFactory stateContextFactory = new StateProcessingContextFactory(
          this.segMetadataManager);
      SegmentUnitTaskExecutorImpl singleStateProcessingEngine = new SegmentUnitTaskExecutorImpl(
          segmentUnitStateProcessorFactory, stateContextFactory,
          Math.max(minThreadPoolSizeForSegmentUnitTaskExecutors,
              dataNodeCfg.getCorePoolSizeForStateProcessingEngineOther()
                  / stateProcessingEnginesCount),
          Math.max(minThreadPoolSizeForSegmentUnitTaskExecutors,
              dataNodeCfg.getMaxPoolSizeForStateProcessingEngineOther()
                  / stateProcessingEnginesCount),
          numPossibleSegments * 3, "StateProcessing-" + i);

      singleStateProcessingEngine.addThreadPoolExecutor(SegmentUnitTaskType.ExpirationChecker,
          dataNodeCfg.getCorePoolSizeForStateProcessingEngineExpirationChecker(),
          dataNodeCfg.getMaxPoolSizeForStateProcessingEngineExpirationChecker());
      stateProcessingEngines[i] = singleStateProcessingEngine;
    }

    stateProcessingEngine = new SegmentUnitTaskExecutorWrapper(stateProcessingEngines);
    stateProcessingEngine.pause();

    this.rawArchiveManager.setStateProcessingEngine(stateProcessingEngine);

    if (!arbiterOnly) {
     
     
     
     
     
      this.rawArchiveManager.setCatchupLogEngine(catchupLogEngine);
     
     
      try {
        tempFileLogPersister = new TempFileLogPersister(
            dataNodeLogPersistingCfg.getRootDirName() + "_temp");
        tempFileLogPersister.insertMissingLogs(mutationLogManager, true);
        dataNodeService.setTempLogPersister(tempFileLogPersister);
      } catch (Exception e) {
        logger.warn("can't initialize tempFileLogPersister", e);
      }
    }

    super.start();

    if (!arbiterOnly) {
      logger.info("Starting log catchup engine");
      catchupLogEngine.start();
    }
    logger.info("start the state processing engine");
    stateProcessingEngine.start();

    if (!arbiterOnly) {
      logger.info("start check log space");
      startCheckLogSpace(dataNodeCfg.getMinReservedDiskSizeMb(),
          dataNodeCfg.getLogDirs().split(","));
    }

    logger.info("start to report the create segment unit");
    startReportingJustCreatedSegUnits();

    logger.info("start to persisted segment unit metadata");
    startPersistSegUnitMetadata(this.rawArchiveManager,
        dataNodeCfg.getPersistSegUnitMetadataRateMs());

    shutdownStatusDecider.persistStartTime();

    shutdownStatusDecider.createEmptyShutdownTimeFile();

    hostHeartbeat.start();

    udpServer.startEchoServer();

    UdpDetectorImpl.INSTANCE.start(dataNodeCfg.getNetworkConnectionDetectServerListeningPort(),
        new DefaultDetectionTimeoutPolicyFactory(
            dataNodeCfg.getNetworkConnectionDetectRetryMaxtimes(),
            dataNodeCfg.getPingHostTimeoutMs()));
    NetworkIoHealthChecker.INSTANCE.start(UdpDetectorImpl.INSTANCE);

    logger.warn("DataNode AppEngine UDP server startup successfully, listening port is {}!!",
        dataNodeCfg.getNetworkConnectionDetectServerListeningPort());

    startNetworkHealthChecker(this.segMetadataManager, getContext(),
        dataNodeCfg.getNetworkHealthCheckerRateMs(),
        udpServer, dataNodeCfg.getNetworkReviveThresholdMs());

    if (dataNodeCfg.isEnableLogCounter()) {
      startLogCounter((RawArchiveManagerImpl) rawArchiveManager, 5000);
    }
  }

  public RawArchiveManager getRawArchiveManager() {
    return rawArchiveManager;
  }

  private void validateRequiredFields() {
    AppContext context = getContext();
    Validate.notNull(context);
    Validate.isTrue(context instanceof DataNodeContext);
    Validate.notNull(instanceStore);
    Validate.notNull(dataNodeCfg);
   
   
   
    if (!arbiterOnly) {
      Validate.notNull(dataNodeLogPersistingCfg);
      Validate.notNull(dataNodeLogPersistingRocksDbCfg);
    }
  }

  private void startCheckingSlowDisksByAwait(RawArchiveManager archiveManager)
      throws UnableToStartException {
    checkSlowDiskAwaitExecutor = new PeriodicWorkExecutorImpl(
        new ExecutionOptionsReader(1, 1, null, dataNodeCfg.getSlowDiskAwaitCheckInterval()),
        () -> () -> {
          for (RawArchive rawArchive : archiveManager.getRawArchives()) {
            Storage storage = rawArchive.getStorage();
            if (storage instanceof PriorityStorageImpl) {
              ((PriorityStorageImpl) storage)
                  .checkSlowDiskByAwait(dataNodeCfg.getIostatFileName(),
                      dataNodeCfg.getSlowDiskAwaitThreshold(),
                      rawArchive.getArchiveMetadata().getDeviceName());
            }
          }
        }, "mobile-slow-disk-checker");
    checkSlowDiskAwaitExecutor.start();
  }

  private void startPendingIoWork(RawArchiveManager archiveManager) throws UnableToStartException {
    checkPendingIoWorkExecutor = new PeriodicWorkExecutorImpl(
        new ExecutionOptionsReader(1, 1, null,
            dataNodeCfg.getStorageIoTimeout()),
        () -> () -> {
          for (RawArchive rawArchive : archiveManager.getRawArchives()) {
            Storage storage = rawArchive.getStorage();
            if (storage instanceof PriorityStorageImpl) {
              ((PriorityStorageImpl) storage).checkPendingTask();
            }
          }
        }, "mobile-slow-disk-checker");
    checkPendingIoWorkExecutor.start();
  }

  public void startCheckStorages(final PluginPlugoutManager pluginPlugoutManager, int refreshRate)
      throws Exception {
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, refreshRate, null);
    checkDiskExecutor = new PeriodicWorkExecutorImpl(optionReader,
        new CheckStoragesWorkerFactory(pluginPlugoutManager, dataNodeCfg,
            storageExceptionHandlerChain,
            priorityStorageHashedWheelTimer));
    checkDiskExecutor.start();
  }

  public void startCheckLogSpace(final long minReservedMb, String[] logDirs) throws Exception {
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, 5000, null);
    checkLogSpaceExecutor = new PeriodicWorkExecutorImpl(optionReader, new WorkerFactory() {
      @Override
      public Worker createWorker() {
        return new CheckLogSpaceWorker(minReservedMb, logDirs);
      }
    }, "logspace-checker");
    checkLogSpaceExecutor.start();
  }

  public void startPersistSegUnitMetadata(final RawArchiveManager archiveManager, int refreshRate)
      throws Exception {
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, refreshRate, null);
    persistSegUnitMetadataExecutor = new PeriodicWorkExecutorImpl(optionReader,
        new WorkerFactory() {
          @Override
          public Worker createWorker() {
            return new PersistSegUnitMetadataWorker(archiveManager);
          }
        }, "segUnitMetadata-bitmap-persister");
    persistSegUnitMetadataExecutor.start();
  }

  public void startReportingJustCreatedSegUnits() {
    justCreatedSegUnitsReporter = new JustCreatedSegUnitsReporter(inforCenterClientFactory);
    justCreatedSegUnitsReporter.setName("Just-Created-SegUnit-Reporter");
    try {
      justCreatedSegUnitsReporter.start();
    } catch (IllegalStateException e) {
      logger.warn("Caught an exception when start reporter to report just created segunits", e);
    }
  }

  private void startNetworkHealthChecker(final SegmentUnitManager segmentUnitManager,
      final AppContext appContext,
      int executeInterval, UdpServer udpServer, long reviveProcessThresholdMs)
      throws UnableToStartException {
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, executeInterval, null);
    checkNetworkHealthExecutor = new PeriodicWorkExecutorImpl(optionReader,
        () -> new NetworkHealthChecker(segmentUnitManager, appContext, dataNodeService,
            reviveProcessThresholdMs, instanceStore), "network-health-checker");
    checkNetworkHealthExecutor.start();
  }

  private void startLogCounter(RawArchiveManagerImpl archiveManager, int executeInterval)
      throws UnableToStartException {
    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, executeInterval, null);
    logCounter = new PeriodicWorkExecutorImpl(optionReader, () -> archiveManager::countLogs,
        "archive-counter");

    logCounter.start();
  }

  private void initDataNodeService() throws Exception {
    
    dataNodeService.setArchiveManager(rawArchiveManager);
    dataNodeService.setContext(getContext());
    dataNodeService.setSegmentUnitManager(segMetadataManager);
    dataNodeService.setCfg(dataNodeCfg);
    dataNodeService.setInstanceStore(instanceStore);
    dataNodeService.setStateProcessingEngine(stateProcessingEngine);
    dataNodeService.setDataNodeSyncClientFactory(dataNodeSyncClientFactory);
    dataNodeService.setDataNodeAsyncClientFactory(dataNodeAsyncClientFactory);
    dataNodeService.setDataNodeAppEngine(this);
    dataNodeService.setHostHeartbeat(hostHeartbeat);
    dataNodeService.setBackupDbReporter(backupDbReporter);
    dataNodeService.setSegmentUnitCanDeletingCheck(segmentUnitCanDeletingCheck);
    dataNodeService.setByteBufAllocator(allocator);
    dataNodeService.setArbiterDatanode(arbiterOnly);
    dataNodeService.setMutationLogManager(mutationLogManager);

    dataNodeService.setBackwardSyncLogRequestReduceCollector(backwardSyncLogRequestReduceCollector);
    dataNodeService
        .setBackwardSyncLogResponseReduceCollector(backwardSyncLogResponseReduceCollector);
    dataNodeService.setSyncLogBatchRequestReduceCollector(syncLogBatchRequestReduceCollector);
    dataNodeService.setSyncLogTaskExecutor(syncLogTaskExecutor);
    

    if (!arbiterOnly) {
      dataNodeService.setPageManager(memoryPageManager);
      dataNodeService.setLogStorageReader(logPersisterAndReaderFactory.buildLogReader());
      dataNodeService.setLogPersister(logPersister);
      dataNodeService.setCatchupLogEngine(catchupLogEngine);
      dataNodeService.setStorageExceptionHandler(persister);
      dataNodeService.setPlalEngine(plalEngine);
      dataNodeService.setCopyPageTaskEngine(copyPageTaskEngine);
      dataNodeService.setSaveLogProxy(saveLogProxy);
      dataNodeService.setIoThrottleManager(ioThrottleManager);
      dataNodeService.setAlarmReporter(alarmReporter);
      dataNodeService.setPluginPlugoutManager(pluginPlugoutManagerImpl);
      dataNodeService.setUnsettledArchiveManager(unsettledArchiveManager);
      dataNodeService.setHashedWheelTimer(priorityStorageHashedWheelTimer);
      plalEngine.setService(dataNodeService);
    }

    dataNodeService.init();
  }

  @Override
  public void stop() {
    logger.info("now shutdown datanode");
    shutDown();

    udpServer.stopEchoServer();
    logger.warn("DataNode AppEngine UDP server stopped successfully!!");

    logger.info("after shutDown action, next step is stop");
    super.stop();
  }

  private void shutDown() {
    int step = 0;
    step++;

    logger.warn(">>>><step{}> shutdown jmx-agent", ++step);
    try {
      if (jmxAgent != null) {
       
        int timeoutSeconds = 10;
        while (timeoutSeconds > 0) {
          try {
            jmxAgent.stop();
            logger.warn("successfully stopped jmx agent");
            break;
          } catch (IllegalStateException e) {
            logger.warn("can't stop jmx agent, sleep for a while and try again", e);
            timeoutSeconds--;
            Thread.sleep(1000);
          }
        }
      }
    } catch (Throwable e) {
      logger.warn("Caught an exception when shutdown jmx-agent. Do nothing", e);
    }

    logger.warn(">>>><step{}> shutdown file handle manager", ++step);
    try {
      FileHandleManagerImpl.getInstance().stop();
    } catch (Exception e) {
      logger.warn("caught an exception", e);
    }

    step++;
    logger.warn(">>>><step{}> shutdown host heartbeat", step);
    hostHeartbeat.stop();

    step++;
    logger.warn(">>>><step{}> shutdown stateProcessingEngine", step);
   
    if (stateProcessingEngine != null) {
      stateProcessingEngine.shutdown();
      try {
        MembershipPusher.getInstance(getContext().getInstanceId()).stop();
      } catch (Exception e) {
        logger.error("caught an exception", e);
      }
    }

    step++;
    logger.warn(">>>><step{}> shutdown checkDiskExecutor", step);
    try {
      if (checkDiskExecutor != null) {
        checkDiskExecutor.stop();
        checkDiskExecutor.awaitTermination(65, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when stopping checking disk. Do nothing", e);
    }

    step++;

    logger.warn(">>>><step{}> shutdown checkSlowDiskExecutor", step);
    try {
      if (checkSlowDiskAwaitExecutor != null) {
        checkSlowDiskAwaitExecutor.stop();
        checkSlowDiskAwaitExecutor.awaitTermination(65, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when stopping checking mobile slow disk. Do nothing", e);
    }

    step++;

    logger.warn(">>>><step{}> shutdown checkPendingIOWorkExecutor", step);
    try {
      if (checkPendingIoWorkExecutor != null) {
        checkPendingIoWorkExecutor.stop();
        checkPendingIoWorkExecutor.awaitTermination(65, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when stopping checking mobile slow disk. Do nothing", e);
    }

    step++;

    logger.warn(">>>><step{}> shutdown checkLogSpaceExecutor", step);
    try {
      if (checkLogSpaceExecutor != null) {
        checkLogSpaceExecutor.stop();
        checkLogSpaceExecutor.awaitTermination(65, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when stopping checking LogSpace. Do nothing", e);
    }

    step++;
    logger.warn(">>>><step{}> shutdown persistSegUnitMetadataExecutor", step);
    try {
      if (persistSegUnitMetadataExecutor != null) {
        persistSegUnitMetadataExecutor.stop();
        persistSegUnitMetadataExecutor.awaitTermination(65, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when stopping persisting bit map. Do nothing", e);
    }

    step++;
    logger.warn(">>>><step{}> shutdown checkNetworkHealthExecutor", step);
    try {
      if (checkNetworkHealthExecutor != null) {
        checkNetworkHealthExecutor.stop();
        checkNetworkHealthExecutor.awaitTermination(65, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when stopping check Network Health Executor. Do nothing", e);
    }

    try {
      if (logCounter != null) {
        step++;
        logger.warn(">>>><step{}> shutdown log counter", step);
        logCounter.stop();
        logCounter.awaitTermination(65, TimeUnit.SECONDS);
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when stopping check Network Health Executor. Do nothing", e);
    }

    step++;
    logger.warn(">>>><step{}> shutdown reportJustCreatedSegUnitsExecutor", step);
    if (justCreatedSegUnitsReporter != null) {
      justCreatedSegUnitsReporter.interrupt();
    }

    step++;
    logger.warn(">>>><step{}> shutdown informationReporter", step);
   
    if (informationReporter != null) {
      informationReporter.stop();
    }

    step++;
    logger.warn(">>>><step{}> shutdown segmentUnitCanDeletingCheck", step);
   
    if (segmentUnitCanDeletingCheck != null) {
      segmentUnitCanDeletingCheck.stop();
    }

    step++;
    logger.warn(">>>><step{}> shutdown io average delay statistic reporter", step);
    if (alarmReporter != null) {
      alarmReporter.stop();
    }

    step++;
    logger.warn(">>>><step{}> shutdown mutationLogManager", step);
    try {
      if (!arbiterOnly) {
        processAllLogsExit(mutationLogManager, segMetadataManager, plalEngine,
            dataNodeCfg.getMaxWaitForDnShutdownTimeSec());
      }
    } catch (Exception e) {
      logger.error("caught an exception when process all logs exit", e);
    }

    step++;
    logger.warn(">>>><step{}> shutdown catchupLogEngine", step);
    if (catchupLogEngine != null) {
      catchupLogEngine.shutdown();
    }

    step++;
    logger.warn(">>>><step{}> shutdown not inserted logs expiration driver", step);
    NotInsertedLogs.stopExpiredTaskDriving();

    logger.info("Have shut down plal engine");
    step++;
    logger.warn(">>>><step{}> shutdown plal driver", step);
    if (plalEngine != null) {
      try {
        plalEngine.stop();
      } catch (Exception e) {
        logger.warn("caught an exception");
      }
    }

    step++;
    logger.warn(">>>><step{}> shutdown logPersister", step);
    try {
      if (logPersister != null) {
        logPersister.close();
      }
    } catch (IOException e) {
      logger.warn("can't close log persister", e);
    }
    step++;
    logger.warn(">>>><step{}> shutdown memoryPageManager", step);
    boolean memCleanShutdown = false;
    if (memoryPageManager != null) {
      try {
        logger.warn("closing page manager for memory");
        memoryPageManager.close();

        memCleanShutdown = (memoryPageManager.getDirtyPageCount() == 0);
        if (!memCleanShutdown) {
          logger.warn(
              " Page manager for memory closed but there were dirty pages: {}, not considering "
                  + "shutdown clean", memoryPageManager.getDirtyPageCount());
        }
      } catch (Exception e) {
        logger.warn("page manager for memory interrupted on shutdown", e);
      }
    }

    step++;
    logger.warn(">>>><step{}> save logs if neccessary", step);
    try {
      if (!arbiterOnly) {
        memCleanShutdown = saveLogsIfNeccessary(mutationLogManager, segMetadataManager,
            saveLogProxy);
      }
    } catch (Exception e) {
      logger.error("caught an exception when save logs", e);
    }

    step++;
    logger.warn(
        ">>>><step{}> shutdown cache synchronous counter,"
            + " the process must be previous to stop task engine",
        step);

    step++;
    logger.warn(">>>><step{}> shutdown archive bitmap persister", step);
    for (RawArchive archive : rawArchiveManager.getRawArchives()) {
      archive.persistBitMapAndSegmentUnitIfNecessary();
    }

    step++;
    logger.warn(">>>><step{}> shutdown persist data thread", step);
    try {
      PersistDataToDiskEngineImpl.getInstance().stop();
    } catch (Exception e) {
      logger.error("Cannot stop persist data to disk engine", e);
    }

    step++;
    logger.warn(">>>><step{}> shutdown rawArchiveManager", step);
    if (rawArchiveManager != null) {
      rawArchiveManager.close();
    }

    step++;
    logger.warn(">>>><step{}> shutdown shutdownStatusDecider", step);
    if (memCleanShutdown) {
      try {
        if (shutdownStatusDecider != null) {
          shutdownStatusDecider.persistShutdownTime();
          logger.warn("it is a clean shutdown, so persit shotdown time");
        }
      } catch (IOException e) {
        logger.warn("Couldn't write shutdown time", e);
      }
    }
    step++;
    logger.warn(">>>><step{}> shutdown ssdStorage", step);
    if (ssdStorage != null) {
      try {
        ssdStorage.close();
      } catch (StorageException e) {
        logger.warn("can not close ssd storage on shutdown", e);
      }
    }

    step++;
    logger.warn(">>>><step{}> shutdown dataNodeSyncClientFactory", step);
    try {
      if (dataNodeSyncClientFactory != null) {
        dataNodeSyncClientFactory.close();
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when closing synced data node clients. Do nothing", e);
    }

    step++;
    logger.warn(">>>><step{}> shutdown tempLogStore", step);
    if (tempFileLogPersister != null) {
      try {
        tempFileLogPersister.close();
      } catch (IOException e) {
       
       
        logger.warn("caught an exception when closing tempFileLogPersister. Do nothing", e);
      }
    }

    step++;
    logger.warn(">>>><step{}> shutdown dataNodeAsyncClientFactory", step);
    try {
      if (dataNodeAsyncClientFactory != null) {
        dataNodeAsyncClientFactory.close();
      }
    } catch (Throwable e) {
     
     
      logger.warn("caught an exception when closing asynced data node clients. Do nothing", e);
    }
    step++;
    logger.warn(">>>><step{}> shutdown heartBeatDataNodeAsyncClientFactory", step);
    try {
      if (heartBeatDataNodeSyncClientFactory != null) {
        heartBeatDataNodeSyncClientFactory.close();
      }
    } catch (Throwable e) {
     
     
      logger
          .warn("caught an exception when closing heart beat asynced data node clients. Do nothing",
              e);
    }
    step++;
    logger.warn(">>>><step{}> shutdown instanceStore", step);
    if (this.instanceStore != null) {
      instanceStore.close();
    }
    step++;
    logger.warn(">>>><step{}> shutdown inforCenterClientFactory", step);
    if (this.inforCenterClientFactory != null) {
      this.inforCenterClientFactory.close();
    }

    step++;
    logger.warn(">>>><step{}> shutdown dihClientFactory", step);
    if (this.dihClientFactory != null) {
      this.dihClientFactory.close();
    }

    step++;
    logger.warn(">>>><step{}> shutdown memory fast manager", step);
    if (fastBufferManagerProxy != null) {
      fastBufferManagerProxy.close();
    }

    step++;
    logger.warn(">>>><step{}> shutdown file fast manager", step);
    if (fileBufferManagerProxy != null) {
      fileBufferManagerProxy.close();
    }

    step++;
    logger.warn(">>>><step{}> shutdown storage io pool", step);
    AsynchronousFileChannelStorageFactory.getInstance().close();

    step++;
    logger.warn(">>>><step{}> shutdown io copy page engine", step);
    if (copyPageTaskEngine != null) {
      copyPageTaskEngine.stop();
    }

    step++;
    logger.warn(">>>><step{}> exit sync log task executor", step);
    syncLogTaskExecutor.stop();

    if (!arbiterOnly) {
      step++;
      logger.warn(">>>><step{}> exit sync log reduce timer", step);
      reduceTimer.stop();
    }

    step++;
    logger.warn(">>>><step{}> exit logger tracer", step);
   
   
   
   
   

    step++;
    logger.warn(">>>><step{}> exit ioservice engine", step);
    dataNodeService.stop();

    step++;
    logger.warn(
        ">>>><step{}> shutdown io controller center,"
            + " the process must be previous to stop task engine",
        step);
    TokenControllerCenter.getInstance().stop();

    step++;
    logger.warn(">>>><step{}> exit priority storage wheel timer", step);
    if (priorityStorageHashedWheelTimer != null) {
      priorityStorageHashedWheelTimer.stop();
    }

    logger.warn(">>>><step over> shutdown datanode completely");
    logger.warn("DN was shut down for memory: " + memCleanShutdown);
  }

  protected void abort(String msg, Throwable e, String... keyValPairs) {
    logger.error("System exit, aborting the application:" + msg + keyValPairs, e);
    System.exit(-1);
  }

  private void relinkUnsettleDisk(String linkName)
      throws InterruptedException, InternalErrorException, IOException {
    String cmd = String
        .format("python %s %s",
            dataNodeCfg.getArchiveConfiguration().getRelinkUnsettleDiskScriptPath(),
            linkName);
    logger.info("relink unsettle disk for linkName:[{}] cmd:[{}]", linkName, cmd);

    try {
      Process pid = Runtime.getRuntime().exec(cmd);
      int returnValue = pid.waitFor();

      if (returnValue != 0) {
        logger.error("relink unsettle disk for linkName:[{}] failed, ret code:[{}]", linkName,
            returnValue);
        throw new InternalErrorException(
            String.format("relink unsettle disk failed, linkName:[%s]", linkName));
      }
    } catch (IOException | InterruptedException | InternalErrorException e) {
      logger.error("relink unsettle disk for linkName:[{}] failed, catch exception.", e);
      throw e;
    }
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public DataNodeConfiguration getDataNodeCfg() {
    return dataNodeCfg;
  }

  public void setDataNodeCfg(DataNodeConfiguration dataNodeCfg) {
    this.dataNodeCfg = dataNodeCfg;
  }

  public LogPersistingConfiguration getDataNodeLogPersistingCfg() {
    return dataNodeLogPersistingCfg;
  }

  public void setDataNodeLogPersistingCfg(LogPersistingConfiguration dataNodeLogPersistingCfg) {
    this.dataNodeLogPersistingCfg = dataNodeLogPersistingCfg;
  }

  public LogPersistRocksDbConfiguration getDataNodeLogPersistingRocksDbCfg() {
    return dataNodeLogPersistingRocksDbCfg;
  }

  public void setDataNodeLogPersistingRocksDbCfg(
      LogPersistRocksDbConfiguration dataNodeLogPersistingRocksDbCfg) {
    this.dataNodeLogPersistingRocksDbCfg = dataNodeLogPersistingRocksDbCfg;
  }

  public DataNodeServiceImpl getDataNodeService() {
    return dataNodeService;
  }

  public void setDihClientFactory(DihClientFactory dihClientFactory) {
    this.dihClientFactory = dihClientFactory;
  }

  public JmxAgent getJmxAgent() {
    return jmxAgent;
  }

  public void setJmxAgent(JmxAgent jmxAgent) {
    this.jmxAgent = jmxAgent;
  }

  public void setAllocator(ByteBufAllocator allocator) {
    this.allocator = allocator;
  }

  public PageManager<Page> getMemoryPageManager() {
    return memoryPageManager;
  }

  public void setArbiterOnly(boolean arbiterOnly) {
    this.arbiterOnly = arbiterOnly;
  }

  private HashedWheelTimer createHashedWheelTimer() {
    return new HashedWheelTimer(new NamedThreadFactory("priority-storage-wheel-timer"), 100,
        TimeUnit.MILLISECONDS, 512);
  }

  public LogStorageReader getLogReader() {
    return logReader;
  }

  public SyncLogTaskExecutor getSyncLogTaskExecutor() {
    return syncLogTaskExecutor;
  }

}
