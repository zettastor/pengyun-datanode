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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.util.HashedWheelTimer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveOptions;
import py.archive.ArchiveStatus;
import py.archive.ArchiveStatusChangeFinishListener;
import py.archive.ArchiveType;
import py.archive.PluginPlugoutManager;
import py.archive.RawArchiveMetadata;
import py.archive.UnsettledArchiveMetadata;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveManager;
import py.datanode.archive.UnsettledArchive;
import py.datanode.archive.UnsettledArchiveManager;
import py.datanode.checksecondaryinactive.CheckSecondaryInactive;
import py.datanode.checksecondaryinactive.CheckSecondaryInactive.CheckSecondaryInactiveThresholdMode;
import py.datanode.checksecondaryinactive.CheckSecondaryInactiveByAbsoluteTime;
import py.datanode.checksecondaryinactive.CheckSecondaryInactiveByRelativeTime;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.NoNeedToBeDeletingException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitCanDeletingCheck;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.membership.statemachine.GlobalDeletingSegmentUnitCollection;
import py.datanode.segment.membership.statemachine.checksecondaryinactive.NewSegmentUnitExpirationThreshold;
import py.datanode.segment.membership.statemachine.processors.LogFlushedChecker;
import py.datanode.service.io.throttle.CopyPageSampleInfo;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.datanode.statistic.AlarmReportData;
import py.datanode.statistic.AlarmReporter;
import py.engine.DelayedTask;
import py.engine.DelayedTaskEngine;
import py.engine.Result;
import py.engine.ResultImpl;
import py.exception.AllSegmentUnitDeadInArchviveException;
import py.exception.ArchiveIsNotCleanedException;
import py.exception.EndPointNotFoundException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.SegmentUnitBecomeBrokenException;
import py.exception.SegmentUnitRecoverFromDeletingFailException;
import py.exception.StorageException;
import py.exception.TooManyEndPointFoundException;
import py.icshare.BackupDbReporter;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceDomain;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.io.qos.MigrationStrategy;
import py.membership.SegmentMembership;
import py.periodic.PeriodicWorkExecutor;
import py.periodic.UnableToStartException;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.storage.Storage;
import py.storage.StorageExceptionHandlerChain;
import py.storage.impl.AsyncStorage;
import py.storage.impl.AsynchronousFileChannelStorageFactory;
import py.storage.impl.PriorityStorageImpl;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.Iface;
import py.thrift.datanode.service.MigratePrimaryRequest;
import py.thrift.infocenter.service.ReportArchivesResponse;
import py.thrift.infocenter.service.ReportSegmentUnitsMetadataResponse;
import py.thrift.infocenter.service.SegUnitConflictThrift;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.CheckSecondaryInactiveThresholdModeThrift;
import py.thrift.share.CheckSecondaryInactiveThresholdThrift;
import py.thrift.share.DatanodeTypeThrift;
import py.thrift.share.InvalidGroupExceptionThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.NextActionInfoThrift;
import py.thrift.share.NextActionThrift;
import py.thrift.share.PageMigrationSpeedInfoThrift;
import py.thrift.share.PrimaryMigrateThrift;
import py.thrift.share.RebalanceTaskListThrift;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.SecondaryMigrateThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitStatusConflictCauseThrift;
import py.thrift.share.VolumeStatusThrift;

public class InformationReporter implements ArchiveStatusChangeFinishListener {
  private static final Logger logger = LoggerFactory.getLogger(InformationReporter.class);
  private final StorageExceptionHandlerChain storageExceptionHandlerChain;
  private final DelayedTaskEngine deleteSegmentUnitEngine;
  private final SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck;
  private final PluginPlugoutManager pluginPlugoutManager;

  // storage the archive in offlining status in order to check is offlined:
  // after all the segment unit is offlined, the archive is offlined
  private final List<Archive> archiveInOfflining = new CopyOnWriteArrayList<Archive>();
  private final List<Archive> archiveInOfflined = new CopyOnWriteArrayList<Archive>();
  private final List<Archive> archiveInDegrade = new CopyOnWriteArrayList<Archive>();
  private final HashedWheelTimer hashedWheelTimer;
  private SegmentUnitManager segmentUnitManager;
  private InformationCenterClientFactory informationCenterClientFactory;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory = null;
  private AppContext appContext;
  private RawArchiveManager archiveManager;
  private DataNodeConfiguration cfg;
  private DataNodeAppEngine engine;
  private InstanceStore instanceStore;
  private boolean arbiterDatanode = false;
  private boolean groupPersistFlg;
  private UnsettledArchiveManager unsettledArchiveManager;
  private PeriodicWorkExecutor segmentUnitReporterExecutor;
  private PeriodicWorkExecutor archiveReporterExecutor;
  private PeriodicWorkExecutor archiveUsedSpaceCalculationExecutor;
  private BackupDbReporter backupDbReporter;
  private AlarmReporter alarmReporter;
  private IoThrottleManager ioThrottleManager;
  private Lock lockForReportArchiveAndSegment = new ReentrantLock();
  /**
   * report the designated volume to  designated HA instance id, each volumes.
   **/
  private Map<Long, Set<Long>> whichHaThisVolumeToReportMap = new ConcurrentHashMap<>();

  /**
   * for Equilibrium volume volume id, instance(form) -> instance(to).
   ***/
  private Map<Long, Map<Long, Long>> volumeReportToInstancesSameTimeForEquilibriumMap =
      new ConcurrentHashMap<>();
  /**
   * the infocenter update the version.
   ****/
  private AtomicLong equilibriumVolumeVersion = new AtomicLong(0);

  /**
   * if the volume equilibrium ok, next time report Archives, set the value to master, the master
   * update the equilibrium table.
   ***/
  private Set<Long> volumeUpdateReportTableOk = new HashSet<>();
  private ExecutorService createSegmentsExecutor;

  public InformationReporter(SegmentUnitManager segmentUnitManager,
      RawArchiveManager archiveManager,
      InformationCenterClientFactory informationCenterClientFactory,
      AppContext appContext, DataNodeConfiguration dataNodeCfg, DataNodeAppEngine engine,
      SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck,
      BackupDbReporter backupDbReporter, InstanceStore instanceStore,
      StorageExceptionHandlerChain storageExceptionHandlerChain,
      GenericThriftClientFactory<Iface> dataNodeSyncClientFactory,
      PluginPlugoutManager pluginPlugoutManager, IoThrottleManager ioThrottleManager,
      HashedWheelTimer hashedWheelTimer)
      throws EndPointNotFoundException, TException, GenericThriftClientFactoryException,
      TooManyEndPointFoundException {
    this.segmentUnitManager = segmentUnitManager;
    this.archiveManager = archiveManager;
    this.appContext = appContext;
    this.cfg = dataNodeCfg;
    this.engine = engine;
    this.informationCenterClientFactory = informationCenterClientFactory;
    this.segmentUnitCanDeletingCheck = segmentUnitCanDeletingCheck;
    this.groupPersistFlg = false;
    this.backupDbReporter = backupDbReporter;
    this.instanceStore = instanceStore;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
    this.pluginPlugoutManager = pluginPlugoutManager;
    this.storageExceptionHandlerChain = storageExceptionHandlerChain;
    this.ioThrottleManager = ioThrottleManager;
    this.hashedWheelTimer = hashedWheelTimer;

    ExecutionOptionsReader segmentUnitReporterOption = new ExecutionOptionsReader(1, 1,
        cfg.getRateOfReportingSegmentsMs(), null);
    segmentUnitReporterExecutor = new PeriodicWorkExecutorImpl(segmentUnitReporterOption,
        new WorkerFactory() {
          @Override
          public Worker createWorker() {
            return new SegmentUnitReporter();
          }
        }, "SegmentUnitReporter");

    ExecutionOptionsReader archiveReporterOption = new ExecutionOptionsReader(1, 1,
        cfg.getRateOfReportingArchivesMs(), null);
    archiveReporterExecutor = new PeriodicWorkExecutorImpl(archiveReporterOption,
        new WorkerFactory() {
          @Override
          public Worker createWorker() {
            return new ArchiveReporter();
          }
        }, "ArchiveReporter");

    deleteSegmentUnitEngine = new DelayedTaskEngine();
    deleteSegmentUnitEngine.setDaemon(true);
    deleteSegmentUnitEngine.setPrefix("delete segment unit when slow disk");
    deleteSegmentUnitEngine.start();
    createSegmentsExecutor = createSegmentUnitsExecutor();

    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, null,
        cfg.getArchiveUsedSpaceCalculateDelayMs());
    archiveUsedSpaceCalculationExecutor = new PeriodicWorkExecutorImpl(optionReader,
        new WorkerFactory() {
          @Override
          public Worker createWorker() {
            return new ArchiveUsedSpace();
          }
        }, "usedSpaceCalculate");
  }

  private static int cmdRun(String[] cmd) {
    logger.info("the cmd running is: {}", Arrays.asList(cmd));

    BufferedReader reader = null;
    BufferedReader error = null;
    try {
      Process linkCmd = Runtime.getRuntime().exec(cmd);
      reader = new BufferedReader(new InputStreamReader(linkCmd.getInputStream()));
      error = new BufferedReader(new InputStreamReader(linkCmd.getErrorStream()));
      try {
        linkCmd.waitFor();
      } catch (Exception e) {
        return -1;
      }

      String line;
      while ((line = reader.readLine()) != null) {
        logger.debug("read line=" + line);
      }

      while ((line = error.readLine()) != null) {
        logger.error("error line=" + line);
      }

      return linkCmd.exitValue();
    } catch (IOException e) {
      logger.warn("caught an exception", e);
      return -1;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("", e);
        }
      }

      if (error != null) {
        try {
          error.close();
        } catch (Exception e) {
          logger.error("", e);
        }
      }
    }
  }

  public void start() {
    if (informationCenterClientFactory == null) {
      logger.info(
          "don't need to start the archive reporter,"
              + " becuase the information center factory is null");
      return;
    }
    try {
      segmentUnitReporterExecutor.start();
      archiveReporterExecutor.start();
      archiveUsedSpaceCalculationExecutor.start();
    } catch (UnableToStartException e) {
      logger.warn("Can't start the membership workers ", e);
    }
  }

  public void stop() {
    if (segmentUnitReporterExecutor != null) {
      segmentUnitReporterExecutor.stopNow();
    }
    if (archiveReporterExecutor != null) {
      archiveReporterExecutor.stopNow();
    }

    if (archiveUsedSpaceCalculationExecutor != null) {
      archiveUsedSpaceCalculationExecutor.stopNow();
    }
    createSegmentsExecutor.shutdownNow();
  }

  @Override
  public void becomeOfflining(Archive archive, ArchiveStatus oldStatus) {
   
   
   

    ArchiveStatus newStatus = ArchiveStatus.OFFLINING;
    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}."
              + " But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    if (archiveInOfflining.contains(archive)) {
      logger.warn("some archive has been monitored by this thread {}", archive);
    } else {
      archiveInOfflining.add(archive);
    }

    this.reportArchiveToInfoCenter();
  }

  @Override
  public void becomeGood(Archive archive, ArchiveStatus oldStatus) {
    ArchiveStatus newStatus = ArchiveStatus.GOOD;
    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}."
              + " But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    becomeNotOfflining(archive);
  }

  @Override
  public void becomeOfflined(Archive archive, ArchiveStatus oldStatus) {
    ArchiveStatus newStatus = ArchiveStatus.OFFLINED;
    archiveInOfflined.add(archive);
    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}."
              + " But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    becomeNotOfflining(archive);
  }

  @Override
  public void becomeEjected(Archive archive, ArchiveStatus oldStatus) {
    ArchiveStatus newStatus = ArchiveStatus.EJECTED;
    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == ArchiveStatus.OFFLINED && archiveInOfflined.contains(archive)) {
      archiveInOfflined.remove(archive);
    }

    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}."
              + " But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    becomeNotOfflining(archive);
  }

  @Override
  public void becomeConfigMismatch(Archive archive, ArchiveStatus oldStatus) {
    ArchiveStatus newStatus = ArchiveStatus.CONFIG_MISMATCH;
    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}."
              + " But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    becomeNotOfflining(archive);
  }

  @Override
  public void becomeDegrade(Archive archive, ArchiveStatus oldStatus) {
    ArchiveStatus newStatus = ArchiveStatus.DEGRADED;
    if (archive instanceof RawArchive) {
      if (!archiveInDegrade.contains(archive)) {
        DeleteAllSegmentUnitInArchiveTask task = new DeleteAllSegmentUnitInArchiveTask(
            (RawArchive) archive);
        deleteSegmentUnitEngine.drive(task);
        archiveInDegrade.add(archive);
        logger.warn("the archive {} will degrade, all segment unit {} will deleting", archive,
            ((RawArchive) archive).getSegmentUnits());
      }
    }

    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}."
              + " But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    becomeNotOfflining(archive);
  }

  @Override
  public void becomeInProperlyEjected(Archive archive, ArchiveStatus oldStatus) {
    ArchiveStatus newStatus = ArchiveStatus.INPROPERLY_EJECTED;
    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == ArchiveStatus.OFFLINED && archiveInOfflined.contains(archive)) {
      archiveInOfflined.remove(archive);
    }
    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}."
              + " But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    becomeNotOfflining(archive);
  }

  @Override
  public void becomeBroken(Archive archive, ArchiveStatus oldStatus) {
    ArchiveStatus newStatus = ArchiveStatus.BROKEN;
    if (archive.getArchiveMetadata().getStatus() != newStatus) {
      logger.warn("Archive status change report from {} to {}. But status in archive meta is {}.",
          oldStatus.name(), newStatus.name(), archive.getArchiveMetadata().getStatus());
      return;
    }

    if (oldStatus == newStatus) {
      logger.info(
          "Archive status change report from {} to {}. "
              + "But status is the same. Don't report to alarm system.",
          oldStatus.name(), newStatus.name());
    } else {
      reportArchiveStatusToAlarmSystem(archive, oldStatus, newStatus);
    }

    becomeNotOfflining(archive);
  }

  private void reportArchiveStatusToAlarmSystem(Archive archive, ArchiveStatus oldStatus,
      ArchiveStatus newStatus) {
    logger
        .info("Archive status change report from [{}] to [{}] archive:[{}].", oldStatus, newStatus,
            archive);

    AlarmReportData alarm = AlarmReportData
        .generateArchiveStatusAlarm(archive, oldStatus, newStatus);
    getAlarmReporter().submit(alarm);
  }

  public void becomeNotOfflining(Archive archive) {
    if (archive.getArchiveMetadata().getStatus() == ArchiveStatus.OFFLINING) {
      logger.warn("the archive status is also offlining");
      return;
    }

    if (archiveInOfflining.contains(archive)) {
      archiveInOfflining.remove(archive);
    } else {
      logger.warn("archive {} is not in archiveInOfflining list", archive);
    }
    try {
      this.reportArchiveToInfoCenter();
    } catch (Exception e) {
      logger.error("report error {}", e);
    }
  }

  private void reportSegmentUnitToInfoCenter() {
   
    ArrayList<Archive> archiveToBeDeletedList = new ArrayList<Archive>();
    logger.info("reportSegmentUnitToInfoCenter start");
    for (Archive archive : archiveInOfflining) {
      try {
        logger.warn("the offing archive {}", archive);
        archive.setArchiveStatus(ArchiveStatus.OFFLINED);
        LogFlushedChecker.archiveWithLastTimeCheckNeedRollBack
            .remove(archive.getArchiveMetadata().getArchiveId());
        archiveToBeDeletedList.add(archive);
      } catch (Exception e) {
        logger.error("set archive status failed: archive is {} ", archive, e);
      }
    }

    archiveInOfflining.removeAll(archiveToBeDeletedList);

    ArrayList<Archive> archiveToBeRemoveList = new ArrayList<Archive>();
    File unsettledFile = new File(cfg.getArchiveConfiguration().getPersistRootDir(),
        cfg.getArchiveConfiguration().getUnsettledArchiveDir());
    for (Archive archive : archiveInOfflined) {
      if (new File(archive.getStorage().identifier()).exists()) {
        String cmdStr =
            "mv " + archive.getStorage().identifier() + " " + unsettledFile.getAbsolutePath() + "/";
        String[] cmdLink = {"/bin/sh", "-c", cmdStr};
        logger.warn("running the command={}", Arrays.asList(cmdLink));
        if (0 != cmdRun(cmdLink)) {
          logger.error("mv link file error");
          continue;
        }
      }

      String linkname = FilenameUtils.getBaseName(archive.getStorage().identifier());
      File file = new File(cfg.getArchiveConfiguration().getPersistRootDir(),
          cfg.getArchiveConfiguration().getUnsettledArchiveDir());
      file = new File(file, linkname);
      logger.warn("the link file is {}", file.getAbsoluteFile());
      UnsettledArchive unsettledArchive;
      try {
        Storage newStorage = new PriorityStorageImpl(
            (AsyncStorage) AsynchronousFileChannelStorageFactory.getInstance()
                .generate(file.getAbsolutePath()), cfg.getWriteTokenCount(),
            storageExceptionHandlerChain, hashedWheelTimer, cfg.getStorageIoTimeout(),
            (int) ArchiveOptions.PAGE_PHYSICAL_SIZE);
        newStorage.close();
        AsynchronousFileChannelStorageFactory.getInstance().close(newStorage);
        UnsettledArchiveMetadata unsettledArchiveMetadata = new UnsettledArchiveMetadata(
            archive.getArchiveMetadata());
        unsettledArchiveMetadata.setArchiveType(ArchiveType.UNSETTLED_DISK);
        unsettledArchive = new UnsettledArchive(newStorage, unsettledArchiveMetadata);

      } catch (Exception e) {
        logger
            .warn("caught an exception when try to open new storage:{}", file.getAbsolutePath(), e);
        continue;
      }

      try {
        switch (archive.getArchiveMetadata().getArchiveType()) {
          case RAW_DISK:
            ((UnsettledArchiveMetadata) unsettledArchive.getArchiveMetadata())
                .addOriginalType(ArchiveType.RAW_DISK);
            try {
              archiveManager.freeArchiveWithOutCheck((RawArchive) archive);
            } catch (AllSegmentUnitDeadInArchviveException e) {
              archiveManager.removeArchive(((RawArchive) archive).getArchiveId(), false);
              try {
                pluginPlugoutManager.plugin(unsettledArchive);
                archiveToBeRemoveList.add(archive);
              } catch (Exception ple) {
                logger.error("plug in the archive error", ple);
                continue;
              }
            } catch (SegmentUnitBecomeBrokenException se) {
              logger.error("mark storage error");
              continue;
            }
            throw new ArchiveIsNotCleanedException();
          case UNSETTLED_DISK:
            archiveToBeRemoveList.add(archive);
            continue;
          default:
        }
      } catch (ArchiveIsNotCleanedException e) {
        continue;
      }
      AsynchronousFileChannelStorageFactory.getInstance().close(archive.getStorage());
    }

    archiveInOfflined.removeAll(archiveToBeRemoveList);

    Collection<SegmentUnit> units = segmentUnitManager.get();
    if (units.size() == 0) {
      return;
    }

    long masterInstanceId = 0L;
    Map<Long, List<SegmentUnitMetadata>> instanceIdListMap = new HashMap<>();
    Map<Long, Instance> instanceIdToInstance = new HashMap<>();
    Set<Instance> instanceFlower = Utils.getAllSuspendInfoCenter(instanceStore);

    for (Instance instance : instanceFlower) {
      instanceIdToInstance.put(instance.getId().getId(), instance);
      instanceIdListMap.put(instance.getId().getId(), new ArrayList<>());
    }
    Set<Instance> instanceMaster = instanceStore
        .getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY);
    if (instanceMaster.size() == 0) {
      logger.warn("when report Segment Unit,get the master infocenter error:");
      return;
    }
    for (Instance instance : instanceMaster) {
      instanceIdToInstance.put(instance.getId().getId(), instance);
      masterInstanceId = instance.getId().getId();
      instanceIdListMap.put(masterInstanceId, new ArrayList<>());
    }

    Map<Long, Set<Long>> volumeIdToInstanceIds = new HashMap<>();
    for (Map.Entry<Long, Set<Long>> entry : whichHaThisVolumeToReportMap.entrySet()) {
      Long infoCenterId = entry.getKey();
      if (instanceIdListMap.get(infoCenterId) == null) {
       
        logger.warn(
            "in report volume, find one foCenter:{}, "
                + "may be inc, so remove it in table, the before map:{}",
            infoCenterId, whichHaThisVolumeToReportMap);
        whichHaThisVolumeToReportMap.remove(infoCenterId);
        Set<Long> availableHa = new HashSet<>();
        availableHa.add(masterInstanceId);
        for (Long volumeId : entry.getValue()) {
          volumeIdToInstanceIds.put(volumeId, availableHa);
        }
        continue;
      }

      for (Long volumeId : entry.getValue()) {
        Set<Long> availableHa = new HashSet<>();
        availableHa.add(infoCenterId);
        if (volumeReportToInstancesSameTimeForEquilibriumMap.containsKey(volumeId)) {
          Map<Long, Long> oldToNewInstance = volumeReportToInstancesSameTimeForEquilibriumMap
              .get(volumeId);
          availableHa.addAll(oldToNewInstance.values());
        }

        Iterator<Long> iterator = availableHa.iterator();
        while (iterator.hasNext()) {
          Long instanceId = iterator.next();
          if (instanceIdListMap.get(instanceId) == null) {
           
            iterator.remove();
          }
        }

        if (availableHa.size() == 0) {
          availableHa.add(masterInstanceId);
        }

        if (volumeIdToInstanceIds.containsKey(volumeId)) {
          logger.warn(
              "in report volume:{}, find it will report:{},"
                  + " but this time set report:{}, this not good",
              volumeId, volumeIdToInstanceIds.get(volumeId), availableHa);
        }

        volumeIdToInstanceIds.put(volumeId, availableHa);
      }
    }

    GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection deletingSegmentUnitCollection;
    Map<SegId, GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection>
        segIdDeletingSegmentUnitCollectionMap
        = GlobalDeletingSegmentUnitCollection.buildCurrentHashMap();
    Map<SegId, GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection>
        processingSegmentUnitReport
        = new HashMap<>();
    for (SegmentUnit unit : units) {
      SegmentUnitMetadata metadata = unit.getSegmentUnitMetadata();

      if (null == (deletingSegmentUnitCollection = segIdDeletingSegmentUnitCollectionMap
          .get(unit.getSegId()))) {
        saveMap(instanceIdListMap, metadata, volumeIdToInstanceIds, masterInstanceId);
        SecondaryCopyPageManager copyPageUnitManger = unit.getSecondaryCopyPageManager();
        if (copyPageUnitManger != null) {
          metadata.setRatioMigration(copyPageUnitManger.progress());
        } else {
          metadata.setRatioMigration(0);
        }
      } else {
        if (GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitStatus.Begin
            == deletingSegmentUnitCollection.status) {
          logger.debug(
              "a deleting segment unit {} need to report deleted status"
                  + " to infocenter, using deleted status",
              segIdDeletingSegmentUnitCollectionMap.get(unit.getSegId()).getSegmentUnitMetadata());
          saveMap(instanceIdListMap,
              segIdDeletingSegmentUnitCollectionMap.get(unit.getSegId())
                  .getSegmentUnitMetadata(), volumeIdToInstanceIds, masterInstanceId);
          processingSegmentUnitReport.put(unit.getSegId(),
              segIdDeletingSegmentUnitCollectionMap.get(unit.getSegId()));
        }
      }
    }

    for (Map.Entry entry : segIdDeletingSegmentUnitCollectionMap.entrySet()) {
      SegId segId = (SegId) entry.getKey();
      deletingSegmentUnitCollection =
          (GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection) entry.getValue();
      if (null == processingSegmentUnitReport.get(segId)
          && GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitStatus.Begin
          == deletingSegmentUnitCollection.status) {
        logger.debug("a deleting segment unit {} need to report deleted status to infocenter",
            segIdDeletingSegmentUnitCollectionMap.get(segId).getSegmentUnitMetadata());
        saveMap(instanceIdListMap,
            deletingSegmentUnitCollection.getSegmentUnitMetadata(), volumeIdToInstanceIds,
            masterInstanceId);
      }
    }

    CountDownLatch countDownLatch = new CountDownLatch(instanceIdListMap.size());
    for (Entry<Long, List<SegmentUnitMetadata>> entry : instanceIdListMap.entrySet()) {
      Instance instance = instanceIdToInstance.get(entry.getKey());
      ReportSegmentUnitToHaInstanceTask reportSegmentUnitToHaInstanceTask =
          new ReportSegmentUnitToHaInstanceTask(
              segIdDeletingSegmentUnitCollectionMap, entry.getValue(), instance.getEndPoint());
      CompletableFuture
          .runAsync(reportSegmentUnitToHaInstanceTask, createSegmentsExecutor)
          .thenRun(() -> {
            countDownLatch.countDown();
          })
          .exceptionally(throwable -> {
            countDownLatch.countDown();
            return null;
          });
    }
    logger.info("reportSegmentUnitToInfoCenter end");
    try {
      countDownLatch.await(10000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("", e);
    }
  }

  private void saveMap(Map<Long, List<SegmentUnitMetadata>> instanceIdToSegmentunits,
      SegmentUnitMetadata segmentUnitMetadata, Map<Long, Set<Long>> volumeIdToInstanceIds,
      Long masterId) {
    Set<Long> instanceIds = volumeIdToInstanceIds
        .get(segmentUnitMetadata.getSegId().getVolumeId().getId());

    if (Objects.isNull(instanceIds)) {
      instanceIdToSegmentunits.get(masterId).add(segmentUnitMetadata);
    } else {
      for (Long instanceId : instanceIds) {
        instanceIdToSegmentunits.get(instanceId).add(segmentUnitMetadata);
      }
    }
  }

  protected void reportArchiveToInfoCenter() {
    List<RawArchive> rawArchiveList = new ArrayList<>();
    if (archiveManager != null) {
      rawArchiveList.addAll(archiveManager.getRawArchives());
    }

    List<Archive> unsettledArchives = new ArrayList<>();
    if (unsettledArchiveManager != null) {
      unsettledArchives.addAll(unsettledArchiveManager.getArchives());
    }
    logger.debug("now wo have archive {}", rawArchiveList);
   

    List<ArchiveMetadataThrift> allArchiveMetadatas = new ArrayList<>();
    long capacity = archiveManager.getTotalPhysicalCapacity();
    long logicalCapacity = archiveManager.getTotalLogicalSpace();
    long freeSpace = archiveManager.getTotalLogicalFreeSpace();
    Group groupFromContext = appContext.getGroup();
    InstanceDomain instanceDomain = appContext.getInstanceDomain();

    if (!arbiterDatanode) {
      lockForReportArchiveAndSegment.lock();
      try {
        calculateCopyPageRatioAndSpeed();
      } finally {
        lockForReportArchiveAndSegment.unlock();
      }
    }
   
    for (RawArchive archive : rawArchiveList) {
      archive.updateLogicalFreeSpaceToMetaData();

      RawArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
      ArchiveStatus status = archiveMetadata.getStatus();
      if (status == ArchiveStatus.EJECTED || status == ArchiveStatus.INPROPERLY_EJECTED) {
        Archive archiveBase = this.archiveManager.getRawArchive(archiveMetadata.getArchiveId());
       
        long pastTimeMs = System.currentTimeMillis() - archiveMetadata.getUpdatedTime();
        if (pastTimeMs > cfg.getThresholdToRemoveEjectedArchive()) {
          logger.warn(
              "an archive in EJECTED or INPROPERLY_EJECTED status long enough:{},"
                  + " config remove time:{}, remove it."
                  + " archive metadata is {}, archive update time:{}",
              pastTimeMs, cfg.getThresholdToRemoveEjectedArchive(), archiveMetadata,
              Utils.millsecondToString(archiveMetadata.getUpdatedTime()));

          try {
            this.archiveManager.removeArchive(archiveMetadata.getArchiveId(), true);
          } catch (ArchiveIsNotCleanedException del) {
            continue;
          } catch (Exception e) {
            logger.warn("caught an exception", e);
          } finally {
           
            if (status == ArchiveStatus.INPROPERLY_EJECTED && archiveBase != null) {
              logger.error("set the storage broken: {}", archiveBase);
              archiveBase.getStorage().setBroken(true);
            }
          }
        }
      }
      ArchiveMetadataThrift archiveMetadataThrift = RequestResponseHelper
          .buildThriftArchiveMetadataFrom(archiveMetadata);
      archiveMetadataThrift.setMigrateFailedSegIdList(
          Optional.ofNullable(archiveMetadata.drainAllMigrateFailedSegIds())
              .orElse(new ArrayList<>())
              .stream().map(segId -> RequestResponseHelper.buildThriftSegIdFrom(segId))
              .collect(Collectors.toList()));
      allArchiveMetadatas.add(archiveMetadataThrift);
    }

    if (unsettledArchives != null && unsettledArchives.size() != 0) {
      for (Archive archive : unsettledArchives) {
        ArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
        ArchiveStatus archiveStatus = archiveMetadata.getStatus();
        if (archiveStatus == ArchiveStatus.EJECTED
            || archiveStatus == ArchiveStatus.INPROPERLY_EJECTED) {
         
          long pastTimeMs = System.currentTimeMillis() - archiveMetadata.getUpdatedTime();
          if (pastTimeMs > cfg.getThresholdToRemoveEjectedArchive()) {
            logger.warn(
                "an archive in EJECTED or INPROPERLY_EJECTED status long enough:{}, "
                    + "config remove time:{}, remove it. archive metadata is {},"
                    + " archive update time:{}",
                pastTimeMs, cfg.getThresholdToRemoveEjectedArchive(), archiveMetadata,
                Utils.millsecondToString(archiveMetadata.getUpdatedTime()));
            UnsettledArchive archiveBase = (UnsettledArchive) this.unsettledArchiveManager
                .getArchive(archiveMetadata.getArchiveId());
            try {
              this.unsettledArchiveManager.removeArchive(archiveMetadata.getArchiveId());
            } catch (Exception e) {
              logger.warn("caught an exception", e);
            } finally {
             
              if (archiveStatus == ArchiveStatus.INPROPERLY_EJECTED && archiveBase != null) {
                logger.error("set the storage broken: {}", archiveBase);
                archiveBase.getStorage().setBroken(true);
              }
            }
          }
        }

        allArchiveMetadatas
            .add(RequestResponseHelper.buildThriftArchiveMetadataFrom(archiveMetadata));
      }
    }

    InformationCenterClientWrapper informationClient = null;
    ReportArchivesResponse reportArchivesRsp = null;
    logger.info("reporting archives to infocenter {}", allArchiveMetadatas);
    try {
      informationClient = informationCenterClientFactory.build();
      EndPoint endPoint = appContext.getEndPoints().get(PortType.CONTROL);
     
      ReportDbRequestThrift reportDbRequest = this.backupDbReporter
          .buildReportDbRequest(endPoint, groupFromContext, appContext.getInstanceId().getId(),
              false);
      DatanodeTypeThrift datanodeTypeThrift = isArbiterDatanode()
          ? DatanodeTypeThrift.SIMPLE :
          DatanodeTypeThrift.NORMAL;

      reportArchivesRsp = informationClient
          .reportArchives(appContext.getInstanceId(), endPoint, capacity, freeSpace,
              logicalCapacity,
              allArchiveMetadatas, groupFromContext, instanceDomain, reportDbRequest,
              datanodeTypeThrift, volumeUpdateReportTableOk);
    } catch (InvalidGroupExceptionThrift e1) {
      logger.error("caught a invalid group exception, system exit ", e1);
     
    } catch (InvalidInputExceptionThrift e2) {
      logger.error("caught a invalid report archive request exception, ", e2);
    } catch (Exception e) {
      StringBuilder builder = new StringBuilder("\n");
      for (ArchiveMetadataThrift unit : allArchiveMetadatas) {
        builder.append(unit.toString());
        builder.append("\n");
      }
      logger.warn("caught an exception when reporting archives: " + builder.toString(), e);
    }
    

    if (!groupPersistFlg && groupFromContext == null && reportArchivesRsp != null && (
        reportArchivesRsp.getGroup()
            != null)) {
      try {
        Group groupToSync = RequestResponseHelper.buildGroupFrom(reportArchivesRsp.getGroup());
        appContext.setGroupInfo(groupToSync);
        logger.warn("set group to {}", groupToSync);
        archiveManager.setGroup(groupToSync);
        groupPersistFlg = true;
      } catch (Exception e) {
        logger.error("failed to persist group{} info to config file,datanode won't work any more",
            reportArchivesRsp.getGroup(), e);
       
       
       
        engine.stop();
      }
    }

    if (reportArchivesRsp != null && reportArchivesRsp.getDatanodeNextAction() != null) {
      NextActionInfoThrift datanodeNextActionInfo = reportArchivesRsp.getDatanodeNextAction();
      NextActionThrift nextAction = datanodeNextActionInfo.getNextAction();
      if (nextAction == NextActionThrift.KEEP) {
       
        logger.info("datanode action:{} domain:{}", nextAction, appContext.getInstanceDomain());
      } else if (nextAction == NextActionThrift.NEWALLOC) {
        logger.warn("datanode action:{}, set new domain id:{} recover current domain id:{}",
            nextAction,
            datanodeNextActionInfo.getNewId(), appContext.getInstanceDomain());
        InstanceDomain currentInstanceDomain = appContext.getInstanceDomain();
        Validate
            .isTrue(currentInstanceDomain == null || currentInstanceDomain.getDomainId() == null);
        appContext.setInstanceDomain(new InstanceDomain(datanodeNextActionInfo.getNewId()));
      } else if (nextAction == NextActionThrift.FREEMYSELF) {
        InstanceDomain currentInstanceDomain = appContext.getInstanceDomain();
       
        logger.warn("datanode action:{}, this datanode has been removed from last domain:{}",
            nextAction,
            currentInstanceDomain.getDomainId());
       
       
        Collection<RawArchive> archives = archiveManager.getRawArchives();
        int archvieSize = archives.size();
        for (RawArchive archive : archives) {
          if (freeStoragePool(archive)) {
            archvieSize--;
          }
        }
       
        if (archvieSize == 0) {
          currentInstanceDomain.setDomainId(null);
          logger.warn("free domain success");
        }
       
        appContext.setInstanceDomain(currentInstanceDomain);
      } else if (nextAction == NextActionThrift.CHANGE) {
        InstanceDomain currentInstanceDomain = appContext.getInstanceDomain();
       
        logger
            .warn("datanode action:{}, domain id has changed, original domain id:{}, new domain:{}",
                nextAction, currentInstanceDomain, datanodeNextActionInfo.getNewId());
       
       
        Collection<RawArchive> archives = archiveManager.getRawArchives();
        int archvieSize = archives.size();
        for (RawArchive archive : archives) {
          if (freeStoragePool(archive)) {
            archvieSize--;
          }

        }
       
        if (archvieSize == 0) {
          currentInstanceDomain.setDomainId(datanodeNextActionInfo.getNewId());
          logger.warn("free domain success");
        }
        appContext.setInstanceDomain(currentInstanceDomain);
      }
    }
   
    if (reportArchivesRsp != null && reportArchivesRsp.getArchiveIdMapNextAction() != null) {
      Map<Long, NextActionInfoThrift> archiveIdMapNextAction = reportArchivesRsp
          .getArchiveIdMapNextAction();
      for (Entry<Long, NextActionInfoThrift> entry : archiveIdMapNextAction.entrySet()) {
        Long archiveId = entry.getKey();
        Archive archiveBase = archiveManager.getRawArchive(archiveId);
        RawArchive archive = (RawArchive) archiveBase;
        NextActionInfoThrift archiveNextActionInfo = entry.getValue();
        NextActionThrift nextAction = archiveNextActionInfo.getNextAction();
        try {
          if (nextAction == NextActionThrift.KEEP) {
           
            if (archiveBase != null) {
              logger
                  .info("archive action:{}, archive id:{}, current storage pool id:{}", nextAction,
                      archiveId, archive.getStoragePoolId());
            } else {
              logger.info(
                  "archive action:{}, archive id:{}, current storage pool id:{},archive is NULL",
                  nextAction, archiveId);
            }
          } else if (nextAction == NextActionThrift.NEWALLOC) {
            Validate.isTrue(archive.getStoragePoolId() == null);
            logger.warn("archive action:{}, set new storage pool id:{} to archive:{}", nextAction,
                archiveNextActionInfo.getNewId(), archiveId);
            archiveManager.setStoragePool(archive, archiveNextActionInfo.getNewId());
          } else if (nextAction == NextActionThrift.FREEMYSELF) {
            logger.warn("next action is free myself");
            freeStoragePool(archive);
          } else if (nextAction == NextActionThrift.CHANGE) {
            logger.warn("next action is change");
            freeStoragePool(archive);
          }
        } catch (JsonProcessingException | StorageException e) {
          logger.warn("caught an exception", e);
        }
      }
    }

    try {
      if (reportArchivesRsp != null && reportArchivesRsp.getReportDbResponse() != null) {
        this.backupDbReporter.processRsp(reportArchivesRsp.getReportDbResponse());
        logger.debug("process DB response:{} done", reportArchivesRsp.getReportDbResponse());
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }

    if (reportArchivesRsp != null) {
      Map<Long, PageMigrationSpeedInfoThrift> mapMigrationSpeed = reportArchivesRsp
          .getArchiveIdMapMigrationSpeed();
      Map<Long, String> mapMigrationStrategy = reportArchivesRsp.getArchiveIdMapMigrationStrategy();
      if (mapMigrationSpeed != null && mapMigrationStrategy != null) {
        logger.debug("response migration speed {} strategy {}", mapMigrationSpeed,
            mapMigrationStrategy);
        for (Long archiveId : mapMigrationStrategy.keySet()) {
          MigrationStrategy strategy = MigrationStrategy
              .valueOf(mapMigrationStrategy.get(archiveId));
          RawArchive archive = archiveManager.getRawArchive(archiveId);
          if (archive == null) {
            continue;
          }
          archive.getArchiveMetadata().setMigrationStrategy(strategy);
          if (!MigrationStrategy.Smart.equals(strategy)) {
            long speedMb = mapMigrationSpeed.get(archiveId).getMaxMigrationSpeed();
            archive.getArchiveMetadata().setMaxMigrationSpeed(speedMb * 1024);
          } else {
            archive.getArchiveMetadata().setMaxMigrationSpeed(cfg.getSmartMaxCopySpeedMb() * 1024);
          }

        }
      }
     
      Map<Long, Map<Long, VolumeStatusThrift>> mapStoragePoolToVolumeStatusMap = reportArchivesRsp
          .getEachStoragePoolVolumesStatus();
      Map<Long, Boolean> mapStoragePoolToStable = mapStoragePoolToVolumeStatusMap.keySet().stream()
          .collect(Collectors.toMap(Function.identity(), storagePoolId ->
              mapStoragePoolToVolumeStatusMap.get(storagePoolId).values().stream()
                  .allMatch(statusThrift -> statusThrift == VolumeStatusThrift.Stable)));

      archiveManager.getRawArchives().forEach(archive -> {
        Boolean stable = mapStoragePoolToStable.get(archive.getStoragePoolId());
        if (stable != null && stable) {
         
          archive.getSegmentUnits().forEach(segmentUnit -> {
            SegmentUnitMetadata segmentUnitMetadata = segmentUnit.getSegmentUnitMetadata();
            segmentUnitMetadata.setMigrationSpeed(0);
            segmentUnitMetadata.setAlreadyMigratedPage(0);
            segmentUnitMetadata.setTotalPageToMigrate(0);
          });
        }
      });
    }

    if (reportArchivesRsp != null
        && reportArchivesRsp.getArchiveIdMapCheckSecondaryInactiveThreshold() != null) {
      for (Entry<Long, CheckSecondaryInactiveThresholdThrift> entry : reportArchivesRsp
          .getArchiveIdMapCheckSecondaryInactiveThreshold().entrySet()) {
        Archive rawArchive;
        if (null == (rawArchive = archiveManager.getRawArchive(entry.getKey()))) {
          continue;
        }
        CheckSecondaryInactiveThresholdThrift thresholdThrift = entry.getValue();
        CheckSecondaryInactive checkSecondaryInactive = NewSegmentUnitExpirationThreshold
            .getExpirationThreshold(rawArchive);
        logger.debug("get the threshold {}", thresholdThrift);
        if (checkSecondaryInactive == null
            || checkSecondaryInactive.getCheckMode() != transformMode(
            thresholdThrift.getMode())) {
          
          switch (thresholdThrift.getMode()) {
            case AbsoluteTime:
              checkSecondaryInactive = NewSegmentUnitExpirationThreshold
                  .createCheckSecondaryInactiveByAbsoluteTime(
                      thresholdThrift.isIgnoreMissPagesAndLogs(),
                      thresholdThrift.getStartTime(), thresholdThrift.getEndTime());
              break;
            case RelativeTime:
              if (thresholdThrift.getWaitTime() == -1) {
                thresholdThrift.setWaitTime(
                    TimeUnit.MILLISECONDS.toSeconds(cfg.getThresholdToRequestForNewMemberMs()));
              }
              checkSecondaryInactive = NewSegmentUnitExpirationThreshold
                  .createCheckSecondaryInactiveByRelativeTime(
                      thresholdThrift.isIgnoreMissPagesAndLogs(),
                      thresholdThrift.getWaitTime());
              break;
            default:
              logger.error("the mode is {}", thresholdThrift.getMode());
              Validate.isTrue(false);
          }
          NewSegmentUnitExpirationThreshold
              .putExpirationThreshold(rawArchive, checkSecondaryInactive);
        } else {
         

          switch (thresholdThrift.getMode()) {
            case AbsoluteTime:
              ((CheckSecondaryInactiveByAbsoluteTime) checkSecondaryInactive)
                  .updateInfo(thresholdThrift.isIgnoreMissPagesAndLogs(),
                      thresholdThrift.getStartTime(), thresholdThrift.getEndTime());
              break;
            case RelativeTime:
              if (thresholdThrift.getWaitTime() == -1) {
                thresholdThrift.setWaitTime(
                    TimeUnit.MILLISECONDS.toSeconds(cfg.getThresholdToRequestForNewMemberMs()));
              }

              ((CheckSecondaryInactiveByRelativeTime) checkSecondaryInactive)
                  .updateInfo(thresholdThrift.isIgnoreMissPagesAndLogs(),
                      thresholdThrift.getWaitTime());
              break;
            default:
              logger.error("the mode is {}", thresholdThrift.getMode());
              Validate.isTrue(false);
          }
        }

      }
    }

    if (reportArchivesRsp != null && reportArchivesRsp.getRebalanceTasks() != null) {
      RebalanceTaskListThrift rebalanceTaskListThrift = reportArchivesRsp.getRebalanceTasks();

      if (rebalanceTaskListThrift.isSetPrimaryMigrateList()) {
        for (PrimaryMigrateThrift primaryMigrateThrift : rebalanceTaskListThrift
            .getPrimaryMigrateList()) {
          try {
            MigratePrimaryRequest request = new MigratePrimaryRequest(RequestIdBuilder.get(),
                primaryMigrateThrift.getTargetInstanceId(), primaryMigrateThrift.getSegId());

            engine.getDataNodeService().migratePrimary(request);
          } catch (TException e) {
            logger.warn("exception caught while migrating primary, {} ---> {} for {}",
                primaryMigrateThrift.getSrcInstanceId(), primaryMigrateThrift.getTargetInstanceId(),
                primaryMigrateThrift.getSegId(), e);
          }
        }
      }

      if (rebalanceTaskListThrift.isSetSecondaryMigrateList()) {
        for (SecondaryMigrateThrift secondaryMigrateThrift : rebalanceTaskListThrift
            .getSecondaryMigrateList()) {
          try {
            CreateSegmentUnitRequest createSegmentUnitRequest = new CreateSegmentUnitRequest(
                RequestIdBuilder.get(),
                secondaryMigrateThrift.getSegId().getVolumeId(),
                secondaryMigrateThrift.getSegId().getSegmentIndex(),
                secondaryMigrateThrift.getVolumeType(),
                secondaryMigrateThrift.getStoragePoolId(),
                secondaryMigrateThrift.getSegmentUnitType(),
                secondaryMigrateThrift.getSegmentWrapSize(),
                secondaryMigrateThrift.isEnableLaunchMultiDrivers(),
                secondaryMigrateThrift.getVolumeSource());
            createSegmentUnitRequest.setInitMembership(secondaryMigrateThrift.getInitMembership());
            createSegmentUnitRequest.setReplacee(secondaryMigrateThrift.getSrcInstanceId());
            createSegmentUnitRequest.setSecondaryCandidate(true);

            engine.getDataNodeService().createSegmentUnit(createSegmentUnitRequest);
          } catch (TException e) {
            logger.warn("exception caught while migrating secondary, {} ---> {} for {}",
                secondaryMigrateThrift.getSrcInstanceId(),
                secondaryMigrateThrift.getTargetInstanceId(),
                secondaryMigrateThrift.getSegId(), e);
          }
        }
      }  

    }  

    if (reportArchivesRsp != null
        && reportArchivesRsp.getVolumeReportToInstancesSameTimeSize() > 0) {
      volumeReportToInstancesSameTimeForEquilibriumMap = reportArchivesRsp
          .getVolumeReportToInstancesSameTime();
      logger.info("to get volumeReportToInstancesSameTimeForEquilibriumMap value :{} ",
          volumeReportToInstancesSameTimeForEquilibriumMap);
    }

    if (reportArchivesRsp != null && reportArchivesRsp.isEquilibriumOkAndClearValue()) {
      if (!volumeReportToInstancesSameTimeForEquilibriumMap.isEmpty()) {
        logger.warn("for Equilibrium ok or init begin, clear the Equilibrium");
      }
      volumeReportToInstancesSameTimeForEquilibriumMap.clear();
      equilibriumVolumeVersion.set(0L);
    }

    long updateVersion = 0;
    Map<Long, Map<Long, Long>> updateTheDatanodeReportTable = new ConcurrentHashMap<>();

    if (reportArchivesRsp != null) {
     
      volumeUpdateReportTableOk.clear();

      updateVersion = reportArchivesRsp.getUpdateReportToInstancesVersion();
      updateTheDatanodeReportTable = reportArchivesRsp.getUpdateTheDatanodeReportTable();
    }

    if (reportArchivesRsp != null && updateVersion > 0) {
      logger
          .info("for Equilibrium, get the form Version :{},local version :{} and update table :{}",
              updateVersion, equilibriumVolumeVersion.get(), updateTheDatanodeReportTable);
      if (updateVersion > equilibriumVolumeVersion.get()) {
        for (Map.Entry<Long, Map<Long, Long>> entry : updateTheDatanodeReportTable.entrySet()) {
          long volumeId = entry.getKey();
          Map<Long, Long> instanceInfo = entry.getValue();

          for (Map.Entry<Long, Long> entry1 : instanceInfo.entrySet()) {
            long oldFormReportInstanceId = entry1.getKey();
            long newToReportInstanceId = entry1.getValue();

            if (whichHaThisVolumeToReportMap.containsKey(oldFormReportInstanceId)) {
              logger.warn("begin to move volume:{} in old instance :{}", volumeId,
                  oldFormReportInstanceId);
              Set<Long> volumes = whichHaThisVolumeToReportMap.get(oldFormReportInstanceId);
              if (volumes != null) {
                volumes.remove(volumeId);
              }
            }

            if (!whichHaThisVolumeToReportMap.containsKey(newToReportInstanceId)) {
              Set<Long> volumes = new HashSet<>();
              whichHaThisVolumeToReportMap.put(newToReportInstanceId, volumes);
            }

            Set<Long> volumes = whichHaThisVolumeToReportMap.get(newToReportInstanceId);
            volumes.add(volumeId);

            volumeUpdateReportTableOk.add(volumeId);

            equilibriumVolumeVersion.set(updateVersion);
            logger.warn("begin to add volume:{} in new instance :{}", volumeId,
                newToReportInstanceId);
          }
        }

        logger.info("after update the report table, the table :{}, and update ok volumes :{}",
            whichHaThisVolumeToReportMap, volumeUpdateReportTableOk);
      }

    }

  }

  private CheckSecondaryInactiveThresholdMode transformMode(
      CheckSecondaryInactiveThresholdModeThrift modeThrift) {
    if (modeThrift == CheckSecondaryInactiveThresholdModeThrift.RelativeTime) {
      return CheckSecondaryInactiveThresholdMode.RelativeTime;
    }
    return CheckSecondaryInactiveThresholdMode.AbsoluteTime;
  }

  private boolean freeStoragePool(RawArchive archive) {
   
    logger.warn("free archive id:{}, storage pool id:{}", archive.getArchiveId(),
        archive.getStoragePoolId());
    try {
      archiveManager.freeArchiveWithOutCheck(archive);
    } catch (AllSegmentUnitDeadInArchviveException aud) {
      try {
        archiveManager.setStoragePool(archive, null);
        logger.warn("free archive {} success", archive.getArchiveId());
        return true;
      } catch (JsonProcessingException | StorageException e) {
        logger.warn("caught an exception", e);
      }
    } catch (SegmentUnitBecomeBrokenException e) {
      logger.warn("set the archive deleting error", e);
    }
    return false;
  }

  public AlarmReporter getAlarmReporter() {
    return alarmReporter;
  }

  public void setAlarmReporter(AlarmReporter alarmReporter) {
    this.alarmReporter = alarmReporter;
  }

  public void setUnsettledArchiveManager(UnsettledArchiveManager unsettledArchiveManager) {
    this.unsettledArchiveManager = unsettledArchiveManager;
  }

  public boolean isArbiterDatanode() {
    return arbiterDatanode;
  }

  public void setArbiterDatanode(boolean arbiterDatanode) {
    this.arbiterDatanode = arbiterDatanode;
  }

  private void calculateCopyPageRatioAndSpeed() {
    Collection<CopyPageSampleInfo> copyPageSampleInfos = ioThrottleManager.getCopyPageSampleInfos();
    if (copyPageSampleInfos.size() == 0) {
      logger.info("no segment is copying page...");
    } else {
     
      for (CopyPageSampleInfo sampleInfo : copyPageSampleInfos) {
        SegId segId = sampleInfo.getSegId();
        SegmentUnit segmentUnit = segmentUnitManager.get(segId);
        if (segmentUnit == null) {
          continue;
        }
        SegmentUnitMetadata segmentUnitMetadata = segmentUnit.getSegmentUnitMetadata();
        if (segmentUnitMetadata.getStatus().isFinalStatus()) {
          continue;
        }
        segmentUnitMetadata.setTotalPageToMigrate(sampleInfo.getTotal());
        segmentUnitMetadata.setAlreadyMigratedPage(sampleInfo.getAlready());
        segmentUnitMetadata.setMigrationSpeed(sampleInfo.getSpeed());
      }
    }

    for (RawArchive archive : archiveManager.getRawArchives()) {
      RawArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
     
      int oldSpeed = 0;
      SegmentUnitMetadata oldMigratedSegment = null;

      archiveMetadata.setTotalPageToMigrate(0);
      archiveMetadata.setAlreadyMigratedPage(0);
      archiveMetadata.setMigrationSpeed(0);
      int dataSizeMb = 0;
      for (SegmentUnit segmentUnit : archive.getSegmentUnits()) {
        SegmentUnitMetadata segmentUnitMetadata = segmentUnit.getSegmentUnitMetadata();
       
        int usedPageCount =
            segmentUnitMetadata.getPageCount() - segmentUnitMetadata.getFreePageCount();
        long segmentDataSizeMb = ((long) usedPageCount) * ArchiveOptions.PAGE_SIZE / 1024 / 1024;
        dataSizeMb += (int) segmentDataSizeMb;
       
        int segTotal = segmentUnitMetadata.getTotalPageToMigrate();
        int segAlready = segmentUnitMetadata.getAlreadyMigratedPage();
        int segSpeed = segmentUnitMetadata.getMigrationSpeed();

        if (!ioThrottleManager.exist(segmentUnit.getSegId())) {
          logger.info("segment had finished copy, set speed to zero {}", segmentUnitMetadata);

          if (segSpeed > 0) {
            oldSpeed = segSpeed;
            oldMigratedSegment = segmentUnitMetadata;
          }
         
         
          segAlready = segTotal;
          segmentUnitMetadata.setAlreadyMigratedPage(segAlready);
          segmentUnitMetadata.setMigrationSpeed(0);
        }
       
        archiveMetadata.addTotalPageToMigrate(segTotal);
        archiveMetadata.addAlreadyMigratedPage(segAlready);
        if (segSpeed > 0) {
          archiveMetadata.setMigrationSpeed(segSpeed);
          logger
              .info("set seg {} speed {} to archive {}", segSpeed, archiveMetadata.getArchiveId());
        }
      }
      archiveMetadata.setDataSizeMb(dataSizeMb);

      if (copyPageSampleInfos.size() > 0) {
       
       
        if (archiveMetadata.getMigrationSpeed() == 0 && oldSpeed > 0) {
          archiveMetadata.setMigrationSpeed(oldSpeed);
          oldMigratedSegment.setMigrationSpeed(oldSpeed);
        }
        logger.warn("archive {} is copying page, total {} already {} old {} speed {} kb/s",
            archiveMetadata.getArchiveId(), archiveMetadata.getTotalPageToMigrate(),
            archiveMetadata.getAlreadyMigratedPage(), oldSpeed,
            archiveMetadata.getMigrationSpeed());
      }
    }
  }

  private ExecutorService createSegmentUnitsExecutor() {
    return new ThreadPoolExecutor(1, 20,
        cfg.getRateOfReportingSegmentsMs() * 2, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
        new NamedThreadFactory("reportSegmentUnitWorker"));
  }

  private class SegmentUnitReporter implements Worker {
    @Override
    public void doWork() throws Exception {
      reportSegmentUnitToInfoCenter();
    }
  }

  private class ArchiveReporter implements Worker {
    @Override
    public void doWork() throws Exception {
      reportArchiveToInfoCenter();
    }
  }

  private class ArchiveUsedSpace implements Worker {
    @Override
    public void doWork() throws Exception {
      for (RawArchive rawArchive : archiveManager.getRawArchives()) {
        rawArchive.updateLogicalUsedSpaceToMetaData();
      }
    }
  }

  private class DeleteAllSegmentUnitInArchiveTask extends DelayedTask {
    private RawArchive archive;

    public DeleteAllSegmentUnitInArchiveTask(RawArchive archive) {
      super(0);
      this.archive = archive;
    }

    @Override
    public Result work() {
     
      boolean allArchiveHaveBeenDelted = true;
      if (archive.getSegmentUnits().size() == 0) {
        return ResultImpl.DEFAULT;
      }
      for (SegmentUnit segmentUnit : archive.getSegmentUnits()) {
        if (segmentUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleting
            || segmentUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleted) {
          continue;
        }
        SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
        Instance instance = instanceStore.get(membership.getPrimary());
        try {
          DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
              .generateSyncClient(instance.getEndPoint());

          if (dataNodeClient == null) {
            logger.error("datanodeCient is null because primary {} is not in store {}",
                membership.getPrimary(), instanceStore.getAll());
            continue;
          }
          dataNodeClient.departFromMembership(RequestResponseHelper
              .buildDepartFromMembershipRequest(segmentUnit.getSegId(), membership,
                  appContext.getInstanceId().getId()).setSynPersist(false));
        } catch (Exception e) {
          allArchiveHaveBeenDelted = false;
          logger.warn("the primary is {} deleting segmentunit error", instance.getEndPoint(), e);
          continue;
        }
      }

      if (!allArchiveHaveBeenDelted) {
        this.updateDelay(60000);
        deleteSegmentUnitEngine.drive(this);
      } else {
        archiveInDegrade.remove(archive);
      }
      return ResultImpl.DEFAULT;
    }
  }

  private class ConflictProcessorFactory {
    public ConflictProcessor createProcessor(SegUnitConflictThrift context,
        List<SegmentUnitMetadata> segUnitRecoverFailList) {
      logger.warn("create process for deal with the conflict context {}, segment unit list {}",
          context,
          segUnitRecoverFailList);
      if (context.getCause() == SegmentUnitStatusConflictCauseThrift.VolumeDeleted) {
        return new VolumeDeletedConflictProcessor(context);
      } else if (context.getCause() == SegmentUnitStatusConflictCauseThrift.VolumeRecycled) {
        return new VolumeRecycledConflictProcessor(context, segUnitRecoverFailList);
      }

      logger.warn("No processor to deal with current conflict");
      return null;
    }
  }

  private abstract class ConflictProcessor {
    protected SegUnitConflictThrift context;

    public abstract void dealWithConflict() throws Exception;
  }

  private class VolumeDeletedConflictProcessor extends ConflictProcessor {
    public VolumeDeletedConflictProcessor(SegUnitConflictThrift context) {
      this.context = context;
    }

    @Override
    public void dealWithConflict() throws Exception {
      SegId segId = new SegId(context.getVolumeId(), context.getSegIndex());
      try {
        segmentUnitCanDeletingCheck.deleteSegmentUnitWithOutCheck(segId, true);
      } catch (NoNeedToBeDeletingException e) {
        logger.error("", e);
      }
    }
  }

  private class VolumeRecycledConflictProcessor extends ConflictProcessor {
    private List<SegmentUnitMetadata> segUnitRecoverFailList;

    public VolumeRecycledConflictProcessor(SegUnitConflictThrift context,
        List<SegmentUnitMetadata> segUnitRecoverFailList) {
      this.context = context;
      this.segUnitRecoverFailList = segUnitRecoverFailList;
    }

    @Override
    public void dealWithConflict() throws Exception {
      SegId segId = new SegId(context.getVolumeId(), context.getSegIndex());
      try {
        archiveManager.recycleSegmentUnitFromDeleting(segId);
      } catch (SegmentUnitRecoverFromDeletingFailException ex) {
        logger.error("Recover the segId fail {}", ex.toString(), ex);
        SegmentUnitMetadata metadata = segmentUnitManager.get(segId).getSegmentUnitMetadata();
        segUnitRecoverFailList.add(metadata);
      }
    }
  }

  public class ReportSegmentUnitToHaInstanceTask implements Runnable {
    public Map<SegId, GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection>
        segIdDeletingSegmentUnitCollectionMap;
    public Collection<SegmentUnitMetadata> unitMetadatas;
    public EndPoint endPoint;

    public ReportSegmentUnitToHaInstanceTask(
        Map<SegId, GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection>
            segIdDeletingSegmentUnitCollectionMap,
        Collection<SegmentUnitMetadata> unitMetadatas, EndPoint endPoint) {
      this.segIdDeletingSegmentUnitCollectionMap = segIdDeletingSegmentUnitCollectionMap;
      this.unitMetadatas = unitMetadatas;
      this.endPoint = endPoint;
    }

    @Override
    public void run() {
      reportUnitToInfocenter(segIdDeletingSegmentUnitCollectionMap, unitMetadatas, endPoint);
    }

    private boolean reportUnitToInfocenter(
        Map<SegId, GlobalDeletingSegmentUnitCollection.DeletingSegmentUnitCollection>
            segIdDeletingSegmentUnitCollectionMap, Collection<SegmentUnitMetadata> unitMetadatas,
        EndPoint endPoint) {
      InformationCenterClientWrapper informationClient;
      ReportSegmentUnitsMetadataResponse response = null;
      try {
        if (endPoint == null) {
         
          informationClient = informationCenterClientFactory.build();
          logger.info("to report Units to master, the total unit size:{}", unitMetadatas.size());
        } else {
          informationClient = informationCenterClientFactory.build(endPoint);
          logger.info("to report Units to instance :{}, the total unit size:{}", endPoint,
              unitMetadatas.size());
        }

        lockForReportArchiveAndSegment.lock();
        try {
          response = informationClient
              .reportSegmentUnitsMetadata(appContext.getInstanceId(), unitMetadatas);
        } finally {
          lockForReportArchiveAndSegment.unlock();
        }

        if (response != null && response.getConflicts().size() != 0) {
          ConflictProcessorFactory factory = new ConflictProcessorFactory();
          List<SegmentUnitMetadata> segUnitRecoverFailList = new ArrayList<SegmentUnitMetadata>();
          for (SegUnitConflictThrift conflictContext : response.getConflicts()) {
            ConflictProcessor processor = factory
                .createProcessor(conflictContext, segUnitRecoverFailList);
            processor.dealWithConflict();
          }

          if (segUnitRecoverFailList.size() != 0) {
            informationClient.reportSegmentUnitRecycleFail(segUnitRecoverFailList);
          }
        }

        if (response != null && CollectionUtils.isNotEmpty(response.getSegUnitsMetadata())) {
         
          List<SegmentUnitMetadataThrift> segUnitsMetadataFromInfoCenter = response
              .getSegUnitsMetadata();
          for (SegmentUnitMetadataThrift
              segUnitMetadataFromInfoCenter : segUnitsMetadataFromInfoCenter) {
            SegId segId = new SegId(segUnitMetadataFromInfoCenter.getVolumeId(),
                segUnitMetadataFromInfoCenter.getSegIndex());
            SegmentUnit segmentUnit = segmentUnitManager.get(segId);
            segmentUnit.getSegmentUnitMetadata()
                .setMinMigrationSpeed(segUnitMetadataFromInfoCenter.getMinMigrationSpeed());
            segmentUnit.getSegmentUnitMetadata()
                .setMaxMigrationSpeed(segUnitMetadataFromInfoCenter.getMaxMigrationSpeed());
          }
        }

        GlobalDeletingSegmentUnitCollection
            .updateSuccessStatus(segIdDeletingSegmentUnitCollectionMap);

        if (response != null && response.getWhichHaThisVolumeToReportSize() > 0) {
          Map<Long, Set<Long>> whichHaThisVolumeToReportFromResponse = response
              .getWhichHaThisVolumeToReport();
          logger.warn("when report to {}, get the response  whichHAThisVolume table is :{}",
              endPoint == null ? "master" : endPoint, whichHaThisVolumeToReportFromResponse);

          for (Entry<Long, Set<Long>> entry : whichHaThisVolumeToReportFromResponse.entrySet()) {
            long instanceId = entry.getKey();
            Set<Long> volumeIdList = entry.getValue();
           
            if (whichHaThisVolumeToReportMap.containsKey(instanceId)) {
             
              Set<Long> volumes = whichHaThisVolumeToReportMap.get(instanceId);
              volumes.addAll(volumeIdList);

              whichHaThisVolumeToReportMap.put(instanceId, volumes);
            } else {
              logger.info(
                  "after report, the master tell me,"
                      + " about this volumes :{} will report to instance :{}",
                  volumeIdList, instanceId);
              whichHaThisVolumeToReportMap.put(instanceId, volumeIdList);
            }

          }
        }

        if (response != null && response.getVolumeNotToReportCurrentInstanceSize() > 0) {
          Map<Long, Set<Long>> volumeNotToReportCurrentInstance = response
              .getVolumeNotToReportCurrentInstance();
          logger.warn("when report to {}, get the response volumeNotToReportCurrentInstance is :{}",
              endPoint == null ? "master" : endPoint, volumeNotToReportCurrentInstance);

          for (Entry<Long, Set<Long>> entry : volumeNotToReportCurrentInstance.entrySet()) {
            long instanceId = entry.getKey();
            Set<Long> volumeIdList = entry.getValue();

            if (whichHaThisVolumeToReportMap.containsKey(instanceId)) {
             
              Set<Long> volumes = whichHaThisVolumeToReportMap.get(instanceId);
              volumes.removeAll(volumeIdList);
              whichHaThisVolumeToReportMap.put(instanceId, volumes);
              logger.warn(
                  "after remove the not report volumes:{} in stance :{}, the report table :{}",
                  volumeIdList, instanceId, whichHaThisVolumeToReportMap);
            }
          }
        }
      } catch (Exception e) {
        GlobalDeletingSegmentUnitCollection
            .updateFailedStatus(segIdDeletingSegmentUnitCollectionMap);
        logger.warn("caught an exception when reporting segment units", e);
      }

      return true;
    }

  }
}
