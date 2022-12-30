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

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import py.app.context.AppContext;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.PluginPlugoutManager;
import py.client.thrift.GenericThriftClientFactory;
import py.datanode.alarm.AlarmReporterImpl;
import py.datanode.archive.RawArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnitCanDeletingCheck;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.statistic.AlarmReportData;
import py.exception.EndPointNotFoundException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.TooManyEndPointFoundException;
import py.icshare.BackupDbReporter;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.InstanceStore;
import py.monitor.common.UserDefineName;
import py.storage.StorageExceptionHandlerChain;
import py.test.TestBase;
import py.thrift.datanode.service.DataNodeService;

public class AlarmSenderTest extends TestBase {
  private InformationReporter informationReporter;

  private AlarmReporterImpl alarmReporter;
  private BlockingQueue<AlarmReportData> alarms = new LinkedBlockingQueue<>();

  @Before
  public void setUp() throws TooManyEndPointFoundException, TException, EndPointNotFoundException,
      GenericThriftClientFactoryException {
    setLogLevel(Level.DEBUG);

    alarmReporter = new AlarmReporterImplForTest();
    alarmReporter.start();

    informationReporter = new InformationReporterForTest(Mockito.mock(SegmentUnitManager.class),
        Mockito.mock(RawArchiveManager.class),
        Mockito.mock(InformationCenterClientFactory.class), Mockito.mock(AppContext.class),
        Mockito.mock(DataNodeConfiguration.class), Mockito.mock(DataNodeAppEngine.class),
        Mockito.mock(BackupDbReporter.class), Mockito.mock(InstanceStore.class),
        Mockito.mock(StorageExceptionHandlerChain.class), null, null,
        Mockito.mock(SegmentUnitCanDeletingCheck.class));
    informationReporter.setAlarmReporter(alarmReporter);
  }

  @After
  public void tearDown() {
    alarmReporter.stop();
  }

  /**
   * submit status change alarm direct to statistic reporter.
   *
   */
  @Test
  public void testSubmitArchiveStatusChangeAlarm() throws InterruptedException {
    long archiveId = RandomUtils.nextLong();
    ArchiveStatus newStatus = ArchiveStatus.GOOD;

    Archive archive = createSimpleArchive(archiveId, newStatus);

    ArchiveStatus oldStatus = ArchiveStatus.OFFLINING;

    AlarmReportData data = AlarmReportData
        .generateArchiveStatusAlarm(archive, oldStatus, newStatus);
    alarmReporter.submit(data);

    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);
  }

  /**
   * submit status change alarm form information reporter.
   */
  @Test
  public void testReportFromInfoReporter() throws InterruptedException {
    long archiveId;
    ArchiveStatus oldStatus;
    ArchiveStatus newStatus;

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.OFFLINING;
    newStatus = ArchiveStatus.GOOD;
    Archive archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeGood(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.OFFLINING;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeOfflining(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.OFFLINED;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeOfflined(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.EJECTED;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeEjected(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.CONFIG_MISMATCH;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeConfigMismatch(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.DEGRADED;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeDegrade(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.INPROPERLY_EJECTED;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeInProperlyEjected(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.BROKEN;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeBroken(archive, oldStatus);
    checkAlarmResult(Objects.requireNonNull(alarms.poll(1, TimeUnit.MINUTES)), archiveId, oldStatus,
        newStatus);
  }

  /**
   * submit status change alarm but actually archive status is not changed, old status is<br/> the
   * same as new status.
   */
  @Test
  public void testStatusNotChange() throws InterruptedException {
    long archiveId;
    ArchiveStatus oldStatus;
    ArchiveStatus newStatus;

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = oldStatus;
    Archive archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeGood(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.OFFLINING;
    newStatus = oldStatus;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeOfflining(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.OFFLINED;
    newStatus = oldStatus;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeOfflined(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.EJECTED;
    newStatus = oldStatus;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeEjected(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.CONFIG_MISMATCH;
    newStatus = oldStatus;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeConfigMismatch(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.DEGRADED;
    newStatus = oldStatus;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeDegrade(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.INPROPERLY_EJECTED;
    newStatus = oldStatus;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeInProperlyEjected(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.BROKEN;
    newStatus = oldStatus;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeBroken(archive, oldStatus);

    Assert.assertNull(alarms.poll(10, TimeUnit.SECONDS));
  }

  @Test
  public void test() throws InterruptedException {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    TestFuture testFuture = new TestFuture();
    CompletableFuture<Void> f2 = CompletableFuture.runAsync(testFuture)
        .thenRun(() -> countDownLatch.countDown()).exceptionally(throwable -> {
          countDownLatch.countDown();
          return null;
        });

    Boolean wait = countDownLatch.await(5, TimeUnit.SECONDS);
    Validate.isTrue(wait);
  }

  /**
   * submit archive status change, but status in archive mismatch with the interface name<br/> e.g.
   * becomingGood, but archive status is offlined
   */
  @Test
  public void testStatusInArchiveMismatch() throws InterruptedException {
    long archiveId;
    ArchiveStatus oldStatus;
    ArchiveStatus newStatus;

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.OFFLINING;
    newStatus = ArchiveStatus.OFFLINED;
    Archive archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeGood(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.BROKEN;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeOfflining(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.OFFLINING;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeOfflined(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.DEGRADED;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeEjected(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.BROKEN;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeConfigMismatch(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.EJECTED;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeDegrade(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.OFFLINING;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeInProperlyEjected(archive, oldStatus);

    archiveId = RandomUtils.nextLong();
    oldStatus = ArchiveStatus.GOOD;
    newStatus = ArchiveStatus.OFFLINED;
    archive = createSimpleArchive(archiveId, newStatus);
    informationReporter.becomeBroken(archive, oldStatus);
    Assert.assertNull(alarms.poll(10, TimeUnit.SECONDS));
  }

  private void checkAlarmResult(AlarmReportData data, long archiveId, ArchiveStatus oldStatus,
      ArchiveStatus newStatus) {
    Assert.assertEquals(data.getUserDefineParams().get(UserDefineName.ArchiveID.name()),
        String.valueOf(archiveId));
    Assert.assertEquals(data.getUserDefineParams().get(UserDefineName.ArchiveOldStatus.name()),
        oldStatus.name());
    Assert.assertEquals(data.getUserDefineParams().get(UserDefineName.ArchiveNewStatus.name()),
        newStatus.name());
  }

  private Archive createSimpleArchive(long archiveId, ArchiveStatus status) {
    ArchiveMetadata archiveMetadata = new ArchiveMetadata();
    Archive archive = Mockito.mock(Archive.class);
    Mockito.when(archive.getArchiveMetadata()).thenReturn(archiveMetadata);

    archiveMetadata.setArchiveId(archiveId);
    archiveMetadata.setStatus(status);
    archiveMetadata.setDeviceName("sda");
    return archive;
  }

  private static class InformationReporterForTest extends InformationReporter {
    public InformationReporterForTest(SegmentUnitManager segmentUnitManager,
        RawArchiveManager archiveManager,
        InformationCenterClientFactory informationCenterClientFactory,
        AppContext appContext, DataNodeConfiguration dataNodeCfg, DataNodeAppEngine engine,
        BackupDbReporter backupDbReporter, InstanceStore instanceStore,
        StorageExceptionHandlerChain storageExceptionHandlerChain,
        GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory,
        PluginPlugoutManager pluginPlugoutManager,
        SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck)
        throws EndPointNotFoundException, TException, GenericThriftClientFactoryException,
        TooManyEndPointFoundException {
      super(segmentUnitManager, archiveManager, informationCenterClientFactory,
          appContext,
          dataNodeCfg, engine, segmentUnitCanDeletingCheck, backupDbReporter, instanceStore,
          storageExceptionHandlerChain,
          dataNodeSyncClientFactory, pluginPlugoutManager, null, null);
    }

    @Override
    protected void reportArchiveToInfoCenter() {
    }
  }

  public class TestFuture implements Runnable {
    @Override
    public void run() {
      throw new IllegalArgumentException();
    }
  }

  private class AlarmReporterImplForTest extends AlarmReporterImpl {
    @Override
    protected void processAlarmData(AlarmReportData data) {
      alarms.offer(data);
    }
  }
}
