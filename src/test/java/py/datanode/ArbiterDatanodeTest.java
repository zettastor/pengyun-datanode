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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.Validate;
import org.junit.Before;
import org.junit.Test;
import py.app.context.DummyInstanceIdStore;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.connection.pool.udp.NioUdpEchoServer;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.service.DataNodeServiceImpl;
import py.datanode.test.DataNodeConfigurationForTest;
import py.instance.DummyInstanceStore;
import py.instance.InstanceId;
import py.instance.PortType;
import py.test.TestBase;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.CacheTypeThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.share.VolumeSourceThrift;
import py.thrift.share.VolumeTypeThrift;

public class ArbiterDatanodeTest extends TestBase {
  private static AtomicInteger ioPort = new AtomicInteger(55002);
  private String persistentRoot = "/tmp/datanode_app_test/var/storage";
  private AtomicInteger port = new AtomicInteger(8001);
  private DataNodeConfiguration cfg;

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
    cfg.getArchiveConfiguration().setPersistRootDir(persistentRoot);

    createUnsetDir(cfg, persistentRoot);

    File arbiterFile = new File(cfg.getArbiterStoreFile());
    if (arbiterFile.exists()) {
      FileUtils.forceDelete(arbiterFile);
    }
  }

  @Test
  public void testCreateAribter() throws Exception {
    String host = InetAddress.getLocalHost().getHostName();
    DummyInstanceIdStore instanceIdStore = new DummyInstanceIdStore();
    instanceIdStore.setInstanceId(new InstanceId(1111));

    DataNodeContext context = new DataNodeContext("testing");
    context.putEndPoint(PortType.CONTROL, new EndPoint(host, port.incrementAndGet()));
    context.putEndPoint(PortType.IO, new EndPoint(host, ioPort.incrementAndGet()));
    context.setInstanceIdStore(instanceIdStore);
    logger.info("io endpoint: {}", context.getEndPointByServiceName(PortType.IO));

    DataNodeServiceImpl dataNodeService = new DataNodeServiceImpl();
    DataNodeAppEngine engine = new DataNodeAppEngine(dataNodeService);
    engine.setArbiterOnly(true);
    engine.setDataNodeCfg(cfg);
    engine.setContext(context);
    engine.setInstanceStore(new DummyInstanceStore());
    engine.setUdpServer(new NioUdpEchoServer(context.getEndPointByServiceName(PortType.CONTROL)));
    engine.setUdpServer(new NioUdpEchoServer(context.getEndPointByServiceName(PortType.IO)));
    engine.start();

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory 
        = GenericThriftClientFactory
        .create(DataNodeService.Iface.class)
        .withMaxConnectionsPerEndpoint(cfg.getMaxConnectionsPerSyncDataNodeEndPoint())
        .withDefaultSocketTimeout(cfg.getDataNodeRequestTimeoutMs())
        .setMaxNetworkFrameSize(20)
        .withMaxChannelPendingSizeMb(cfg.getMaxChannelPendingSizeMb());
    dataNodeSyncClientFactory.setNeedCache();
    

    SegId segId = new SegId(1, 0);
    CreateSegmentUnitRequest request = new CreateSegmentUnitRequest(RequestIdBuilder.get(),
        segId.getVolumeId().getId(), segId.getIndex(),
        VolumeTypeThrift.SMALL, 11, SegmentUnitTypeThrift.Arbiter, 1,
        false, VolumeSourceThrift.CREATE);

    Set<Long> secondarys = new HashSet<>();
    secondarys.add(11L);
    Set<Long> arbiters = new HashSet<>();
    arbiters.add(instanceIdStore.getInstanceId().getId());
    request.setInitMembership(
        new SegmentMembershipThrift(segId.getVolumeId().getId(), segId.getIndex(), 1, 0, 1,
            secondarys, arbiters));
    dataNodeService.reviveRequestProcessing();
    DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
        .generateSyncClient(context.getEndPoints().get(PortType.CONTROL));
    dataNodeClient.createSegmentUnit(request);

    logger.warn("the list is {}", engine.getDataNodeService().getSegmentUnitManager().get());
    Validate.notNull(engine.getDataNodeService().getSegmentUnitManager().get(segId));
    engine.stop();
  }

  private void createUnsetDir(DataNodeConfiguration config, String persistentRoot)
      throws IOException {
    Path path = FileSystems.getDefault()
        .getPath(persistentRoot, config.getArchiveConfiguration().getUnsettledArchiveDir());
    FileUtils.forceMkdir(path.toFile());
  }

}
