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

import org.apache.commons.lang3.Validate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.app.NetworkConfiguration;
import py.app.context.DummyInstanceIdStore;
import py.app.context.InstanceDomainFileStore;
import py.app.context.InstanceIdFileStore;
import py.app.context.InstanceIdStore;
import py.app.healthcheck.DihClientBuilder;
import py.app.healthcheck.DihClientBuilderImpl;
import py.app.healthcheck.HealthChecker;
import py.app.healthcheck.HealthCheckerWithThriftImpl;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.connection.pool.udp.NativeUdpEchoServer;
import py.connection.pool.udp.NioUdpEchoServer;
import py.connection.pool.udp.UdpServer;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.configuration.RocksDbPathConfig;
import py.datanode.service.DataNodeServiceImpl;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.dih.client.worker.DihClientBuildWorkerFactory;
import py.dih.client.worker.HeartBeatWorkerFactory;
import py.icshare.GroupFileStore;
import py.icshare.InstanceDomainFileStoreImpl;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.monitor.jmx.configuration.JmxAgentConfiguration;
import py.monitor.jmx.server.JmxAgent;

@Configuration
@PropertySource(value = {"classpath:config/datanode.properties",
    "classpath:config/${datanode.info.placeholder:datanode_info}.properties"})
@Import({DataNodeConfiguration.class, LogPersistingConfiguration.class, NetworkConfiguration.class,
    JmxAgentConfiguration.class, LogPersistRocksDbConfiguration.class})
public class DataNodeAppConfig {
  @Value("${app.name:DataNode}")
  private String appName = "DataNode";

  @Value("${app.location:PY}")
  private String appLocation = "PY";

  @Value("${app.main.endpoint:10011}")
  private String mainEndPoint = "10011";

  @Value("${app.heartbeat.endpoint:20011}")
  private String heartbeatEndPoint = "20011";

  @Value("${app.io.endpoint:30011}")
  private String ioEndPoint = "30011";

  @Value("${dih.endpoint:10000}")
  private String dihEndPoint = "10000";

  @Value("${thrift.client.timeout:20000}")
  private int thriftClientTimeout = 20000;

  @Value("${health.checker.rate:1000}")
  private int healthCheckerRate = 1000;

  @Value("${testing:false}")
  private boolean testing = false;

  @Value("${instance.id.for.testing:10011}")
  private int instanceIdForTesting = 10011;

  /**
   * set the max frame size of server, you should be adjust the value with responding client.
   */
  @Value("${max.network.frame.size:16000000}")
  private int maxNetworkFrameSize = 16 * 1000 * 1000;
  @Autowired
  private DataNodeConfiguration dataNodeConfiguration;
  @Autowired
  private LogPersistingConfiguration logPersistingConfiguration;
  @Autowired
  private LogPersistRocksDbConfiguration logPersistRocksDbConfiguration;
  @Autowired
  private NetworkConfiguration networkConfiguration;
  @Autowired
  private JmxAgentConfiguration jmxAgentConfiguration;

  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  public int getMaxNetworkFrameSize() {
    return maxNetworkFrameSize;
  }

  public void setMaxNetworkFrameSize(int maxNetworkFrameSize) {
    this.maxNetworkFrameSize = maxNetworkFrameSize;
  }

  @Bean
  public DataNodeAppEngine dataNodeAppEngine() throws Exception {

    DataNodeAppEngine dataNodeAppEngine = new DataNodeAppEngine(dataNodeService());
    dataNodeAppEngine.setContext(appContext());
    RocksDbPathConfig rocksDbPathConfig = new RocksDbPathConfig(dataNodeConfiguration,
        logPersistRocksDbConfiguration);
    dataNodeConfiguration.setRocksDbPathConfig(rocksDbPathConfig);
    logPersistRocksDbConfiguration.setRocksDbPathConfig(rocksDbPathConfig);
    dataNodeAppEngine.setDataNodeCfg(dataNodeConfiguration);
    dataNodeAppEngine.setDataNodeLogPersistingCfg(logPersistingConfiguration);
    dataNodeAppEngine.setDataNodeLogPersistingRocksDbCfg(logPersistRocksDbConfiguration);
    dataNodeAppEngine.setDihClientFactory(dihClientFactory());
    dataNodeAppEngine.setHealthChecker(healthChecker());
    dataNodeAppEngine.setDihClientBuilder(dihClientBuilder());
    dataNodeAppEngine.setInstanceStore(instanceStore());
    dataNodeAppEngine.setUdpServer(udpEchoServer());

    Validate.isTrue(maxNetworkFrameSize > 0);
    dataNodeAppEngine.setMaxNetworkFrameSize(maxNetworkFrameSize);
    dataNodeAppEngine.setJmxAgent(jmxAgent());
    return dataNodeAppEngine;
  }

  @Bean
  public JmxAgent jmxAgent() throws Exception {
    JmxAgent jmxAgent = JmxAgent.getInstance();
    jmxAgent.setAppContext(appContext());
    return jmxAgent;
  }

  private DataNodeServiceImpl dataNodeService() {
    return new DataNodeServiceImpl();
  }

  @Bean
  public InstanceIdStore instanceIdStore() {
    if (!testing) {
      return new InstanceIdFileStore(
          dataNodeConfiguration.getArchiveConfiguration().getPersistRootDir(), appName,
          mainEndPoint().getPort());
    } else {
      DummyInstanceIdStore store = new DummyInstanceIdStore();
      store.setInstanceId(new InstanceId(instanceIdForTesting));
      return store;
    }
  }

  @Bean
  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory(1);
    return dihClientFactory;
  }

  @Bean
  public InstanceStore instanceStore() throws Exception {
    DihInstanceStore instanceStore = DihInstanceStore.getSingleton();
    instanceStore.setDihClientFactory(dihClientFactory());
    instanceStore.setDihEndPoint(localDihEp());
    instanceStore.init();
    return instanceStore;
  }

  @Bean
  public EndPoint ioEndPoint() {
    if (networkConfiguration.isEnableDataDepartFromControl()) {
      return EndPointParser.parseInSubnet(ioEndPoint, networkConfiguration.getDataFlowSubnet());
    } else {
      return EndPointParser.parseInSubnet(ioEndPoint, networkConfiguration.getControlFlowSubnet());
    }
  }

  @Bean
  public EndPoint localDihEp() {
    return EndPointParser.parseInSubnet(dihEndPoint, networkConfiguration.getControlFlowSubnet());
  }

  @Bean
  public EndPoint mainEndPoint() {
    return EndPointParser.parseInSubnet(mainEndPoint, networkConfiguration.getControlFlowSubnet());
  }

  @Bean
  public EndPoint heartbeatEndPoint() {
    return EndPointParser
        .parseInSubnet(heartbeatEndPoint, networkConfiguration.getControlFlowSubnet());
  }

  @Bean
  public EndPoint monitorEndPoint() {
    return EndPointParser
        .parseInSubnet(jmxAgentConfiguration.getJmxAgentPort(),
            networkConfiguration.getControlFlowSubnet());
  }

  @Bean
  public DataNodeContext appContext() {
    DataNodeContext appContext = new DataNodeContext(appName);
   
    appContext.putEndPoint(PortType.CONTROL, mainEndPoint());
    appContext.putEndPoint(PortType.IO, ioEndPoint());

    appContext.putEndPoint(PortType.HEARTBEAT, heartbeatEndPoint());

    appContext.putEndPoint(PortType.MONITOR, monitorEndPoint());

    appContext.setLocation(appLocation);
    appContext.setInstanceIdStore(instanceIdStore());
    appContext
        .setPersistenceRoot(dataNodeConfiguration.getArchiveConfiguration().getPersistRootDir());
    appContext.setGroupStore(groupFileStore());
    appContext.setDomainFileStore(domainFileStore());
    return appContext;
  }

  @Bean
  public HealthChecker healthChecker() {
    HealthCheckerWithThriftImpl healthChecker = new HealthCheckerWithThriftImpl(appContext());
    healthChecker.setCheckingRate(healthCheckerRate);
    healthChecker.setServiceClientClazz(py.thrift.datanode.service.DataNodeService.Iface.class);
    healthChecker.setHeartBeatWorkerFactory(heartBeatWorkerFactory());
    return healthChecker;
  }

  @Bean
  public HeartBeatWorkerFactory heartBeatWorkerFactory() {
    HeartBeatWorkerFactory factory = new HeartBeatWorkerFactory();
    factory.setRequestTimeout(thriftClientTimeout);
    factory.setAppContext(appContext());
    factory.setDihClientFactory(dihClientFactory());
    factory.setLocalDihEndPoint(localDihEp());
    return factory;
  }

  @Bean
  public DihClientBuildWorkerFactory dihClientBuildWorkerFactory() {
    DihClientBuildWorkerFactory dihClientBuildWorkerFactory = new DihClientBuildWorkerFactory();
    dihClientBuildWorkerFactory.setDihClientFactory(dihClientFactory());
    dihClientBuildWorkerFactory.setLocalDihEndPoint(localDihEp());
    dihClientBuildWorkerFactory.setRequestTimeout(thriftClientTimeout);
    return dihClientBuildWorkerFactory;
  }

  @Bean
  public DihClientBuilder dihClientBuilder() {
    DihClientBuilderImpl dihClientBuilder = new DihClientBuilderImpl();
    dihClientBuilder.setDihClientBuildWorkerFactory(dihClientBuildWorkerFactory());
    return dihClientBuilder;
  }

  @Bean
  public GroupFileStore groupFileStore() {
    GroupFileStore groupFileStore = new GroupFileStore(
        dataNodeConfiguration.getArchiveConfiguration().getPersistRootDir(), appName);
    return groupFileStore;
  }

  @Bean
  public InstanceDomainFileStore domainFileStore() {
    InstanceDomainFileStoreImpl domainFileStore = new InstanceDomainFileStoreImpl(
        dataNodeConfiguration.getArchiveConfiguration().getPersistRootDir(), appName);
    return domainFileStore;
  }

  @Bean
  public UdpServer udpEchoServer() {
    int port = dataNodeConfiguration.getNetworkConnectionDetectServerListeningPort();
    String ip = appContext().getEndPointByServiceName(PortType.IO).getHostName();
    EndPoint endPoint = new EndPoint(ip, port);
    if (dataNodeConfiguration.isNetworkConnectionDetectServerUsingNativeOrNot()) {
      return new NativeUdpEchoServer(endPoint);
    } else {
      return new NioUdpEchoServer(endPoint);
    }
  }
}
