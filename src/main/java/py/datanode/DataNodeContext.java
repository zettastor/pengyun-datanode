/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.datanode;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import py.app.context.AppContextImpl;
import py.common.struct.EndPoint;
import py.instance.PortType;

/**
 * Store all context information initialized from cmd line or from configuration file.
 *
 *
 */
public class DataNodeContext extends AppContextImpl {
  private String persistenceRoot;

  public DataNodeContext(String name) {
    super(name);
  }

  public String getPersistenceRoot() {
    return persistenceRoot;
  }

  public void setPersistenceRoot(String persistenceRoot) {
    this.persistenceRoot = persistenceRoot;
  }

  @Override
  public EndPoint getHeartbeatMyselfEndPoint() {
    EndPoint endPoint = getEndPointByServiceName(PortType.HEARTBEAT);
    if (endPoint == null) {
      return getMainEndPoint();
    }

    return endPoint;
  }

  @Override
  public Map<PortType, EndPoint> getEndPointsThrift() {
    Map<PortType, EndPoint> endPointsToListenTo = new HashMap<>();
    for (Entry<PortType, EndPoint> endPoint : endPoints.entrySet()) {
      if (endPoint.getKey() != PortType.IO && endPoint.getKey() != PortType.MONITOR) {
        endPointsToListenTo.put(endPoint.getKey(), endPoint.getValue());
      }
    }
    return endPointsToListenTo;
  }

  @Override
  public String toString() {
    return "DataNodeContext [super=" + super.toString() + ", persistenceRoot=" + persistenceRoot
        + "]";
  }
}
