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
