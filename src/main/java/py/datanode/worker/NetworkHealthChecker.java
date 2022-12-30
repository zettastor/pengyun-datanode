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

package py.datanode.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.segment.SegmentUnitStatus;
import py.common.PyService;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.service.DataNodeServiceImpl;
import py.instance.Instance;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.netty.core.NetworkDetect;
import py.periodic.Worker;

public class NetworkHealthChecker implements Worker {
  private static final Logger logger = LoggerFactory.getLogger(NetworkHealthChecker.class);

  private final SegmentUnitManager segmentUnitManager;
  private final AppContext myContext;
  private final DataNodeServiceImpl dataNodeService;
  private final long reviveProcessThresholdMs;
  private final InstanceStore instanceStore;
  private boolean paused = false;
  private long lastPauseTime = 0;

  public NetworkHealthChecker(SegmentUnitManager segmentUnitManager, AppContext myContext,
      DataNodeServiceImpl dataNodeService, long reviveProcessThresholdMs,
      InstanceStore instanceStore) {
    this.segmentUnitManager = segmentUnitManager;
    this.myContext = myContext;
    this.dataNodeService = dataNodeService;
    this.reviveProcessThresholdMs = reviveProcessThresholdMs;
    this.instanceStore = instanceStore;
  }

  @Override
  public void doWork() {
    try {
      boolean networkUnhealthy = false;
      if (!NetworkDetect
          .myselfAlive(myContext.getEndPointByServiceName(PortType.IO).getHostName())) {
        logger.error("network IO Flow unhealthy, changing all segment units to Start");
        networkUnhealthy = true;
      }

      if (!NetworkDetect
          .myselfAlive(myContext.getEndPointByServiceName(PortType.CONTROL).getHostName())) {
        logger.error("network Control Flow unhealthy, changing all segment units to Start");
        networkUnhealthy = true;
      }

      if (networkUnhealthy) {
        pauseDataNodeServiceRequestProcessing();
      } else {
        reviveDataNodeServiceRequestProcessing();
      }

    } catch (Throwable t) {
      logger.error("processing error", t);
    }
  }

  private void changeAllSegmentUnitToStartAndPauseSegmentUnit() {
    for (SegmentUnit segmentUnit : segmentUnitManager.get()) {
      try {
        if (!segmentUnit.getSegmentUnitMetadata().getStatus().isFinalStatus()) {
          segmentUnit.setPauseVotingProcess(true);
          segmentUnit.getArchive()
              .asyncUpdateSegmentUnitMetadata(segmentUnit.getSegId(), null,
                  SegmentUnitStatus.Start);
        }
      } catch (Exception e) {
        logger.warn("can't update segment unit to Start {}", segmentUnit);
      }
    }
  }

  private void reviveSegmentUnit() {
    for (SegmentUnit segmentUnit : segmentUnitManager.get()) {
      try {
        if (!segmentUnit.getSegmentUnitMetadata().getStatus().isFinalStatus()) {
          segmentUnit.setPauseVotingProcess(false);
        }
      } catch (Exception e) {
        logger.warn("can't update segment unit to Start {}", segmentUnit);
      }
    }
  }

  private void pauseDataNodeServiceRequestProcessing() throws Exception {
    if (!paused) {
      lastPauseTime = System.currentTimeMillis();
      dataNodeService.pauseRequestProcessing();
      paused = true;
      changeAllSegmentUnitToStartAndPauseSegmentUnit();
    }
  }

  private void reviveDataNodeServiceRequestProcessing() {
    if (paused) {
      long timePassed = System.currentTimeMillis() - lastPauseTime;
      if (timePassed <= reviveProcessThresholdMs) {
        logger.warn(
            "Though network has been up again," 
                + " it has not been long enough since last pause {}/{}, keep everything paused",
            timePassed, reviveProcessThresholdMs);
      } else {
        logger.warn("revive request processing and udp echoing");
        dataNodeService.reviveRequestProcessing();
        paused = false;
        reviveSegmentUnit();
      }
    }
  }
}
