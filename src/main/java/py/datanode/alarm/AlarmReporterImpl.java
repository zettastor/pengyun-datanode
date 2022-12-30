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

package py.datanode.alarm;

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.statistic.AlarmReportData;
import py.datanode.statistic.AlarmReporter;
import py.querylog.eventdatautil.EventDataWorker;

public class AlarmReporterImpl implements AlarmReporter {
  private static final Logger logger = LoggerFactory.getLogger(AlarmReporterImpl.class);
  private static final int THREAD_STOP_MAX_WAIT_SECOND = 20;

  private final ScheduledExecutorService processService = new ScheduledThreadPoolExecutor(1,
      new DefaultThreadFactory("alarm-reporter"));

  @Override
  public void submit(AlarmReportData data) {
    try {
      processService.execute(() -> processAlarmData(data));
    } catch (RejectedExecutionException e) {
      logger.error("catch exception when submit alarm:[{}]", data, e);
    }
  }

  @Override
  public void register(Supplier<AlarmReportData> alarmSupplier, long reportRateInMs) {
    processService.scheduleWithFixedDelay(() -> {
      try {
        processAlarmData(alarmSupplier.get());
      } catch (Throwable e) {
        logger.error("catch exception", e);
      }
    }, reportRateInMs, reportRateInMs, TimeUnit.MILLISECONDS);
  }

  protected void processAlarmData(AlarmReportData data) {
    try {
      EventDataWorker eventDataWorker = new EventDataWorker(data.getPyService(),
          data.getUserDefineParams());
      eventDataWorker.work(data.getOperationName().name(), data.getCounters());
    } catch (Throwable e) {
      logger.error("catch Throwable when process data:[{}]", data, e);
    }
  }

  public void start() {
   
  }

  public void stop() {
    processService.shutdown();
    try {
      processService.awaitTermination(THREAD_STOP_MAX_WAIT_SECOND, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("catch exception", e);
      processService.shutdownNow();
    }
  }
}
