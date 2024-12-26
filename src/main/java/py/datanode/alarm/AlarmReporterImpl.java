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
