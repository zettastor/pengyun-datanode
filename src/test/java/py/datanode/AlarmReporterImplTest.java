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

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import py.datanode.alarm.AlarmReporterImpl;
import py.datanode.statistic.AlarmReportData;
import py.datanode.statistic.AverageValueCollector;
import py.datanode.statistic.MaxValueCollector;
import py.monitor.common.UserDefineName;
import py.test.TestBase;

public class AlarmReporterImplTest extends TestBase {
  private AlarmReporterImpl alarmReporter;

  private BlockingQueue<AlarmReportData> alarms = new LinkedBlockingQueue<>();

  @Before
  public void setUp() {
    setLogLevel(Level.DEBUG);

    alarmReporter = new AlarmReporterImplForTest();
    alarmReporter.start();
  }

  @After
  public void tearDown() {
    alarmReporter.stop();
  }

  @Test
  public void testAverage() {
    AverageValueCollector collector = new AverageValueCollector();

    final int round = 20;
    for (int i = 0; i < round; i++) {
      int count = RandomUtils.nextInt(10000) + 1;
      int sum = 0;

      for (int j = 0; j < count; j++) {
        int num = RandomUtils.nextInt(1000);
        sum += num;
        collector.submit(num);
      }

      Assert.assertEquals(sum / count, collector.average());
    }
  }

  @Test
  public void testMaxValue() {
    final int initialValue = 0;
    MaxValueCollector collector = new MaxValueCollector(initialValue);
    int value = initialValue;

    final int round = 20;
    for (int i = 0; i < round; i++) {
      int count = RandomUtils.nextInt(10000) + 1;
      int maxValue = initialValue;

      for (int j = 0; j < count; j++) {
        int delta = RandomUtils.nextInt(100) - 50;

        value += delta;
        maxValue = Math.max(maxValue, value);

        collector.inc(delta);
      }

      Assert.assertEquals(maxValue, collector.getAndResetMaxValue());
    }
  }

  @Test
  public void ioDelayTest() throws Exception {
    int readDelay = 5555;

    AlarmReportData data = AlarmReportData.generateIoDelayAlarm(readDelay, TimeUnit.MILLISECONDS);
    alarmReporter.submit(data);

    Assert.assertEquals(data, alarms.poll(1, TimeUnit.MINUTES));
  }

  @Test
  public void registerTest() throws InterruptedException {
    final long interval = 2000;
    AtomicLong lengthGen = new AtomicLong(0);
    String queueName = "queueNameRand";
    int expectCount = 3;

    alarmReporter.register(
        () -> AlarmReportData.generateQueueSizeAlarm(lengthGen.incrementAndGet(), queueName),
        interval);

    TimeUnit.MILLISECONDS.sleep(interval * (expectCount + 1));

    LinkedList<AlarmReportData> datas = new LinkedList<>();
    alarms.drainTo(datas);

    Assert.assertTrue(datas.size() >= expectCount);
    for (AlarmReportData data : datas) {
      Assert
          .assertEquals(queueName, data.getUserDefineParams().get(UserDefineName.QueueName.name()));
    }
  }

  private class AlarmReporterImplForTest extends AlarmReporterImpl {
    @Override
    protected void processAlarmData(AlarmReportData data) {
      alarms.offer(data);
    }
  }
}
