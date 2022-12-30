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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Test;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.worker.CheckLogSpaceWorker;
import py.periodic.UnableToStartException;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.periodic.impl.ExecutionOptionsReader;
import py.periodic.impl.PeriodicWorkExecutorImpl;
import py.test.TestBase;

public class CheckLogSpaceTest extends TestBase {
  private static final long DEFAULT_TOTOL_SPACE = 2L * 1024 * 1024 * 1024;
  private static final long DEFAULT_FREE_SPACE = 1024L * 1024 * 1024;
  private final AtomicBoolean exit = new AtomicBoolean(false);

  @Before
  public void beforeMethod() {
    exit.set(false);
  }

  @Test
  public void testEnoughLogSpace() throws Exception {
    DataNodeConfiguration configuration = new DataNodeConfiguration();
    configuration.setMinReservedDiskSizeMb(200);
    runCheckLogSpaceWorker(configuration);
    assertTrue(!exit.get());
  }

  @Test
  public void testNotEnoughLogSpace() throws Exception {
    DataNodeConfiguration configuration = new DataNodeConfiguration();
    configuration.setMinReservedDiskSizeMb(2000);
    runCheckLogSpaceWorker(configuration);
    assertTrue(exit.get());
  }

  private File mockFile() {
    File file = mock(File.class);
    when(file.isDirectory()).thenReturn(true);
    when(file.exists()).thenReturn(true);
    when(file.getTotalSpace()).thenReturn(DEFAULT_TOTOL_SPACE);
    when(file.getFreeSpace()).thenReturn(DEFAULT_FREE_SPACE);
    when(file.getUsableSpace()).thenReturn(DEFAULT_FREE_SPACE);
    return file;
  }

  public void runCheckLogSpaceWorker(DataNodeConfiguration configuration) throws Exception {
    File[] dirLogs = new File[3];
    dirLogs[0] = mockFile();
    dirLogs[1] = mockFile();
    dirLogs[2] = mockFile();
    CountDownLatch latch = new CountDownLatch(1);

    ExecutionOptionsReader optionReader = new ExecutionOptionsReader(1, 1, 1000, null);
    PeriodicWorkExecutorImpl checkLogSpaceExecutor = new PeriodicWorkExecutorImpl(optionReader,
        new WorkerFactory() {
          @Override
          public Worker createWorker() {
            return new CheckLogSpaceWorker(configuration.getMinReservedDiskSizeMb(), dirLogs) {
              @Override
              public void exit() {
                logger.warn("exit+++++++");
                exit.set(true);
                latch.countDown();
              }

              @Override
              public void doWork() throws Exception {
                super.doWork();
                latch.countDown();
              }
            };
          }
        }, "logspace-checker");
    try {
      checkLogSpaceExecutor.start();
    } catch (UnableToStartException e) {
      e.printStackTrace();
    }

    assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
    checkLogSpaceExecutor.stop();
  }
}
