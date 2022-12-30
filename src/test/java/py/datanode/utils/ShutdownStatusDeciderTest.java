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

package py.datanode.utils;

import static org.junit.Assert.assertEquals;

import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import py.datanode.utils.ShutdownStatusDecider.DataNodeShutdownStatus;
import py.test.TestBase;

public class ShutdownStatusDeciderTest extends TestBase {
  private static File persistenceRoot = new File("/tmp/");
  private File startFile = new File(persistenceRoot,
      ShutdownStatusDecider.DATANODE_START_TIME_FILE_NAME);
  private File endFile = new File(persistenceRoot,
      ShutdownStatusDecider.DATANODE_SHUTDOWN_TIME_FILE_NAME);

  public ShutdownStatusDeciderTest() throws Exception {
    super.init();
  }

  /**
   * there are some situations that we can judge if the host is crash or datanode service is first
   * deployed.
   *
   */
  @Test
  public void testHostCrash() throws Exception {
    ShutdownStatusDecider decider = new ShutdownStatusDecider(persistenceRoot);
   
    assertEquals(DataNodeShutdownStatus.HOST_CRASH, decider.getStatus());
    ShutdownStatusDecider.persistTimeToFile(startFile, 0);

    assertEquals(DataNodeShutdownStatus.HOST_CRASH, decider.getStatus());
  }

  /**
   * there are some situations that we can judge if the JVM of datanode is crashed.
   */
  @Test
  public void testJvmCrash() throws Exception {
    ShutdownStatusDecider decider = new ShutdownStatusDecider(persistenceRoot);
   
    ShutdownStatusDecider.persistTimeToFile(startFile, System.currentTimeMillis());
    assertEquals(decider.getStatus(), DataNodeShutdownStatus.JVM_CRASH);

    ShutdownStatusDecider.persistTimeToFile(endFile, 0);
    assertEquals(decider.getStatus(), DataNodeShutdownStatus.JVM_CRASH);
  }

  /**
   * there is a situation where we can judge if the JVM of datanode is CLEAN colsed.
   */
  @Test
  public void testCleanShutdown() throws Exception {
    final ShutdownStatusDecider decider = new ShutdownStatusDecider(persistenceRoot);
    ShutdownStatusDecider.persistTimeToFile(startFile, System.currentTimeMillis());
    Thread.sleep(1000);
    ShutdownStatusDecider.persistTimeToFile(endFile, System.currentTimeMillis());
    assertEquals(decider.getStatus(), DataNodeShutdownStatus.CLEAN);
  }

  @Before
  public void beforeMethod() {
    deleteAllFile();
  }

  @After
  public void afterMethod() {
    deleteAllFile();
  }

  private void deleteAllFile() {
    if (startFile.exists()) {
      startFile.delete();
    }

    if (endFile.exists()) {
      endFile.delete();
    }
  }

}
