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
