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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import org.apache.log4j.Logger;

public class DiskFinderTest {
  private static final Logger logger = Logger.getLogger(DiskFinderTest.class);
  private static final String ECLIPSE_SCRIPT_PATH =
      System.getProperty("user.dir") + "/src/main/bin/FindRawDisk.pl";
  private static final String TERMINAL_SCRIPT_PATH =
      System.getProperty("user.dir") + "/bin/FindRawDisk.pl";
  private static final String FOUND_SIGNAL = " is found.";
  private static final String NOT_FOUND_SIGNAL = " is not found.";

  public String rawToFind;
  public String scriptPath;

  public DiskFinderTest(String rawToFind) {
    this.rawToFind = rawToFind;
    File eclipseDir = new File(ECLIPSE_SCRIPT_PATH);
    if (eclipseDir.exists()) {
      scriptPath = ECLIPSE_SCRIPT_PATH;
    } else {
      scriptPath = TERMINAL_SCRIPT_PATH;
    }
  }

  public static void main(String[] args) {
    DiskFinderTest diskFinder = new DiskFinderTest("raw10");
    if (diskFinder.find()) {
      System.out.println("Found");
    } else {
      System.out.println("Not Found");
    }
  }

  public boolean find() {
    try {
      String cmd = "perl " + scriptPath + " " + rawToFind;
      Process pid = Runtime.getRuntime().exec(cmd);
      BufferedReader reader = null;
      if (pid != null) {
        reader = new BufferedReader(new InputStreamReader(pid.getInputStream()));
        pid.waitFor();
      } else {
        logger.error("Could not run " + cmd);
      }

      String line = null;
      while (reader != null && (line = reader.readLine()) != null) {
        if (line.contains(rawToFind + NOT_FOUND_SIGNAL)) {
          return false;
        } else if (line.contains(rawToFind + FOUND_SIGNAL)) {
          return true;
        }
      }
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      System.out.println("Caught an exception");
    }

    return true;
  }
}
