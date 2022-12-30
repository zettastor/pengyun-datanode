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
