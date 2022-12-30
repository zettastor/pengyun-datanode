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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyListener;
import org.junit.Test;
import py.archive.ArchiveType;
import py.datanode.configuration.DataNodeConfiguration;
import py.test.TestBase;

public class CheckStorageWorkerTest extends TestBase {
  @Test
  public void match() throws Exception {
    DataNodeConfiguration cfg = new DataNodeConfiguration();
    String rawName = "raw1";
    String diskName = ArchiveType.RAW_DISK.name();
    String before = String
        .format(cfg.getArchiveConfiguration().getArchivePluginMatcher(), rawName, diskName);
    logger.warn("before:{}", before);

    String after = cfg.getArchiveConfiguration().getArchivePluginMatcher().replace("%s", "(\\w+)");
    Pattern pattern = Pattern.compile(after);
    Matcher matcher = pattern.matcher(before);
    boolean result = matcher.matches();
    logger.warn("results: {}, {}, {}", result, matcher.group(), matcher.groupCount());
    logger.warn("group: {}, {}, {}", matcher.group(0), matcher.group(1), matcher.group(2));
    assertEquals(rawName, matcher.group(1));
    assertEquals(diskName, matcher.group(2));

    Matcher matcher1 = pattern.matcher(before);
    boolean result1 = matcher1.matches();
    logger.warn("results: {}, {}, {}", result1, matcher1.group(), matcher1.groupCount());
    logger.warn("group: {}, {}, {}", matcher1.group(0), matcher1.group(1), matcher1.group(2));
    assertEquals(rawName, matcher1.group(1));
    assertEquals(diskName, matcher1.group(2));
  }

  @Test
  public void match1() throws Exception {
    DataNodeConfiguration cfg = new DataNodeConfiguration();
    String line = "plugout devName raw1, serialNumber 0QEMU_QEMU_HARDDISK_drive-scsi1-0-0-1";
    String after = cfg.getArchiveConfiguration().getArchivePlugoutMatcher().replace("%s", "(.+)");
    logger.warn("after={}", after);
    Pattern pattern = Pattern.compile(after);
    Matcher matcher = pattern.matcher(line);
    boolean result = matcher.matches();
    logger.warn("results: {}, {}, {}", result, matcher.group(), matcher.groupCount());
    logger.warn("group: {}, {}, {}", matcher.group(0), matcher.group(1), matcher.group(2));
  }

  @Test
  public void checkPath() throws Exception {
    String currentDir = System.getProperty("user.dir");
    logger.warn("{}", currentDir);
    String sub = "bin/wts";

    File file1 = new File(currentDir, sub);
    logger.warn("file1={}", file1.getAbsolutePath());
    File file2 = new File(currentDir + "/", sub);
    logger.warn("file2={}", file2.getAbsolutePath());
    File file3 = new File(currentDir, "/" + sub);
    logger.warn("file3={}", file3.getAbsolutePath());
    File file4 = new File(currentDir + "/", "/" + sub);
    logger.warn("file4={}", file4.getAbsolutePath());
    assertEquals(file1.getAbsolutePath(), file2.getAbsolutePath());
    assertEquals(file1.getAbsolutePath(), file3.getAbsolutePath());
    assertEquals(file1.getAbsolutePath(), file4.getAbsolutePath());
  }

  public void notifyDeleted() throws Exception {
    String path = "/tmp/test-" + System.currentTimeMillis();
    logger.warn("java library path={}", System.getProperty("java.library.path"));
    File file = new File(path);
    file.createNewFile();
    AtomicBoolean deleted = new AtomicBoolean(false);

    int watchId = JNotify.addWatch(path, JNotify.FILE_DELETED, false, new JNotifyListener() {
      @Override
      public void fileCreated(int i, String s, String s1) {
      }

      @Override
      public void fileDeleted(int i, String s, String s1) {
        logger.warn("delete file, {}, {}, {}", i, s, s1);
        deleted.set(true);
      }

      @Override
      public void fileModified(int i, String s, String s1) {
      }

      @Override
      public void fileRenamed(int i, String s, String s1, String s2) {
      }
    });

    file.delete();
    Thread.sleep(2000);
    JNotify.removeWatch(watchId);
    assertTrue(deleted.get());
  }
}
