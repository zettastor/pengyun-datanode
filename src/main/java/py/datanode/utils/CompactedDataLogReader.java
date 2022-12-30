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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryReader;
import py.datanode.segment.datalog.MutationLogEntryReaderCompactImplForPersistedLogs;
import py.datanode.segment.datalog.MutationLogEntryWriter;
import py.datanode.segment.datalog.MutationLogEntryWriterJsonImpl;

public class CompactedDataLogReader {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("usage: CompactedDataLogReader filePathToDataLogFile");
      System.exit(1);
    }

    MutationLogEntryReader reader = null;
    MutationLogEntryWriter writer = null;

    try {
      Path filePath = FileSystems.getDefault().getPath(args[0]);
      File logFile = filePath.toFile();
      FileInputStream is = new FileInputStream(logFile);

      reader = new MutationLogEntryReaderCompactImplForPersistedLogs();
      reader.open(is);

      writer = new MutationLogEntryWriterJsonImpl();
      writer.open(System.out);

      while (true) {
        MutationLogEntry log = reader.read();
        if (log != null) {
          writer.write(log);
        } else {
          break;
        }
      }

    } catch (Exception e) {
      System.err.println("caught an exception");
      e.printStackTrace();
    } finally {
      try {
        if (reader != null) {
          reader.close();
        }

        if (writer != null) {
          writer.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

}
