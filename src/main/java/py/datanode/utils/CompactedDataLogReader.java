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
