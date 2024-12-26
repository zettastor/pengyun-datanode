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

package py.datanode.worker;

import io.netty.util.HashedWheelTimer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.PluginPlugoutManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.periodic.Worker;
import py.periodic.WorkerFactory;
import py.storage.StorageExceptionHandlerChain;

public class CheckStoragesWorkerFactory implements WorkerFactory {
  private static final Logger logger = LoggerFactory.getLogger(CheckStoragesWorkerFactory.class);

  private final PluginPlugoutManager pluginPlugoutManager;
  private final StorageExceptionHandlerChain storageExceptionHandlerChain;
  private final HashedWheelTimer hashedWheelTimer;
  private final Thread errorThread;
  private final Thread inputThread;
  private final Semaphore errorSemaphore = new Semaphore(0);
  private final Semaphore inputSemaphore = new Semaphore(0);

  private volatile CountDownLatch waitLatch;
  private volatile List<String> results;
  private volatile Process currentProcess;
  private Object lock = new Object();
  private DataNodeConfiguration cfg;

  public CheckStoragesWorkerFactory(PluginPlugoutManager pluginPlugoutManager,
      DataNodeConfiguration cfg,
      StorageExceptionHandlerChain storageExceptionHandlerChain,
      HashedWheelTimer hashedWheelTimer) {
    this.storageExceptionHandlerChain = storageExceptionHandlerChain;
    this.pluginPlugoutManager = pluginPlugoutManager;
    this.cfg = cfg;
    this.hashedWheelTimer = hashedWheelTimer;

    this.errorThread = new Thread("error") {
      public void run() {
        while (true) {
          try {
            errorSemaphore.acquire();
          } catch (InterruptedException e) {
            logger.warn("caught an exception", e);
            break;
          }

          InputStream is1 = currentProcess.getErrorStream();
          try {
            BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));
            String line1;
            while ((line1 = br1.readLine()) != null) {
              if (line1 != null) {
                logger.warn("error line={}", line1);
              }
            }
          } catch (IOException e) {
            logger.warn("read from error stream", e);
          } finally {
            try {
              is1.close();
            } catch (IOException e) {
              logger.error("", e);
            }
            waitLatch.countDown();
          }
        }
        logger.trace("exit the error thread={}", Thread.currentThread().getId());
      }
    };

    errorThread.setDaemon(true);
    errorThread.start();

    inputThread = new Thread("input") {
      public void run() {
        while (true) {
          try {
            inputSemaphore.acquire();
          } catch (InterruptedException e) {
            logger.warn("caught an exception", e);
            break;
          }

          InputStream is2 = currentProcess.getInputStream();
          BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
          try {
            String line1;
            while ((line1 = br2.readLine()) != null) {
              if (line1 != null) {
                logger.warn("input line={}", line1);
                results.add(line1);
              }
            }
          } catch (IOException e) {
            logger.warn("read from error stream", e);
          } finally {
            try {
              is2.close();
            } catch (IOException e) {
              logger.error("", e);
            }
            waitLatch.countDown();
          }
        }
        logger.trace("exit the read thread={}", Thread.currentThread().getId());
      }
    };

    inputThread.setDaemon(true);
    inputThread.start();
  }

  @Override
  public Worker createWorker() {
    return new CheckStoragesWorker(pluginPlugoutManager, new ProcessWaiter(), cfg,
        storageExceptionHandlerChain, hashedWheelTimer);
  }

  public class ProcessWaiter {
    public List<String> wait(Process process) {
      synchronized (lock) {
        results = new ArrayList<>();
        waitLatch = new CountDownLatch(2);
        currentProcess = process;
        errorSemaphore.release();
        inputSemaphore.release();
        try {
          waitLatch.await();
        } catch (InterruptedException e) {
          logger.warn("caught an exception", e);
        }
        return results;
      }
    }
  }
}
