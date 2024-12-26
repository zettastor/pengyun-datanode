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

import java.util.concurrent.TimeUnit;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitMetadata;
import py.exception.EndPointNotFoundException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.ServiceIsNotAvailableException;
import py.exception.TooManyEndPointFoundException;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;

public class JustCreatedSegUnitsReporter extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(JustCreatedSegUnitsReporter.class);

  private InformationCenterClientFactory informationCenterClientFactory;
  private boolean interrupted = false;

  public JustCreatedSegUnitsReporter(
      InformationCenterClientFactory informationCenterClientFactory) {
    this.informationCenterClientFactory = informationCenterClientFactory;
  }

  public void run() {
    InformationCenterClientWrapper ccClient = null;

    while (!interrupted) {
     
      boolean buildSuccess = false;
      try {
        ccClient = informationCenterClientFactory.build();
        buildSuccess = true;
      } catch (EndPointNotFoundException | TooManyEndPointFoundException 
          | GenericThriftClientFactoryException e) {
        logger
            .warn("Unable to build client of control-center to report just created segment units");
      } catch (Exception e) {
        logger.warn("Caught an exception", e);
      }

      if (!buildSuccess) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
          logger.error("", e1);
        }
        continue;
      }

      SegmentUnitMetadata segUnit = null;
      try {
        segUnit = SegmentUnitMetadata.justCreatedSegUnits.poll(15, TimeUnit.SECONDS);
        if (null == segUnit) {
          logger.info(
              "Elapased 15 seconds, no just created segment" 
                  + " unit needs to report to control-center");
          continue;
        }

        ccClient.reportJustCreatedSegUnit(segUnit);
      } catch (ServiceIsNotAvailableException | TException | InterruptedException e) {
        logger.warn("Caught an exception when report just created segUnit to control-center {}",
            segUnit);
      } catch (Exception e) {
        logger.warn("Caught an exception", e);
        continue;
      }
    }
  }

  public void interrupt() {
    super.interrupt();
    this.interrupted = true;
  }
}
