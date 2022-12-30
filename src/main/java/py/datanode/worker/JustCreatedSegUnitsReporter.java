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
