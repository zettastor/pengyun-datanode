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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import py.instance.Group;

public class ArbiterLauncher extends py.app.Launcher {
  private static final Logger logger = LoggerFactory.getLogger(ArbiterLauncher.class);
  private Group groupInfo = null;

  public ArbiterLauncher(String beansHolder, String serviceRunningPath) {
    super(beansHolder, serviceRunningPath);
  }

  public ArbiterLauncher(String beansHolder, String serviceRunningPath, String groupId) {
    super(beansHolder, serviceRunningPath);
    this.groupInfo = (groupId == null ? null : new Group(Integer.valueOf(groupId)));
  }

  public static void main(String[] args) {
    logger.warn("now we get start command pram:{}, length:{}", args, args.length);
   
    if (args.length < 1 || args.length > 2) {
      String usage = String
          .format("Usage: \n\t%s beans-holder service-running-path", Launcher.class.getName());
      System.out.println(usage);
      System.exit(1);
    }

    String groupInfo = null;
    if (args.length > 1) {
      groupInfo = args[1];
    }

    ArbiterLauncher launcher = new ArbiterLauncher(DataNodeAppConfig.class.getName() + ".class",
        args[0],
        groupInfo);
    launcher.launch();
  }

  @Override
  public void startAppEngine(ApplicationContext appContext) {
    try {
      DataNodeAppEngine engine = appContext.getBean(DataNodeAppEngine.class);
      logger.warn("Going to start data node app engine");
      if (groupInfo != null) {
        logger.warn("start data node with:{}", groupInfo);
        engine.getContext().setGroupInfo(groupInfo);
      }
      engine.setArbiterOnly(true);
      engine.start();
      startMonitorAgent(appContext);
    } catch (Throwable t) {
      logger.error("Caught an exception when start node service {}", appContext, t);
      System.exit(0);
    }
  }

}