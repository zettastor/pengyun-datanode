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