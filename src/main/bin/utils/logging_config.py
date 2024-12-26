# -*- encoding=utf-8 -*-
# Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
"""
config for logging, if need use log, please import this module
"""
import inspect
import logging
import logging.config
import logging.handlers
import os


def get_logger(logger_name=None):
  if logger_name:
    return logging.getLogger(logger_name)
  else:
    stack = inspect.stack()
    file_path = stack[1][1]
    module_name = os.path.splitext(os.path.basename(file_path))[0]
    return logging.getLogger(module_name)


# create log folder
log_folder = os.path.join(os.getcwd(), "logs")
if not os.path.exists(log_folder):
  os.mkdir(log_folder)

logging_config_path = os.path.join(os.getcwd(), "config", "python_logging.conf")

if os.path.exists(logging_config_path):
  # use config file to init log
  logging.config.fileConfig(logging_config_path, disable_existing_loggers=False)

  logger = logging.getLogger(__name__)
  logger.info("Init logger by config:[%s]", logging_config_path)

else:
  # init log directly in code
  formatter = logging.Formatter(
      "%(levelname)s [%(asctime)s] [%(threadName)s] [%(name)s] [%(filename)s:%(funcName)s:%(lineno)s]: %(message)s")

  file_handler = logging.handlers.RotatingFileHandler("logs/python.log", "a",
                                                      100 * 1024 * 1024, 3,
                                                      "utf-8")
  file_handler.setLevel(logging.NOTSET)
  file_handler.setFormatter(formatter)

  root_logger = logging.root
  root_logger.setLevel(logging.DEBUG)
  root_logger.addHandler(file_handler)

  logger = logging.getLogger(__name__)
  logger.info("Init logger by code config.")
