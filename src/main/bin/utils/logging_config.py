# -*- encoding=utf-8 -*-
"""
config for logging, if need use log, please import this module
"""

#  Copyright (c) 2022. PengYunNetWork
#
#  This program is free software: you can use, redistribute, and/or modify it
#  under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
#  as published by the Free Software Foundation.
#
#  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
#   without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
#   You should have received a copy of the GNU Affero General Public License along with
#   this program. If not, see <http://www.gnu.org/licenses/>.

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
