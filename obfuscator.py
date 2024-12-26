#!/usr/bin/python
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
import os
import re
from mechanize import Browser
from os import listdir
from os.path import isfile, join

current_path = os.path.dirname(os.path.realpath(__file__))
script_path = join(current_path, "src/main/bin")
onlyfiles = [f for f in listdir(script_path) if isfile(join(script_path, f))]

output_dir = '/tmp/dn_scripts_obs'
if not os.path.exists(output_dir): os.makedirs(output_dir)
output_files = [f for f in listdir(output_dir) if isfile(join(output_dir, f))]

br = Browser()
br.open("http://perlobfuscator.com/po.cgi")
for perl_script in onlyfiles:
  if (not re.search(".*\.p[lm]$", perl_script)):
    continue
  if (perl_script in output_files):
    continue
  # if (not perl_script in ["ArchiveForLogCache.pm", "Common.pm", "FDisk.pm", "MKFS.pm", "CheckRamSize.pm", "EnvironmentUtils.pm", "osversion.pm"]) :
  # continue

  print
  "Obfuscating script %s ..." % perl_script
  perl_in = open(join(script_path, perl_script))

  br.select_form(name="theform")
  br['perl'] = perl_in.read();
  br.find_control(name="decomment").items[0].selected = True
  br.find_control(name="sightly").items[0].selected = True
  '''
  br.find_control(name="uu").items[0].selected = True
  br.find_control(name="stringy_bare").items[0].selected = True
  br.find_control(name="mangle_string").items[0].selected = True
  '''
  response = br.submit()  # submit current form
  # print response.read()
  perl_in.close()

  br.select_form(name="theform")
  perl_out = open(join(output_dir, perl_script), "w")
  perl_out.write(br['result'])
  perl_out.close()

  '''
  print "-----before obfuscate"
  print br['perl']
  print "+++++after obfuscate"
  print br['result']
  '''

print
"*** Done obfuscation!***"
file = open(join(output_dir, 'success123456789'), 'w+')
file.close()
