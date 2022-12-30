#!/usr/bin/python
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
