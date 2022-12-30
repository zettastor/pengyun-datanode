#!/usr/bin/perl

use Exporter qw(import);

use FindBin qw($RealBin);
use File::Basename;
use File::Spec;
use Cwd;
# TODO: 
# 1. EnvironmentUtils can only be included when the script that calls it is run at the same directory
# as EnvironmentUtils does. 
# 2. Path::Class needs installed.
#
#
# get the the script absolute directory path
# We always assume that this script is under $env_root/bin
# so we can get the current script absolute directory path,
# and its parent dir is the environment root.

sub get_environment_alias {
# get the environment root first 
  my $env_root = get_actual_environment_root();
  return basename($env_root);
} 


sub get_actual_environment_root {
# get the script's directory
  my $myscript_dir = get_my_script_dir();
  return dirname($myscript_dir);
} 

sub get_my_script_dir {
	my $script_dir = File::Spec->catfile(cwd(),"bin");
	return $script_dir;
}

sub get_my_abs_script_dir {
  return $RealBin;
}

1;
