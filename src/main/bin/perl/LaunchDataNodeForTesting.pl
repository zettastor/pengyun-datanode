#!/usr/bin/perl

use strict;
use warnings;
use File::Path qw(make_path);
use FindBin '$RealBin';
use File::Spec;
use Getopt::Long;

sub create_dn_config_file {
  my ($port, $heartbeat_port, $io_port, $config_file) = (@_);
  my $donot_start_flusher = 0;
  unless ( -e $config_file ) {
      die "The specified dn configuration property file $config_file doesn't exist";
  }
  my $tmp_file = "/tmp/dn_config";
  `grep -v \"instance.id.for.testing\" $config_file > $tmp_file; mv $tmp_file $config_file`;
  `grep -v \"app.main.endpoint\" $config_file > $tmp_file; mv $tmp_file $config_file`;
  `grep -v \"app.io.endpoint\" $config_file > $tmp_file; mv $tmp_file $config_file`;
  `grep -v \"app.heartbeat.endpoint\" $config_file > $tmp_file; mv $tmp_file $config_file`;

my $cmd = "echo \"instance.id.for.testing=$port\" >> $config_file";
  (system ($cmd) == 0) or die "can't create instance.id.for.testing to the configuration file\n";

  $cmd = "echo \"app.main.endpoint=$port\" >> $config_file";
  (system ($cmd) == 0) or die "can't create the data node $port configuration file\n";

  $cmd = "echo \"app.heartbeat.endpoint=$heartbeat_port\" >> $config_file";
  (system ($cmd) == 0) or die "can`t create the data node $heartbeat_port configuration file\n";

  $cmd = "echo \"app.io.endpoint=$io_port\" >> $config_file";
  (system ($cmd) == 0) or die "can`t create the data node $io_port configuration file\n";

}

my $my_script_dir = $RealBin;
require ("$my_script_dir/EnvironmentUtils.pm");
$my_script_dir = get_my_script_dir();
my $env_root = get_actual_environment_root();
die "This script must be run from the datanode environment.\n" unless get_environment_alias() =~ /.*datanode.*/;

# give a impossible port (1) by the default
my $port = 1;
my $verbose= 1;
my $heartbeat_port = 1;
my $io_port = 1;

############## TODO: make sure that port parameter is required #######
GetOptions ( "port=i"  => \$port,
			 "heartbeatPort=i"  => \$heartbeat_port,
			 "ioPort=i"  => \$io_port,
             "verbose"  => \$verbose)
 or die "Error in command line arguments. \n\nThe Format is LaunchDataNodeForTesting.pl --port <port> --heartbeatPort <heartbeat_port> -ioPort <io_port> --verbose \n";

my $config_file = "$env_root/config/datanode.properties";
# change some properties in the datanode.properties file for the testing 
create_dn_config_file($port, $heartbeat_port, $io_port, $config_file);

my $cmd = "$my_script_dir/start_datanode.sh";
print "$cmd\n" if $verbose;
(system($cmd) == 0) or die "ERROR: can't launch data node $?\n";
