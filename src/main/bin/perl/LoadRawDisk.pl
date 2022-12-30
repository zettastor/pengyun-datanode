#!/usr/bin/perl
# The script loads raw disks
#
# this script checks whether initialization has been done or not. If it has not, 
# the script will initialize the datanode env. Otherwise, it won't done anything. 
# The initialization process might encounter errors. Those errors only result in a warning
# and this script will always exit succesfully.  This is becuase we don't
# want initialization failures to prevent the datanode initialization.

use strict;
use warnings;
use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use Common;
use EnvironmentUtils;

my $my_script_dir = $RealBin;

my $log = Log::Log4perl->get_logger("LoadRawDisks");

exitProcess(__FILE__, __LINE__, "This script must be run from the datanode environment.") unless get_environment_alias() =~ /datanode.*/;
my $env_root = get_actual_environment_root();

# wrap it all in an eval so any errors can be caught
my $disk_init = "$my_script_dir/InitRawDisk.pl";
exitProcess(__FILE__, __LINE__, "Can't find InitRawDisk at :$disk_init") unless -e $disk_init;

$log->debug("starting");
system("$disk_init");
$log->debug("finished");

# always exit cleanly
exit;
