#!/usr/bin/perl

use strict;
use warnings;
use File::Path qw(make_path);
use FindBin '$RealBin';
use Getopt::Long;
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0).'/bin';
use EnvironmentUtils;
use CheckRamSize;
use Common;

my $envRoot = get_actual_environment_root();

exitProcess(__FILE__, __LINE__,
    "This script must be run from the datanode environment.") unless get_environment_alias() =~ /datanode.*/;

###### initialize var directory #############
my $logDir = "$envRoot/logs";
my $configDir = "$envRoot/config";
my $systemRawDiskDir = "/dev/raw";
my $storageDir = "$envRoot/var/storage";

my $log = Log::Log4perl->get_logger("InitDataNode");

$log->warn("010 starting");
make_path($logDir, $storageDir);

$log->debug("starting LoadRawDisk.pl");
system("/usr/bin/perl $envRoot/bin/LoadRawDisk.pl");
$log->debug("finished LoadRawDisk.pl");

$log->debug("Let's sleep 2 seconds so that the stupid perl can read those raw files that were just created");
sleep 2;

$log->debug("starting LinkRawDisk.pl");
# link all disks including ssd and pcie
system("/usr/bin/perl $envRoot/bin/LinkRawDisk.pl $systemRawDiskDir");
$log->debug("finished LinkRawDisk.pl");

sleep 2;

$log->warn("010 finished");
exit 0;
