#!/usr/bin/perl
use strict;
use warnings FATAL => 'all';

use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use EnvironmentUtils;
use CheckRamSize;
use Common;
use osversion;

my $envRoot = get_actual_environment_root();
exitProcess(__FILE__, __LINE__, "This script must be run from the datanode environment.") unless get_environment_alias() =~ /datanode.*/;

my $perlLog = "$envRoot/config/perl_log4j.properties";

Log::Log4perl->init($perlLog);
my $log = Log::Log4perl->get_logger("ReinitArchive");

$log->debug("ReinitArchive parameter:[@ARGV].");

if (scalar(@ARGV) < 3) {
    $log->error("ReinitArchive parameter count less then 3, please check");
    exit(-1);
}

my $linkName = $ARGV[0];
my $forceRebuild = $ARGV[1];
my @appTypes = @ARGV[2..scalar(@ARGV)-1];
my $appTypeStr = listToStr(@appTypes);
$log->debug("ReinitArchive linkName:[$linkName] forceRebuild:[$forceRebuild] appTypes:[$appTypeStr].");

my $cmd = "python $envRoot/bin/ReinitArchive.py --link_name $linkName --type $appTypeStr ";
if ($forceRebuild eq "true") {
    $cmd .= " --force_rebuild ";
}

my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);

foreach my $line (@cmdRetLines) {
    chomp($line);
    print("$line\n");
}

exit($cmdRetCode)
