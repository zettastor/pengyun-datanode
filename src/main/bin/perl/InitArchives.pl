#!/usr/bin/perl
# This script is used in the production environment to initialize linked raw disks. 
# By default, if these disks have been initialized, nothing is done but its update-time
# and users are changed. Otherwise, format these disks for data node to use ram disks.

# In some rare cases where all raw disks in a datanode need to be reformatted and wiped out,
# change the mode to overwrite. But use it with cautions because all data will be gone.

use strict;
use warnings;
use File::Path qw(make_path);
use FindBin '$RealBin';
use File::Spec;
use Getopt::Long;
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0).'/bin';
use EnvironmentUtils;
use CheckRamSize;
use Common;

my $myScriptDir = $RealBin;

$myScriptDir = get_my_script_dir();
my $envRoot = get_actual_environment_root();

my $log = Log::Log4perl->get_logger("InitArchives");
$log->warn("020 starting");

my $dataNodeStartTime = "$envRoot/var/storage/DataNodeStartTime";
my $systemRawDiskDir = "/dev/raw";
my $ruleFile = "$envRoot/var/storage/60-raw.rules";

open(RULEFILE, "$ruleFile") or exitProcess(__FILE__, __LINE__, "no such file:$ruleFile");
my @ruleFileContentList;
while(my $perLine = <RULEFILE>){
    push @ruleFileContentList, $perLine;
    $log->debug("rule file line:$perLine");
}
close(RULEFILE);

my $ruleFileParameterList;

$log->debug("starting to get rule file parameter");
$ruleFileParameterList = getRuleFileParameter();
$log->debug("finished to get rule file parameter:$ruleFileParameterList");


my $firstStart = undef;
if (-e $dataNodeStartTime) {
    $firstStart = "false";
} else {
    $firstStart = "true";
}

#Raw disk Archive Init.
my $subDir = APP_RAW_SUBDIR;
my @rawFileList = scanFiles("$envRoot/var/storage/$subDir");
my $rawNum = @rawFileList;

unless ($rawNum) {
    exitProcess(__FILE__, __LINE__, "Ther isn't any raw file in DIR $envRoot/var/storage/$subDir");
}

rawDiskArchiveInit(\@rawFileList, $ruleFileParameterList, $firstStart, APP_RAW_SUBDIR);

$log->debug("020 finished");
exit 0;
