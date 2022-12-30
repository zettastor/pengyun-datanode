#!/usr/bin/perl
# Link the raw disk to the system storage directory

use strict;
use warnings;
use File::Basename;
use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use Common;
use EnvironmentUtils;

#ARGV[0]: /dev/raw
my $system_raw_disk_dir = $ARGV[0];

my $myScriptDir = $RealBin;
my $envRoot = get_actual_environment_root();
if($envRoot eq "") {
    exitProcess(__FILE__, __LINE__, "envRoot is null");
}

my $storageDir   = "$envRoot/var/storage";

my $log = Log::Log4perl->get_logger("linkRawDisk");

if (getUsePythonScript() eq "true") {
    $log->debug("link raw disk operation has done in InitRawDisk.pl, so here don't need to do it again.");
    exit(0);
}

$log->debug("Remove all disk folders.");
foreach my $folderPath (getAllDatanodeDiskFolderPaths()) {
    if(-e $folderPath){
        execCmdAndGetRetLines("rm -rf $folderPath");
    }
}

$log->debug("Link raw disk starting.");

# create all directory
$log->debug("make path:[$storageDir]");
make_path($storageDir);
foreach my $folderPath (getAllDatanodeDiskFolderPaths()) {
    $log->debug("make path:[$folderPath]");
    make_path($folderPath);
}

unless ( defined $system_raw_disk_dir ) {
   my $name = basename($0);
   exitProcess(__FILE__, __LINE__, "there is no $name system_raw_disk_dir");
   exit 1;
}

my $firstStart = isDatanodeFirstTimeStart();

my @rawNamesInSystem = getAllRawNamesInFolder($system_raw_disk_dir);
$log->debug("System has raw disks as follow:@rawNamesInSystem" );

# query all raw disk and record propeties
# Common::typeDefinition::rawInfoList
my $rawInfoList = getRawInfoList($system_raw_disk_dir, @rawNamesInSystem);

#print remained raw disk info for debug
printRemainedRawDiskInfo($rawInfoList);

# check whether auto decide disk usage
if (getAutoDistributeDiskUsageSwitch() eq "false") {
    my $linkFolderPath = findFolderPathByAppType(APP_UNSET);
    for (my $index = 0; $index < scalar(@$rawInfoList); $index++) {
        my $devType = $$rawInfoList[$index]->{devType};
        my $rawName = $$rawInfoList[$index]->{rawName};

        if ($devType eq SSD_NAME || $devType eq PCIE_NAME) {
            $log->debug("link raw:$system_raw_disk_dir/$rawName, link type:$devType, linkFolderPath is $linkFolderPath");
            linkDiskByAppType($rawName, $system_raw_disk_dir, $linkFolderPath, $devType, APP_UNSET);
            splice @$rawInfoList, $index, 1;
            $index--;
        }
    }

    archiveInitByApplication($firstStart, getCleanDiskSubFolder());
}

printRemainedRawDiskInfo($rawInfoList);

# Link Raw disk begin.
$log->debug("Start to link the raw disk as data disk.");

for (my $index = 0; $index < @$rawInfoList; $index++) {
    my $devType  = $$rawInfoList[$index]->{devType};
    my $rawName  = $$rawInfoList[$index]->{rawName};
    my $linkPath = findFolderPathByAppType(APP_RAW);

    $log->debug("link raw:$system_raw_disk_dir/$rawName, link type:$devType, linkPath is $linkPath");
    linkDiskByAppType($rawName, $system_raw_disk_dir, $linkPath, $devType, APP_RAW);
}

#Init Archive of all raw disk
archiveInitByApplication($firstStart, APP_RAW_SUBDIR);

deleteDeviceByRollbackFile();

$log->debug("DataNode Linked and Archive init finished");

