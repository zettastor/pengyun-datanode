#!/usr/bin/perl
use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0).'/bin';

use EnvironmentUtils;
use CheckRamSize;
use Common;
use osversion;

# As device name is assigned by the kernel dynamically, 
# so we use the udev rule based on the device serial numer to 
# bundle raw disk.
#
# step 1:the program gets all the rules in the 60-rules.d first.
# one physical raw disk one rule based on its serial number.
# step 2:gets all the raw disk in the os.
# step 3:compare the result of step 1 and step 2 and then know the 
# new installed disk and write its rule in 60-raw.rules.

my $myScriptDir = $RealBin;

$myScriptDir = get_my_script_dir();
my $envRoot = get_actual_environment_root();
my $log = Log::Log4perl->get_logger("InitRawDisk");

if (getUsePythonScript() eq "true") {
    my $cmd = "python $envRoot/bin/init_raw_disk.py ";
    runPythonScriptAndExit($cmd);
}

exitProcess(__FILE__, __LINE__,
    "This script must be run from the datanode environment.") unless get_environment_alias() =~ /datanode.*/;

###### initialize var directory #############
my $storageDir = "$envRoot/var/storage";
my $rawDiskDir = "$storageDir/rawDisks";
my $systemRawDiskDir = "/dev/raw";
my $ruleFile = "$envRoot/var/storage/60-raw.rules";
my $recordSSDFile = "$envRoot/var/storage/ssd.record";
my $rollbackFilepath = "$storageDir/rollback.record";

# judge the system if it is supported
if (osversion::is_supported() == 0) {
    exitProcess(__FILE__, __LINE__, "system is not supported");
    exit 1;
}

#if 60-raw.rules not exists,then create it
touchFileIfNotExist($ruleFile);

removeIfExist($recordSSDFile);
touchFileIfNotExist($recordSSDFile);

removeIfExist($rollbackFilepath);

$log->debug("starting");

#check 60rule file
check60RuleFile($ruleFile);

# gets all the disk rule in the 60-raw.rules and store it in a hash table.
# key:serial number value:rawN the device bundle to 
$log->debug("starting to get dev list from rule file:$ruleFiles");
#Common::typeDefinition::oldDevList
my %oldDevList = getSerialToRawNameMapFrom06Rule();

$log->debug("finished to get dev list from the 60rule file");
while(my ($key, $value) = each %oldDevList){
    $log->debug("old device serial num:$key, rawName:$value");
}

$log->debug("starting to remove the raw device from $systemRawDiskDir");
removeRawDeviceInFolder($systemRawDiskDir);
$log->debug("finished to remove the raw device from $systemRawDiskDir");

# gets N of the last rawN	
my $lastRawNum = getMaxRawNumFromFileNameList(values(%oldDevList));
$log->debug("get the last raw number:$lastRawNum from 60rule file");

my $rawDiskMaxN = getMaxNFromDatanodeFolders();
if ($lastRawNum < $rawDiskMaxN) {
    $lastRawNum++;
}

$log->debug("get max raw Num:$lastRawNum");

# gets all raw disk in the system
$log->debug("starting to get all raw disk in the system");
# Common::typeDefinition::newDevList
my %newDevList = getAllRawDisk();

while(my ($key, $value) = each %newDevList){
    $log->debug("new device serial num:[$key], devName:[$value]");
}

# get remove raw disk
# Common::typeDefinition::oldDevList
my %plugoutList = getPlugoutRawDisks(\%oldDevList, \%newDevList);

while(my ($key, $value) = each %plugoutList){
    $log->debug("plugout device serial num:$key rawName:$value");
}

#remove raw and link
$log->debug("starting to remove device from rule file:$ruleFile");
removeRawDiskAndFileLineInfo(%plugoutList);
$log->debug("finished to remove device from rule file:$ruleFile");

$log->debug("starting to unlink disk, $rawDiskDir, $systemRawDiskDir");
unlinkDataNode($systemRawDiskDir, values(%plugoutList));
$log->debug("finished to unlink disk");

# check if the host has raw disk or exit
if (scalar(keys %newDevList) == 0) {
    exitProcess(__FILE__, __LINE__, "no raw disk in the system");
    exit 1;
}

# Common::typeDefinition::existDiskList
my $diskExistList = getExistRawDisks(\%newDevList, \%oldDevList);

# Common::typeDefinition::existDiskList
my $pluginList = getPluginRawDisks($lastRawNum, \%newDevList, \%oldDevList);

removeIfExist($ruleFile);
my @allDeviceList = (@{$diskExistList}, @{$pluginList});
mountThenUpdateRuleAndSSDFile(@allDeviceList);

deleteDeviceByRollbackFile();

# reboot the udev
##system("udevadm trigger");

$log->debug("finished");
exit 0;
