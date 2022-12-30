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
exitProcess(__FILE__, __LINE__,
    "This script must be run from the datanode environment.") unless get_environment_alias() =~ /datanode.*/;

###### initialize directory #############
my $storageDir = "$envRoot/var/storage";
my $systemRawDiskDir = "/dev/raw";
my $ruleFile = "$envRoot/var/storage/60-raw.rules";
my $perlLog = "$envRoot/config/perl_log4j.properties";

Log::Log4perl->init($perlLog);
my $log = Log::Log4perl->get_logger("checkArchives");

if (getUsePythonScript() eq "true") {
    my $cmd = "python $envRoot/bin/init_raw_disk.py --only_check_plugin ";
    runPythonScriptAndExit($cmd);
}

$log->debug("======CheckArchives begin!!====");

#check 60rule file
check60RuleFile($ruleFile);

# gets all the disk rule in the 60-raw.rules and store it in a hash table.
# key:serialNum value:rawNum
# Common::typeDefinition::oldDevList
my %oldDevList = getSerialToRawNameMapFrom06Rule();

# gets N of the last rawN	
my $lastRawNum = getMaxRawNumFromFileNameList(values(%oldDevList));
my $rawDiskMaxN = getMaxNFromDatanodeFolders();
if ($lastRawNum < $rawDiskMaxN) {
    $lastRawNum++;
}

# gets all raw disk in the system
my @oldDevNames = getDevNamesFrom06Rule();
my %newDevList = checkAllRawDisk(@oldDevNames);

# Common::typeDefinition::existDiskList
my $pluginList = getPluginRawDisks($lastRawNum, \%newDevList, \%oldDevList);

if (scalar(@{$pluginList}) == 0) {
    $log->debug("======There is no disk plugin, process will finished.====");
    exit 0;
}

#mount raw to black device.
$log->debug("=====begin to Raw the plugined disk.");
mountThenUpdateRuleAndSSDFile(@{$pluginList});

# filter out init failed disks
for (my $index = 0; $index < @{$pluginList}; $index++) {
    my $devName = $$pluginList[$index]->{devName};

    if (checkIsInRollbackFile($devName)) {
        $log->debug("device devName:[$devName] is in rollback file, delete it from list.");
        splice @{$pluginList}, $index, 1;
        $index--;
    }
}

deleteDeviceByRollbackFile();

# begin of link disk

# collect all plugin disks info and use java to detect disk format(if detected will link file direct)
my $rawInfoList = getRawInfoByPluginList($pluginList);

remainedDiskInfoInPluginList($rawInfoList);

# init existing disks
foreach my $subDir (getAllDatanodeDiskSubFolders()) {
    $log->debug("Try to init existing disks.");
    my $folderFullPath = "$storageDir/$subDir";

    for (my $index = 0; $index < @{$rawInfoList}; $index++){
        my $rawName = $$rawInfoList[$index]->{rawName};
        my $appType = $$rawInfoList[$index]->{appType};
        my $rawPath = "$systemRawDiskDir/$rawName";

        if (APP_KNOWN eq $appType && defined(findLinkNameByOriginName($rawPath, $folderFullPath))) {
            $log->debug("Find existing disk, index:[$index] rawName:[$rawName] appType:[$appType] Try to init existing disk.");

            initDiskAndAckMsgThenRemoveFromList($rawInfoList, $subDir, $index);
            $index--;
        } else {
            $log->debug("Find unexisting disk, index:[$index] rawName:[$rawName] appType:[$appType].");
        }
    }
}

remainedDiskInfoInPluginList($rawInfoList);

# check whether auto decide disk usage
if (getAutoDistributeDiskUsageSwitch() eq "false") {
    my $linkFolderPath = findFolderPathByAppType(APP_UNSET);
    for (my $index = 0; $index < scalar(@$rawInfoList); $index++) {
        my $rawName = $$rawInfoList[$index]->{rawName};
        my $devType = $$rawInfoList[$index]->{devType};
        my $appType = $$rawInfoList[$index]->{appType};

        if ($appType eq APP_UNFORMAT && ($devType eq SSD_NAME || $devType eq PCIE_NAME)) {
            $log->debug("link for clean disk, rawDisk: devType:$devType, rawName $rawName, appType $appType");

            linkDiskByAppType($rawName, $systemRawDiskDir, $linkFolderPath, $devType, APP_UNSET);

            initDiskAndAckMsgThenRemoveFromList($rawInfoList, getCleanDiskSubFolder(), $index);
            $index--;
        }
    }
}

remainedDiskInfoInPluginList($rawInfoList);

# link for raw disk
for (my $index = 0; $index < @{$rawInfoList}; $index++) {
    my $rawName = $$rawInfoList[$index]->{rawName};
    my $devType = $$rawInfoList[$index]->{devType};
    my $appType = $$rawInfoList[$index]->{appType};

    if ($appType eq APP_UNFORMAT) {
        $log->debug("Index:[$index] rawDisk: devType:$devType, rawName $rawName, appType $appType");

        my $linkPath = findFolderPathByAppType(APP_RAW);
        linkDiskByAppType($rawName, $systemRawDiskDir, $linkPath, $devType, APP_RAW);

        initDiskAndAckMsgThenRemoveFromList($rawInfoList, APP_RAW_SUBDIR, $index);
        $index--;
    }
}

$log->debug("======CheckArchives finished!!====");

exit 0;

sub initDiskAndAckMsgThenRemoveFromList {
    my ($rawInfoList, $subDir, $index) = (@_);
    my $rawName = $$rawInfoList[$index]->{rawName};

    my @devList = ($$rawInfoList[$index]);
    initPluginRawDisksByInfoList(\@devList, "false", $subDir);
    pluginAckMsg($rawName, undef, $subDir);
    splice @$rawInfoList, $index, 1;
}
