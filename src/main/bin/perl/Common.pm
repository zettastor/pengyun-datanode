#!/usr/bin/perl

use Exporter qw(import);
use Tie::File;
use File::Basename;
use File::Path qw(make_path);
use File::Path qw(remove_tree);
use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0).'/bin';

use EnvironmentUtils;
use Log::Log4perl;
use osversion;
use ConfigLocation;

use constant PCIE_NAME => "pcie";
use constant SSD_NAME => "ssd";
use constant SATA_NAME => "sata";

#this name is response to /dev/raw/rawX,don't modifify it.
use constant RAW_NAME => "raw";

#TODO: will be deleted later.
use constant APP_RAW => "RAW_DISK";              #RAW_DISK(2),
use constant APP_UNSET => "UNSET_DISK";          #UNSET_DISK(6),
#TODO: will be deleted later.

use constant APP_UNFORMAT => "APP_UNKNOWN";    #unknow disk format,may be unformat.
use constant APP_KNOWN => "APP_KNOWN";

use constant APP_RAW_SUBDIR => "rawDisks";

use constant LINK_NAME_SEPARATOR => "_";

use constant GB_SIZE => 1073741824;
use constant MB_SIZE => 1048576;

use constant BIG_GB => 10000;

#use constant DEV_DISK_TYPE_PCIE => "PCIE";
#use constant DEV_DISK_TYPE_SSD => "SSD";
#use constant DEV_DISK_TYPE_SATA => "SATA";

my $myEnvRoot = &get_actual_environment_root();

my $configFolderPath = "$myEnvRoot/config";
my $configFile = "$configFolderPath/datanode.properties";
my $archiveConfigFile = "$configFolderPath/archive.properties";
my $perlLog = "$configFolderPath/perl_log4j.properties";

my $storageDir = "$myEnvRoot/var/storage";
my $ssdRecordFile = "$storageDir/ssd.record";
my $sixZeroruleFilepath = "$storageDir/60-raw.rules";
my $dataNodeStartTime = "$storageDir/DataNodeStartTime";
my $rollbackFilepath = "$storageDir/rollback.record";

# initialize the log
make_path("$myEnvRoot/logs");

Log::Log4perl->init($perlLog);
my $log = Log::Log4perl::get_logger("Common");

my $archiveInitMode = getArchiveInitMode();

my %configCache = ();

# the function is called when the perl script meet error, and exit the perl execution.
# parameter 1 is the file name:__FILE__
# parameter 2 is the file name:__LINE__
# parameter 3 is the message which tells you the reason that exits.
sub exitProcess {
    my ($fileName, $fileLine, $msg) = (@_);
    $log->error("exit fileName:$fileName, fileLine:$fileLine, msg:$msg");
    die "exit";
}

# the function scan the specified DIR to get files that match the keyword
# parameter 1 is the directory you want to scan 
# parameter 2 is the keyword that filename matches
sub scanFiles{
    my ($dir) = (@_);
    my $pcieName = PCIE_NAME;
    my $ssdName = SSD_NAME;
    my $sataName = SATA_NAME;

    my @filenames;
    foreach my $filename (getAllFilenamesFromFolder($dir)) {
        if ($filename =~ m/$pcieName/ || $filename =~ m/$ssdName/ || $filename =~ m/$sataName/) {
            push @filenames, $filename;
        }
    }

    return @filenames;
}

# the function get middle file old dev list
# parameter 1 is file content list
# return: Common::typeDefinition::existDiskList, but devName is ""
sub getRuleFileParameter{
    my @ruleFileContentList = readFileLines($sixZeroruleFilepath);

    my @list;
    foreach my $perLine (@ruleFileContentList) {
        my $serialNum = parseSerialNumFromOne60RuleLine($perLine);
        my $devName   = parseDevNameFromOne60RuleLine($perLine);
        my $rawName   = parseRawNameFromOne60RuleLine($perLine);

        if ($serialNum && $devName && $rawName) {
            my %map = (
                serialNum => $serialNum,
                devName   => $devName,
                rawNum    => $rawName
            );
            push @list, \%map;
        }
    }
    return (\@list);
}

# return (devName, serial)
sub getDevNameAndSerialByRawNameFrom60rule {
    my ($targetRawName) = @_;

    my ($devName, $serialNum, $rawName) = queryDevInfoFrom60ruleFile(undef, undef, $targetRawName);
    return ($devName, $serialNum);
}

# query (devName, serialNum, rawName) from 60rule
sub queryDevInfoFrom60ruleFile {
    my ($targetDevName, $targetSerialNum, $targetRawName) = @_;

    if (!$targetDevName && !$targetSerialNum && !$targetRawName) {
        $log->warn("Query device info from 60rule, but all condition is empty.");
        return (undef, undef, undef);
    }

    my @ruleFileContentList = readFileLines($sixZeroruleFilepath);

    foreach my $perLine (@ruleFileContentList) {
        my $serialNum = parseSerialNumFromOne60RuleLine($perLine);
        my $devName   = parseDevNameFromOne60RuleLine($perLine);
        my $rawName   = parseRawNameFromOne60RuleLine($perLine);

        if (!$serialNum || !$devName || !$rawName) {
            next;
        }

        if ($targetDevName && !($targetDevName eq $devName)) {
            next;
        }

        if ($targetSerialNum && !($targetSerialNum eq $serialNum)) {
            next;
        }

        if ($targetRawName && !($targetRawName eq $rawName)) {
            next;
        }

        return ($devName, $serialNum, $rawName);
    }

    return (undef, undef, undef);
}

#$ruleFileParameterList, "false"
sub initPluginRawDisksByInfoList{
    my ($rawInfoList, $firstStart, $subDir) = (@_);

    for (my $index = 0; $index < @{$rawInfoList}; $index++) {
        my $rawName   = $$rawInfoList[$index]->{rawName};
        my $devType   = $$rawInfoList[$index]->{devType};
        my $serialNum = $$rawInfoList[$index]->{serialNum};
        my $devName   = $$rawInfoList[$index]->{devName};
        $log->debug("Try to init plugin disk, rawName:[$rawName] devType:[$devType] serialNum:[$serialNum] devName:[$devName].");

        my $rawPath = "/dev/raw/$rawName";
        my $folderFullPath = "$storageDir/$subDir";

        my $linkName = findLinkNameByOriginName($rawPath, $folderFullPath);
        if (!defined($linkName)) {
            $log->debug("Can't find link name for $rawPath in $folderFullPath, Can't init archive.");
            next;
        }

        $log->debug("Find link name:[$linkName] for $rawPath in $folderFullPath.");
        $log->debug("Raw disk init start:firstStart:envRoot:$myEnvRoot, $firstStart, serialNum:$serialNum, linkName:$subDir/$linkName,devName $devName, runInRealTime true, devType: $devType subDir $subDir");
        runArchiveJavaProgram($myEnvRoot, $firstStart, $serialNum, "$subDir/$linkName", $devName, "true", $devType, $subDir);
    }
}

sub findLinkNameByOriginName {
    my ($originName, $folder) = @_;

    my $cmd = "ls -la $folder 2>&1";
    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);
    if ($cmdRetCode != 0 || scalar(@cmdRetLines) <= 0) {
        $log->debug("Can't find any link file in folder:[$subDir].");
        return undef;
    }

    foreach my $line (@cmdRetLines) {
        if ($line =~ /(\w+)\s*\-\>\s*$originName/) {
            my $linkName = $1;
            $log->debug("Find link file:[$linkName] for origin file:[$originName] in folder:[$folder].");
            return $linkName;
        }
    }

    $log->debug("Can't find link file for origin file:[$originName] in folder:[$subDir].");
    return undef;
}

# the function modify middle file content
# parameter 1 is file path
# parameter 2 is disk serial num
sub modifyFileContent{

    my ($fileName, $seriNum) = (@_);

    my @lines;

    tie(@lines, 'Tie::File', $fileName) or die;

    my $num = 0;
    while($num < @lines){
        if ($lines[$num] =~ /$seriNum/) {
            $lines[$num] =~ s/addDisk/noChange/g;
        }
        $num++;
    }
    untie(@lines);
}

sub checkExistInSsdRecordFile{
    my ($deviceName) = (@_);

    return checkIfFileContainWord($ssdRecordFile, $deviceName);
}

sub checkIfFileContainWord {
    my ($filepath, $word) = @_;

    my @lines = readFileLines($filepath);
    foreach my $line (@lines) {
        if ($line =~ /\b$word\b/) {
            $log->debug("file:[$filepath] line:[$line] contain word:[$word].");
            return 1;
        }
    }

    $log->debug("file:[$filepath] don't contain word:[$word].");
    return 0;
}

sub removeLineIfContainWord {
    my ($filepath, $word) = @_;

    $log->debug("Try to remove line from file:[$filepath] by word:[$word].");

    if (!$word) {
        $log->debug("Try to remove line from file:[$filepath] by word:[$word] but word is empty.");
        return;
    }

    my $pattern = "\\b$word\\b";
    removeLineIfMatchPattern($filepath, $pattern);
}

sub removeLineIfContainSerialNum {
    my ($filepath, $serialNum) = @_;

    $log->debug("Try to remove line from file:[$filepath] by serialNum:[$serialNum].");

    if (!$serialNum) {
        $log->debug("Try to remove line from file:[$filepath] by serialNum:[$serialNum] but serialNum is empty.");
    }

    my $pattern = "\"\\s*$serialNum\\s*\"";
    removeLineIfMatchPattern($filepath, $pattern);
}

sub removeLineIfMatchPattern {
    my ($filepath, $pattern) = @_;

    if (!$pattern) {
        $log->debug("remove line from file:[$filepath] by pattern:[$pattern], but pattern is empty.");
        return;
    }

    $log->debug("remove line from file:[$filepath] by pattern:[$pattern].");

    my @lines;
    tie(@lines, 'Tie::File', $filepath) or exitProcess("remove line from file:[$filepath] by pattern:[$pattern] failed, tie failed.");
    for (my $index = 0; $index < scalar(@lines); $index++) {
        my $line = $lines[$index];
        if ($line =~ /$pattern/) {
            $log->warn("remove line:[$line] from file:[$filepath] by pattern:[$pattern].");
            splice(@lines, $index, 1);
            $index--;
        }
    }

    untie(@lines) or exitProcess("fail to untie when remove line from file:[$filepath] by pattern:[$pattern].");
}

# the function parse 60rule get devList
# parameter 1 is file handle
# oldDevList{key:serialsNum}=value:rawName
# return: oldDevList -> Common::typeDefinition::oldDevList
sub getSerialToRawNameMapFrom06Rule {
    my %serialToRawNameMap;

    my @lines = readFileLines($sixZeroruleFilepath);
    foreach my $line (@lines) {
        #ACTION=="change",KERNEL=="sda",PROGRAM=="/lib/udev/scsi_id -g -u /dev/%k",RESULT=="0QEMU_QEMU_HARDDISK_drive-scsi1-0-0-0",RUN+="/sbin/raw /dev/raw/raw1 %N"

        my $serialNum = parseSerialNumFromOne60RuleLine($line);
        my $rawName = parseRawNameFromOne60RuleLine($line);

        if ($serialNum && $rawName) {
            $serialToRawNameMap{$serialNum} = $rawName;
        }
    }

    return %serialToRawNameMap;
}

sub parseSerialNumFromOne60RuleLine {
    my ($line) = @_;

    #ACTION=="change",KERNEL=="sda",PROGRAM=="/lib/udev/scsi_id -g -u /dev/%k",RESULT=="0QEMU_QEMU_HARDDISK_drive-scsi1-0-0-0",RUN+="/sbin/raw /dev/raw/raw1 %N"
    if ($line =~ /RESULT=="(.+?)"/) {
        my $serialNum = $1;
        chomp($serialNum);
        return $serialNum;
    } else {
        return undef;
    }
}

sub parseRawNameFromOne60RuleLine {
    my ($line) = @_;

    #ACTION=="change",KERNEL=="sda",PROGRAM=="/lib/udev/scsi_id -g -u /dev/%k",RESULT=="0QEMU_QEMU_HARDDISK_drive-scsi1-0-0-0",RUN+="/sbin/raw /dev/raw/raw1 %N"
    if ($line =~ /RUN\+=".+\b(raw\d+)\b.+?"/) {
        my $rawName = $1;
        chomp($rawName);
        return $rawName;
    } else {
        return undef;
    }
}

sub parseDevNameFromOne60RuleLine {
    my ($line) = @_;

    #ACTION=="change",KERNEL=="sda",PROGRAM=="/lib/udev/scsi_id -g -u /dev/%k",RESULT=="0QEMU_QEMU_HARDDISK_drive-scsi1-0-0-0",RUN+="/sbin/raw /dev/raw/raw1 %N"
    if ($line =~ /KERNEL=="\b(.+?)\b"/) {
        my $devName = $1;
        chomp($devName);
        return $devName;
    } else {
        return undef;
    }
}

sub getDevNamesFrom06Rule {
    my @lines = readFileLines($sixZeroruleFilepath);
    my @devNames = ();

    foreach my $line (@lines) {
        my $devName = parseDevNameFromOne60RuleLine($line);

        if ($devName) {
            push(@devNames, $devName);
        }
    }

    return @devNames;
}

# the function check 60rule
# parameter 1 is 60rule path
sub check60RuleFile{
    my ($fileName) = (@_);
    my $action = "ACTION==";
    my $kernel = ",KERNEL==";
    my $program = ",PROGRAM==";
    my $result = ",RESULT==";
    my $run = ",RUN";

    my @lines;
    tie(@lines, 'Tie::File', $fileName) or exitProcess(__FILE__, __LINE__, "can not read data from file:$fileName");

    my $num = 0;
    while($num < @lines){
        if ($lines[$num] ne "") {
            if (($lines[$num] =~ /$action/) && ($lines[$num] =~ /$kernel/) && ($lines[$num] =~ /$program/) && ($lines[$num] =~ /$result/) && ($lines[$num] =~ /$run/)) {
            } else {
                splice (@lines, $num, 1);
            }
        } else {
            splice (@lines, $num, 1);
        }
        $num++;
    }
    untie(@lines);
}

# the function perl script run java program
# parameter 1 is environment path
# parameter 2 is datanode firstTimeStart flag
# parameter 3 is disk is serial Num
# parameter 4 is rawdisk
# parameter 5 is system raw disk dir
# parameter 6 is raw to devName
# parameter 7 is sytem raw disk dir
sub runArchiveJavaProgram{
    my ($envRoot, $firstTimeStart, $serialNumber, $linkName, $devName, $runInRealTime, $storageType, $subDir) = (@_);

    my $devFullName = "/dev/$devName";

    $log->debug("before run java command: ArchiveInitializer");
    my $cmd = "java -noverify -cp :$envRoot/build/jar/*:$envRoot/lib/*:$envRoot/config py.datanode.archive.ArchiveInitializer --firstTimeStart $firstTimeStart --serialNumber $serialNumber --devName $devFullName --storageType $storageType --runInRealTime $runInRealTime --storage $envRoot/var/storage/$linkName --subDir $subDir 2>&1 > $envRoot/logs/launch.log";
    $log->debug("after run java command:$cmd");
    if (system($cmd) == 0) {
        $log->warn("success to run cmd:$cmd");
        return 0;
    } else {
        $log->warn("fail to run cmd:$cmd");
        return -1;
    }
}

# the function remove dev/raw/rawn
sub removeRawDeviceInFolder{
    my ($systemRawDiskDir) = (@_);

    my @rawNames = getAllRawNamesInFolder($systemRawDiskDir);

    foreach my $rawName (@rawNames) {
        unrawRawDisk($rawName);
    }

    ###empty the system raw disk dir to prevent some useless raw file that cause data node start fail
    system("rm -rf $systemRawDiskDir/raw[0-9]*");
}

sub getMaxRawNumFromFileNameList{
    my (@filenames) = (@_);

    my $maxNum = 0;
    foreach my $filename (@filenames) {
        if ($filename =~ /(\d+)$/) {
            my $currentRawNum = $1;
            if ($currentRawNum > $maxNum) {
                $maxNum = $currentRawNum;
            }
        }
    }

    $log->debug("get max number:[$maxNum] from filenames:[@filenames].");

    return $maxNum;
}

# the function get disk file system Type
# parameter 1 is dev name
sub hasFileSystemInRawDisk{
    my ($devName) = (@_);

    chomp($devName);

    my $allDev = `df -lT |grep "\/dev\/sd*" | awk '{print \$1 "," \$2}'`;
    my @devArray = split(/[\r\n]/, "$allDev");

    my %devList;

    my $count = 0;
    while($count < @devArray){
        my @array = split(/,/, "$devArray[$count]");
        chomp($array[0]);
        chomp($array[1]);
        $devList{$array[0]} = $array[1];
        if ($array[0] =~ /$devName/)
        {
            if (exists  $devList{$array[0]}) {
                return 1;
            }
        }
        $count++;
    }
    return 0;
}

sub getDiskInfo {
    my @diskArry;
    my $str = `lsblk`;
    my @strArry = split(/[\r\n]/, $str);

    #delete the title "NAME            MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT"
    splice @strArry, 0, 1;

    my $recordNum = @strArry;

    my $num = 0;
    while($num < $recordNum) {

        #get the first word perl line.
        my $getStr = $strArry[$num];
        my $noParDisk = false;

        #get next string to judge whether the next string is paritio
        if (($getStr =~ /disk/i) && (($num < $recordNum - 1) && ($strArry[$num + 1] =~ /^[a-z]/i))) {
            $noParDisk = true;
        } elsif (($num == ($recordNum - 1)) && ($getStr =~ /disk/i)) {
            $noParDisk = true;
        } else {
            $noParDisk = false;
        }

        if ($noParDisk eq true) {
            my @subArry = split(/ /, $getStr);
            $getStr = $subArry[0];
            if (($getStr =~ /^[a-z]/) && !($getStr =~ /pyd/i)) {
                $getStr =~ s/(\W+)([a-z])/$2/;
                push @diskArry, $getStr;
            }
        }

        $num++;
    }
    return (\@diskArry);
}

# return: array("sda", "sdb", "sdc)
sub getDisksWithoutPartionInfo {
    my @diskArry;
    my $cmdRetstr = `lsblk`;
    $log->debug("getDisksWithoutPartionInfo cmd result:$cmdRetstr");

    my @cmdRetStrArry = split(/[\r\n]/, $cmdRetstr);

    #delete the title "NAME            MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT"
    splice @cmdRetStrArry, 0, 1;

    my $totalRowNum= scalar @cmdRetStrArry;
    my @ignoreDeviceRegs = getIgnoreDeviceRegs();

    for (my $rowIndex = 0; $rowIndex < $totalRowNum; $rowIndex++) {
        #get the first word perl line.
        my $line = $cmdRetStrArry[$rowIndex];
        my $noParDisk = false;

        #get next string to judge whether the next string is paritio
        if (($line =~ /disk/i) && (($rowIndex < $totalRowNum - 1) && ($cmdRetStrArry[$rowIndex + 1] =~ /^[a-z]/i))) {
            $noParDisk = true;
        } elsif (($rowIndex == ($totalRowNum - 1)) && ($line =~ /disk/i)) {
            $noParDisk = true;
        } else {
            $noParDisk = false;
            next;
        }

        #sdb      8:16   0   20G  0 disk
        my ($devName,) = split(/ /, $line);
        $devName =~ s/(\W+)([a-z])/$2/;

        # check if ignore by config
        my $isIgnoredByConfig = false;
        foreach my $reg (@ignoreDeviceRegs) {
            chomp($reg);
            if ($devName =~ /$reg/) {
                $log->debug("getDisksWithoutPartionInfo $devName is ignored by [$reg].");
                $isIgnoredByConfig = true;
                last;
            }
        }

        if (($isIgnoredByConfig eq false)
            && ($devName =~ /^[a-z]/)
            && !($devName =~ /pyd/i)
            #&& getDiskSize("/dev/$devName") > 0
        ) {
            $log->debug("getDisksWithoutPartionInfo: find device:$devName");
            push @diskArry, $devName;
        }
    }

    return (\@diskArry);
}

sub getParitionInfo {
    my @diskArry;

    my $str = `lsblk`;

    my @strArry = split(/[\r\n]/, $str);

    #delete the title "NAME            MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT"
    splice @strArry, 0, 1;

    my $num = 0;
    while($num < @strArry) {

        #get the first word perl line.
        my $getStr = $strArry[$num];
        if ($getStr =~ /part/i) {
            my @subArry = split(/ /, $getStr);
            $getStr = $subArry[0];

            if (!($getStr =~ /^[a-z]/) && ($getStr =~ /\d$/) && !($getStr =~ /pyd/i)) {
                $getStr =~ s/(\W+)([a-z])/$2/;
                push @diskArry, $getStr;
            }
        }
        $num++;
    }

    return (\@diskArry);
}



# the function get all raw disks
# parameter 1 is log path
# return: Common::typeDefinition::newDevList
sub getAllRawDisk{
    my @emptyList = ();
    checkAllRawDisk(@emptyList);
}

# get scsi id for device
# parameter like /dev/sda or /dev/raw/raw1
sub getScsiIdByJavaThenSystem {
    my ($devPath) = @_;

    $log->debug("Get sisiid for devPath:[$devPath] first by java program then by system.");
    my $scsiid = getScsiIdByJavaProgram($devPath);
    if ($scsiid) {
        $log->debug("Get sisiid for devPath:[$devPath] first by java program success scsiid:[$scsiid].");
        return $scsiid;
    }

    $scsiid = getScsiIdFromSystem($devPath);
    if ($scsiid) {
        $log->debug("Get sisiid for devPath:[$devPath] second by system success scsiid:[$scsiid].");
        return $scsiid;
    }

    $log->debug("Get sisiid for devPath:[$devPath] first by java program then by system failed, return [undef].");
    return undef;
}

# use java program to read scsiid in archive metadata
# parameter like /dev/sda or /dev/raw/raw1
sub getScsiIdByJavaProgram {
    my ($devPath) = @_;

    $log->debug("Get scsiid by datanode java program for devPath:[$devPath].");

    my $cmd = "java -noverify -cp :$myEnvRoot/build/jar/*:$myEnvRoot/lib/*:$myEnvRoot/config py.datanode.QuerySerialNumber --storagePath $devPath";
    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);
    if (($cmdRetCode != 0) || (scalar(@cmdRetLines) != 1)) {
        $log->debug("Get scsiid by datanode java program for devPath:[$devPath] failed, return [undef].");
        return undef;
    }

    my $scsiid = $cmdRetLines[0];
    chomp($scsiid);

    $log->debug("Get scsiid by datanode java program for devPath:[$devPath] success, scsiid:[$scsiid].");
    return $scsiid;
}

# parameter like /dev/sda or /dev/raw/raw1
sub getScsiIdFromSystem {
    my ($devPath) = @_;

    $log->debug("Get scsiid from system for devPath:[$devPath].");

    my $cmd = "/lib/udev/scsi_id -g -u $devPath";
    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);
    if (($cmdRetCode != 0) || (scalar(@cmdRetLines) != 1)) {
        $log->debug("Get scsiid from system for devPath:[$devPath] failed, return [undef].");
        return undef;
    }

    my $scsiid = $cmdRetLines[0];
    chomp($scsiid);

    $log->debug("Get scsiid from system for devPath:[$devPath] success, scsiid:[$scsiid].");
    return $scsiid;
}

## get the iscsi virtual disk.
## In some customer test environment, client and data node co-exist on same server.
## When data node startup, we should not use some disk added by the iscsi client.
sub getAllIscsiVirtualDisk {
    my $diskByPath = `ls -al /dev/disk/by-path`;
    my @pathLines = split(/[\r\n]/, "$diskByPath");
    my $iscsiSession = `iscsiadm -m session 2>&1`;
    $log->debug("cmd:iscsiadm -m session, result:$iscsiSession");
    my @virtualDiskList;

    my @sessions = split(/[\r\n]/, "$iscsiSession");
    foreach my $session (@sessions) {
        (my $ip = $session) =~ s/.*\s+(\d+\.\d+\.\d+\.\d+\:\d+).*/$1/g;
        chomp($ip);
        if ($ip) {
            foreach my $pathLine (@pathLines) {
                if ($pathLine =~ /.*ip-$ip.*/) {
                    (my $virtualDisk = $pathLine) =~ s/.*->\s+.*\/(.*)/$1/g;
                    $virtualDiskList[@virtualDiskList] = $virtualDisk;
                }
            }
        }
    }
    return @virtualDiskList;
}

sub isVirtualDisk{
    my ($disk, @virtualDisks) = (@_);
    foreach my $virtualDisk (@virtualDisks) {
        if ($disk eq $virtualDisk) {
            return 1;
        }
    }
    return 0;
}

## get all the serial number of disks which have been mapped to multipath disk
## the return result is
sub getDiskNameOfAllMultiPathDisk {
    my $multiPathResult = `ls -l /dev/mapper`;
    my %serialWithDiskNameMap = undef;

    my @multiPathName = split(/[\r\n]/, $multiPathResult);

    foreach $num (@multiPathName) {
        if ($num =~ /->/) {

            #lrwxrwxrwx. 1 root root 7 Jul 23 11:23 centos-root -> ../dm-0
            if ($num =~ m/.*\s+(.*)\s+->\s+\.\.\/(.*)/) {
                my $mapName = $1;
                $serialWithDiskNameMap{"$mapName"} = "mapper\/".$mapName;
                $log->debug("$mapName->$serialWithDiskNameMap{\"$mapName\"}\n");

                #the info dm-x is not used on current.
                #my $targetName = $2;
            }
        }
    }

    return %serialWithDiskNameMap;
}


#sub getSeriesNumberAndDiskNameOfAllMultiPathDisk {
#    $ENV{"LANG"} = "en_US.UTF-8";
#    my $multiPathResult = `fdisk -l 2>/dev/null | grep "Disk \/dev\/mapper\/*" | grep -v "\/dev\/md" `;
#    my @multiPathSeries = split(/[\r\n]/, $multiPathResult);
#    my %serialWithDiskNameMap;
#    foreach $num (@multiPathSeries) {
#        chomp($num);
#        $num =~ s/.*\/dev\/mapper\/(.*)\:.*/$1/g;
#        my $diskName = "mapper\/".$num;
#        ## in virtual machine, some the serial number is split by space. it need to replace the space with "-";
#        if ($num =~ /.*\s+.*/) {
#            $num = replaceSpaceWithUnderLine($num);
#        }
#        $serialWithDiskNameMap{"$num"} = $diskName;
#    }
#    return %serialWithDiskNameMap;
#}
#
#### replace the space with "-"
#sub replaceSpaceWithUnderLine {
#    my ($strWithSpace) = (@_);
#    my @slices = split(/\s+/, $strWithSpace);
#    my $strWithLine;
#    foreach my $slice (@slices) {
#        if ($strWithLine eq "") {
#            $strWithLine = $strWithLine.$slice;
#        } else {
#            $strWithLine = $strWithLine."_".$slice;
#        }
#    }
#    return $strWithLine;
#}

# the function check all raw disks
# param: Common::typeDefinition::oldDevRawList
sub checkAllRawDisk {
    my (@filteredDevNames) = @_;

    my %newDevList;
    my @virtualDiskList = getAllIscsiVirtualDisk();
    my %serialNumAndDiskNameOfMultiPath = getDiskNameOfAllMultiPathDisk();

    # gets all the disk in system
    $ENV{"LANG"} = "en_US.UTF-8";

    #get no partion disk from disks.
    my $diskArryTmp = getDisksWithoutPartionInfo();
    my @devNames = @$diskArryTmp;

    #no disk in current datanode, fdisk must be execute failed. For there must be a system disk
    if (scalar(@devNames) == 0) {
        $log->error("fail to get disk info.");
        return %newDevList;
    }

    #devMap the key is devname, value is extend devname
    my %devMap;

    #get extend disk list
    for (my $index = 0; $index < scalar(@devNames); $index++) {
        my $devName = $devNames[$index];
        $log->debug("disk info devName:[$devName].");

        #old disks would skip mount check in the following.
        if (listContains($devName, @filteredDevNames)) {
            $log->debug("devName:[$devName] is in filtered devNames:[@filteredDevNames], will skip the format check by cmd mount.");
            next;
        }

        #All disks has no partion,
        $devMap{$devName} = "null";
    }

    #get mount information in the system
    my $mountResult = `mount`;
    $log->debug("mount command result:$mountResult");

    for (my $index = 0; $index < scalar(@devNames); $index++) {
        my $devName = $devNames[$index];
        chomp($devName);
        my $devPath = "/dev/$devName";
        my $mountFlag = "false";

        #check devname mounted
        if ($mountResult =~ /$devPath/) {
            $mountFlag = "true";
        } else {
            $mountFlag = "false";
        }
        $log->debug("current device name: $devPath, mount flag name:$mountFlag");

        if ($mountFlag eq "false" && !hasFileSystemTypeRawDiskMounted($devName, %devMap) && !isVirtualDisk($devName,
            @virtualDiskList)) {
            # No partition was found
            my $seri = getScsiIdByJavaThenSystem($devPath);

            # if no serial number exist, use dev name for serial number
            if (!$seri) {
                $log->warn("there is no serial num for device:$devPath");
                $seri = $devPath;
            }

            ## check the disk has been used as multipath disk, if so we should use name of multipath disk name
            my $diskNameInMultiPath = $serialNumAndDiskNameOfMultiPath{getScsiIdFromSystem($devPath)};
            if ($diskNameInMultiPath ne "") {
                $newDevList{"$seri"} = $diskNameInMultiPath;
                $log->debug("key:$seri, value:$diskNameInMultiPath");
            } else {
                $newDevList{"$seri"} = $devName;
                $log->debug("key:$seri, value:$devName");
            }
        }
    }
    return %newDevList;
}

# return: Common::typeDefinition::rawInfoList
sub getRawInfoList {
    my ($systemRawDiskDir, @rawNames) = (@_);

    my @devList;

    foreach my $rawName (@rawNames) {

        #PCIE/SSD/SATA
        my $devType = get_dev_type($rawName);
        my $appType = getDiskFormatAndNoticeToLink("$systemRawDiskDir/$rawName", $devType);
        my $diskSize = getDiskSize("$systemRawDiskDir/$rawName");

        $log->debug("rawName:[$rawName], devType:[$devType], appType:[$appType], diskSize:[$diskSize].");
        if ($appType eq APP_UNFORMAT) {
            my %devMap = (
                rawName  => $rawName,
                devType  => $devType,
                appType  => $appType,
                diskSize => $diskSize,
            );

            push @devList, \%devMap;
        }
    }

    return (\@devList);
}

# return: Common::typeDefinition::rawInfoList
sub getRawInfoByPluginList {
    my ($pluginList) = (@_);
    my @devList;

    for (my $index = 0; $index < @{$pluginList}; $index++) {
        my $serialNum = $$pluginList[$index]->{serialNum};
        my $devName = $$pluginList[$index]->{devName};
        my $rawName = $$pluginList[$index]->{rawName};

        my $devType = get_dev_type($rawName);
        my $appType = getDiskFormatAndNoticeToLink("/dev/raw/$rawName", $devType);
        my $diskSize = getDiskSize("/dev/raw/$rawName");

        $log->debug("rawName: $rawName, devName:$devName, serialNum:$serialNum, devType: $devType, appType: $appType, diskSize:$diskSize");

        my %devMap = (
            rawName   => $rawName,
            devType   => $devType,
            appType   => $appType,
            diskSize  => $diskSize,
            devName   => $devName,
            serialNum => $serialNum,
        );

        push @devList, \%devMap;
    }

    return (\@devList);
}

sub remainedDiskInfoInPluginList {
    my ($diskInfoList) = (@_);

    for (my $index = 0; $index < @{$diskInfoList}; $index++) {
        my $rawName   = $$diskInfoList[$index]->{rawName};
        my $devName   = $$diskInfoList[$index]->{devName};
        my $devType   = $$diskInfoList[$index]->{devType};
        my $appType   = $$diskInfoList[$index]->{appType};
        my $diskSize  = $$diskInfoList[$index]->{diskSize};
        my $serialNum = $$diskInfoList[$index]->{serialNum};

        $log->debug("$index: remained Disk:$index: rawName: $rawName, devName:$devName, serialNum:$serialNum, devType: $devType, appType: $appType, diskSize:$diskSize");
    }
}

# the function get remove raw disk
# parameter 1 is old disk serial number  list
# parameter 2 is new disk serial list
# return: Common::typeDefinition::oldDevList
sub getPlugoutRawDisks{
    my ($oldDevList, $newDevList) = (@_);
    my %removeRawDiskMap;

    for my $serialNum (keys %{$oldDevList}) {
        if (!exists(${$newDevList}{$serialNum})) {
            my $rawName = ${$oldDevList}{$serialNum};
            $removeRawDiskMap{$serialNum} = $rawName;
        }
    }

    return %removeRawDiskMap;
}

# the function remove raw disk and remove file line
# parameter 1 is 60rule path
# parameter 2 is remove disk list, Common::typeDefinition::oldDevList
sub removeRawDiskAndFileLineInfo{
    my (%removeRawDiskList) = (@_);

    while (my ($serialNum, $rawName) = each %removeRawDiskList) {
        unrawRawDisk($rawName);

        removeLineIfContainSerialNum($sixZeroruleFilepath, $serialNum);
        removeLineIfContainWord($sixZeroruleFilepath, $rawName);

        removeLineIfContainWord($ssdRecordFile, $rawName);

        $log->debug("remove the record: serial:[$serialNum], rawName:[$rawName], from ssd record and 60rule file");
    }
}

sub unrawRawDisk {
    my ($rawName) = @_;

    my $rawCmdPath = getRawCmdPathAccordingOSVersion();

    $log->debug("starting to call raw to unraw rawName:[$rawName].");
    my $cmd = "$rawCmdPath /dev/raw/$rawName 0 0";
    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);
    $log->debug("unmap the raw device:[$rawName], result code:[$cmdRetCode].");
}

sub getRawCmdPathAccordingOSVersion {
    my $binFolderName = "";

    if (osversion::is_ubuntu()) {
        $binFolderName = "sbin";
    } elsif (osversion::is_redhat() || osversion::is_centos()) {
        $binFolderName = "bin";
    } else {
        exitProcess(__FILE__, __LINE__, "not support, is not ubuntu, redhat, centos");
        exit 1;
    }

    my $rawCmdPath = "/$binFolderName/raw";
    return $rawCmdPath;
}

# the function remove DataNode Link
# parameter 1 is system raw disk dir
# parameter 2 is remove disk rawName list
sub unlinkDataNode {
    my ($systemRawDiskDir, @removedRawNames) = (@_);

    $log->warn("unlinkDataNode systemRawDiskDir:$systemRawDiskDir, removedRawNames:[@removedRawNames].");
    my @folderPaths = getAllDatanodeDiskFolderPaths();

    foreach my $rawName (@removedRawNames) {
        my $rawPath = "$systemRawDiskDir/$rawName";
        $log->warn("starting to unlink raw:$rawPath");

        foreach my $folderPath (@folderPaths) {
            my $cmd = "ls -la $folderPath 2>&1";
            my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);

            if (($cmdRetCode != 0) || (scalar(@cmdRetLines) <= 0)) {
                next;
            }

            foreach my $linkLine (@cmdRetLines) {
                if ($linkLine =~ /(\w+)\s*\-\>\s*$rawPath/) {
                    my $linkName = $1;

                    my $removeLinkCmd = "rm -rf  $folderPath/$linkName";
                    $log->warn("remove link file:$removeLinkCmd");
                    execCmdAndGetRetLines($removeLinkCmd);
                }
            }
        }
    }
}

# write lines into file with append mode
# will append \n to each line
sub appendFileLines {
    my ($filepath, @contentLines) = @_;

    $log->debug("try to write content:[@contentLines] to file:[$filepath].");

    open(FILEHANDLER, ">>$filepath");
    foreach my $contentLine (@contentLines) {
        my $line = undef;
        if ($contentLine =~ /\n$/) {
            $line = $contentLine;
        } else {
            $line = "$contentLine\n";
        }

        $log->debug("append line:[$line] to file:[$filepath].");
        print FILEHANDLER "$line";
    }

    close(FILEHANDLER);
    $log->debug("close file:[$filepath].");
}

# the function get plugin disk
# parameter 1 is raw  N max num
# parameter 2 is new dev  disk list
# parameter 3 is old dev disk list
# parameter 4 is the data node properties file
# return: Common::typeDefinition::existDiskList
sub getPluginRawDisks{
    my ($lastRawNum, $newDevList, $oldDevList) = (@_);

    my @pluginList;
    for my $serialNum (keys %{$newDevList}) {
        if (!exists($oldDevList->{$serialNum})) {
            $lastRawNum++;
            my $rawName = RAW_NAME."$lastRawNum";
            my $devName = ${$newDevList}{$serialNum};

            my %map = (
                serialNum => $serialNum,
                devName   => $devName,
                rawName   => $rawName
            );

            push @pluginList, \%map;
            $log->warn("get an plugin disk serialNum:[$serialNum] devName:[$devName] generated rawName:[$rawName].");
        }
    }

    return \@pluginList;
}


###Get raw disk exist since last time start
# return: Common::typeDefinition::existDiskList
sub getExistRawDisks{
    my ($newDevList, $oldDevList) = (@_);

    my @existDiskList;
    for my $serialNum (keys %{$newDevList}) {
        if (exists $oldDevList->{$serialNum}) {
            my $devName = ${$newDevList}{$serialNum};
            my $rawName = ${$oldDevList}{$serialNum};

            my %map = (
                serialNum => $serialNum,
                devName   => $devName,
                rawName   => $rawName
            );

            push @existDiskList, \%map;
            $log->debug("get an exist disk serialNum:[$serialNum] devName:[$devName] rawName:[$rawName].");
        }
    }
    return \@existDiskList;
}

# generate one line that match 60rule file pattern
sub generateOne60RuleLine{
    my ($rawName, $serialNum, $devName) = (@_);
    my $action = "";
    my $bin = "";
    my $isUbuntu = osversion::is_ubuntu();
    my $isCentOSOrRedHat = osversion::is_redhat() || osversion::is_centos();

    if ($isUbuntu) {
        $action = "change";
        $bin = "sbin";
    } elsif ($isCentOSOrRedHat) {
        $action = "add";
        $bin = "bin";
    } else {
        exitProcess(__FILE__, __LINE__, "it cannot happen, not ubuntu, centos, redhat");
        exit 1;
    }

    my $line = "ACTION==\"$action\",KERNEL==\"$devName\",PROGRAM==\"/lib/udev/scsi_id -g -u /dev/%k\",RESULT==\"$serialNum\",RUN+=\"/$bin/raw /dev/raw/$rawName %N\" ";
    $log->debug("generate one 60 rule line:[$line].");

    return $line;
}

# the function exist file system type flag
# parameter 1 is devname
# parameter 2 is devname list
sub hasFileSystemTypeRawDiskMounted {
    my ($devName, %devMap) = (@_);
    my $flag = - 1;
    my $tmpMountPath = "/tmp/tmpMount";
    if (exists $devMap{$devName}) {
        if ($devMap{$devName} ne "null") {
            $log->warn("device:$devName exist $devMap{$devName} partition information");
        } else {
            $flag = checkMount("/dev/$devName", $tmpMountPath);
        }
    }

    if ($flag == 0) {
        return 0;
    } else {
        return 1;
    }
}
# the function check dev mounted
# parameter 1 is devname
# parameter 2 is mount point
sub checkMount {
    my ($devName, $tmpMountPath) = (@_);
    my $flag = "false";

    unless (-e $tmpMountPath) {
        make_path($tmpMountPath);
    }

    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines("mount $devName $tmpMountPath 2>&1");

    if ($cmdRetCode == 0) {
        $log->debug("mount $devName $tmpMountPath success");
        $flag = "true";
    } else {
        $log->debug("mount $devName $tmpMountPath failure");
        $flag = "false";
    }

    $log->debug("mounted flag:$flag");
    if ($flag eq "true") {
        $cmd = `umount $tmpMountPath 2>&1`;
        $log->warn("umount $tmpMountPath after mounting success");
        remove_tree($tmpMountPath);
        return 1;
    } else {
        $cmd = `umount $tmpMountPath 2>&1`;
        $log->warn("umount $tmpMountPath after mounting failure");
        remove_tree($tmpMountPath);
        return 0;
    }
}

# the function datanode link dev/raw/rawN
# parameter 1 is dev/raw/rawN
# parameter 2 is system raw disk path
# parameter 3 is datanode path
# parameter 4 is link type
# 0 for sata disk, 1 for ssd, 2 for pcie.
sub linkDataNode {
    my ($rawName, $systemRawDiskDir, $datanodeRawDiskDir, $devType) = (@_);
    my $rawDevPath = "$systemRawDiskDir/$rawName";

    $log->debug("link raw:$rawName from $systemRawDiskDir to $datanodeRawDiskDir, type:$devType");

    my $rawConstant = RAW_NAME;
    my $linkFileName = $rawName;
    $linkFileName =~ s/$rawConstant/$devType/g;

    my $linkFilePath = "$datanodeRawDiskDir/$linkFileName";
    if (-e $linkFilePath) {
        execCmdAndGetRetLines("rm -rf  $linkFilePath 2>&1");
        $log->warn("delete link file:$linkFilePath.");
    }

    execCmdAndGetRetLines("ln -s $rawDevPath $linkFilePath 2>&1");
    $log->debug("link cmd:ln -s $rawDevPath $linkFilePath.");
}

sub linkDiskByAppType {
    my ($rawName, $systemRawDiskDir, $linkFolderPath, $devType, @appTypes) = (@_);

    $log->debug("link raw:$rawName from $systemRawDiskDir to $linkFolderPath, type:$devType");

    my $linkFileName = generateLinkName($rawName, $devType, @appTypes);

    if (!defined($linkFileName)) {
        $log->error("link raw:$rawName from $systemRawDiskDir to $linkFolderPath, type:$devType failed. Can't generate link name.");
        return;
    }

    my $linkFilePath = "$linkFolderPath/$linkFileName";
    if (-e $linkFilePath) {
        execCmdAndGetRetLines("rm -rf  $linkFilePath 2>&1");
        $log->warn("delete link file:$linkFilePath.");
    }

    my $rawDevPath = "$systemRawDiskDir/$rawName";
    execCmdAndGetRetLines("ln -s $rawDevPath $linkFilePath 2>&1");
    $log->debug("link cmd:ln -s $rawDevPath $linkFilePath.");
}

sub generateLinkName {
    my ($rawName, $devType, @appTypes) = @_;
    $log->debug("Generate link name by rawName[$rawName] devType[$devType] appType[".listToStr(@appTypes)."].");

    my $rawN = undef;
    if ($rawName =~ /(\d+)$/) {
        $rawN = $1;
    } else {
        $log->error("[$rawName] does not has number in it.");
        return undef;
    }

    my $linkPattern = undef;

    $linkPattern = convertAppTypeToLinkPattern($appTypes[0]);
    my $linkName = undef;
    if ($linkPattern) {
        $linkName = $linkPattern.LINK_NAME_SEPARATOR.$devType.$rawN;
    } else {
        $linkName = $devType.$rawN;
    }

    $log->debug("Generate link name by rawName[$rawName] devType[$devType] appType[".listToStr(@appTypes)."] linkName[$linkName].");

    return $linkName;
}

sub listToStr {
    my (@list) = @_;

    if (scalar(@list) <= 0) {
        return "";
    }

    my $str = $list[0];
    for (my $index = 1; $index < scalar(@list); $index++) {
        $str .= " ".$list[$index];
    }

    return $str;
}

# the function check datanode raw disk dir link status
# parameter 1 is datanode raw disk path
# parameter 2 link name to raw device
sub checkDataNodeRawDiskDirLink {
    my ($linkFilePath, $rawName) = (@_);

    my $cmd = `ls -la $linkFilePath 2>&1`;
    $log->debug("ls -la $linkFilePath result: $cmd");

    my $rawCmd = `raw -qa 2>&1`;
    $log->debug("cmd:raw -qa, result: $rawCmd");

    my $listDevCmd = `ls -la /dev/raw 2>&1`;
    $log->debug("cmd:ls -la /dev/raw, result: $listDevCmd");

    if ($cmd =~ /$rawName/) {
        if ($cmd =~ /$rawName \-\> \/dev\/raw/) {
            $log->debug("exist file:$rawName and  link");
            return 1;
        } else {
            $log->debug("exist file:$rawName and not link");
            #remove dir
            return 0;
        }
    } else {
        #no exist file in directory
        $log->warn("not exist file:$rawName");
        return - 1;
    }
}

sub checkExistDiskLink {
    my ($dirPath, $key) = (@_);

    my $cmd = `ls -la $dirPath 2>&1`;

    if ($cmd =~ $key) {
        return 1;
    }
    else {
        return 0;
    }
}

# check if specific appType disk, for example,
sub checkDiskLinkExistByAppType {
    my ($appType) = @_;

    my $folder = findFolderPathByAppType($appType);
    my $linkNamePattern = convertAppTypeToLinkPattern($appType);

    if (!$linkNamePattern) {
        $log->error("can't check disk link exist for appType:[$appType].");
        return 0;
    }

    my @filenames = getAllFilenamesFromFolder($folder);
    foreach my $filename (@filenames) {
        if ($filename =~ /$linkNamePattern/) {
            $log->debug("Find link file[$filename] in folder[$folder] for appType[$appType] link pattern[$linkNamePattern].");
            return 1;
        }
    }

    $log->debug("Can't find link file in folder[$folder] for appType[$appType] link pattern[$linkNamePattern].");
    return 0;
}

# query link pattern by appType
sub convertAppTypeToLinkPattern {
    my ($appType) = @_;

    my $linkNamePattern = undef;

    if ($appType eq APP_RAW) {
        $linkNamePattern = "";
    } elsif ($appType eq APP_UNSET) {
        $linkNamePattern = "";
    } else {
        # will not happen
        $log->error("Unknown appType[$appType] can't find link pattern for it.");
        die("ERROR: Unknown appType[$appType] can't find link pattern for it.");
        $linkNamePattern = undef;
    }

    if (defined($linkNamePattern)) {
        $log->debug("Convert appType:[$appType] to linkNamePattern:[$linkNamePattern].")
    } else {
        $log->debug("Convert appType:[$appType] to linkNamePattern failed.")
    }

    return $linkNamePattern;
}

sub pluginAckMsg {
    my ($rawName, $link_type, $subDir) = (@_);

    my $rawPath = "/dev/raw/$rawName";
    my $folderFullPath = "$storageDir/$subDir";
    my $linkName = findLinkNameByOriginName($rawPath, $folderFullPath);
    if (!defined($linkName)) {
        $log->error("Can't find link name for $rawPath in $folderFullPath, Can't print ack msg to java program.");
        return;
    }

    my $pluginAckMsgFormat = getArchivePluginMatcher();

    printf($pluginAckMsgFormat, $linkName, $subDir);
    # must add the change line, otherwise the java process can not parse the previous line print.
    print "\n";

    $log->debug("pluginAckMsgFormat is [$pluginAckMsgFormat], link_name:[$linkName], subDir:[$subDir]");
}

# get the ssd device name regular expression from the datanode config file
sub getSSDReg{
    return readConfigArray("ssd.device.name.reg", ",");
}

sub getIgnoreDeviceRegs {
    return readConfigArray("ignore.device.name.reg", ",");
}

# return default value if read config failed
sub readConfigItem {
    my ($itemName, $defaultValue) = @_;

    if (exists $configCache{$itemName}) {
        my $value = $configCache{$itemName};
        if (defined($value)) {
            $log->debug("Read config hit cache, itemName:[$itemName] cachedValue:[$value].");
        } else {
            $log->debug("Read config hit cache, itemName:[$itemName] cachedValue:[undef].");
        }
        return $value;
    }

    my $itemValue = undef;
    if (exists($ConfigLocation::configLocation{$itemName})) {
        my $configFileName = $ConfigLocation::configLocation{$itemName};
        my $configFilePath = "$configFolderPath/$configFileName";
        $log->debug("config:[$itemName] should in file:[$configFileName], try to read.");
        $itemValue = readConfigItemFromFile($itemName, $configFilePath);
    } else {
        $log->error("Can't find config location in predefinition for config:[$itemName], please check config location definition.")
    }

    if ($itemValue) {
        $log->debug("Read config:[$itemName] success. Return value:[$itemValue]. Save cache.");
        $configCache{$itemName} = $itemValue;
        return $itemValue;
    } else {
        if (defined $defaultValue) {
            $log->error("Read config:[$itemName] failed. Return default value:[$defaultValue]. Save cache.");
        } else {
            $log->error("Read config:[$itemName] failed. Return default value:[undef]. Save cache.");
        }

        $configCache{$itemName} = $defaultValue;
        return $defaultValue;
    }
}

sub readConfigItemFromFile {
    my ($itemName, $filepath) = @_;

    $log->debug("Read [$filepath] config:[$itemName]");

    if (!open(CONFIGFILE, "$filepath")){
        $log->error("Open config file:[$filepath] failed. No such file.");
        return undef;
    }

    my $itemValue = undef;
    while(my $line = <CONFIGFILE>){
        if ($line =~ /^\s*$itemName\s*=\s*(.+)\s*$/) {
            $itemValue = $1;
            chomp($itemValue);
            last;
        }
    }

    close(CONFIGFILE);

    if ($itemValue) {
        $log->debug("Read [$filepath] config:[$itemName] success. Return value:[$itemValue].");
        return $itemValue;
    } else {
        $log->error("Read [$filepath] config:[$itemName] failed. Return value:[undef]");
        return undef;
    }
}

# read config that has multi value
# ssd.reg=sda,sdb,sd[c-d]
# return () if config value is empty
sub readConfigArray {
    my ($itemName, $separator) = @_;

    $log->debug("Read config array item, itemName:[$itemName] separator:[$separator].");

    my @configArray = ();
    my $itemValue = readConfigItem($itemName, undef);
    if (!$itemValue) {
        $log->debug("Read config array item failed, itemName:[$itemName] return empty.");
        return @configArray;
    }

    @configArray = split(/$separator/, $itemValue);
    $log->debug("Read config array item success, itemName:[$itemName] itemValue:[$itemValue] itemArray:[@configArray]");

    return @configArray;
}

# return map, key like raw1, value like sda
sub readSSDRecordFile {
    my @fileLines = readFileLines($ssdRecordFile);

    #ssd.record content is :
    #raw1, sda
    my %ssdRecordMap = ();
    foreach my $line (@fileLines) {
        if ($line =~ /(\w+)\s*,\s*(\w+)/) {
            my $rawName = $1;
            my $devName = $2;
            $ssdRecordMap{$rawName} = $devName;
        }
    }

    return %ssdRecordMap;
}

# return file lines as array
sub readFileLines {
    my ($filepath) = @_;

    $log->debug("Read file:[$filepath].");

    my @fileLines = ();

    if (!open(CONFIGFILE, "$filepath")){
        $log->error("Open file:[$filepath] failed. No such file.");
        return @fileLines;
    }

    while(my $line = <CONFIGFILE>){
        chomp($line);
        push(@fileLines, $line);
    }

    close(CONFIGFILE);

    return @fileLines;
}

# get the pcie device name regular expression from the datanode config file
sub getPCIEReg{
    return readConfigArray("pcie.device.name.reg", ",");
}

sub getArchiveInitMode {
    return readConfigItem("archive.init.mode", "append");
}

sub getArchivePluginMatcher {
    return readConfigItem("archive.plugin.matcher", "plugin rawName %s, archiveType %s");
}

sub listContains {
    my ($target, @list) = @_;

    if (scalar(@list) <= 0) {
        return 0;
    }

    foreach my $item (@list) {
        if ($target eq $item) {
            $log->debug("target:[$target] is in list:[@list].");
            return 1;
        }
    }

    $log->debug("target:[$target] is not in list:[@list].");
    return 0;
}

#print the remained disk information.
#input: raw disk map list. Common::typeDefinition::rawInfoList
sub printRemainedRawDiskInfo {
    my ($rawInfoList) = (@_);

    $log->debug("The info of remained raw disk as following:");
    for (my $index = 0; $index < @{$rawInfoList}; $index++) {
        my $rawName  = $$rawInfoList[$index]->{rawName};
        my $devType  = $$rawInfoList[$index]->{devType};
        my $appType  = $$rawInfoList[$index]->{appType};
        my $diskSize = $$rawInfoList[$index]->{diskSize};

        $log->debug("index:[$index]: rawName:[$rawName], devType:[$devType], appType:[$appType], diskSize:[$diskSize].");
    }
}

sub findFolderPathByAppType {
    my ($appType) = @_;

    if ($appType eq APP_RAW) {
        return "$storageDir/".APP_RAW_SUBDIR;
    } elsif ($appType eq APP_UNSET) {
        return "$storageDir/".getCleanDiskSubFolder();
    } else {
        $log->error("Unknown appType[$appType] can't find folder for it.");
        die("ERROR: Unknown appType[$appType] can't find folder for it.");
        return undef;
    }
}

#Read Archive and get storage type.
sub getDiskFormatAndNoticeToLink {
    my ($storagePath, $storageType) = (@_);

    my $firstStart = isDatanodeFirstTimeStart();
    $log->debug("archive init mode is $archiveInitMode and fisrtStart is $firstStart");
    if (($archiveInitMode =~ /overwrite/) && ($firstStart eq "true")) {
        return APP_UNFORMAT;
    }

    my $diskType = undef;

    $log->debug("getDiskType starting, path:$storagePath storageType:$storageType");

    my $envRoot = $myEnvRoot;

    my $cmd = "java -noverify -cp :$envRoot/build/jar/*:$envRoot/lib/*:$envRoot/config py.datanode.ArchiveTypeQueryAndLink --storagePath $storagePath --storageType $storageType";
    $log->debug("user command is $cmd");
    system($cmd);

    $log->debug("getDiskType finished, path:$storagePath");

    #the  return code only 2Byte.So the return code defined should less than 2Byte.
    my $value = 0;
    if ($? == - 1) {
        $value = $!;
        $log->debug("return value:$!, execute unsuccessfully");
    } else {
        $value = $? >> 8;
        $log->debug("return value:$?, format status get successfully, statu code: $value(200:foramt OK, 201:format unknown.)");
    }

    if ($value == 200) {
        $diskType = APP_KNOWN;
    } else {
        $diskType = APP_UNFORMAT;
    }

    $log->debug("finished:diskType is $diskType");

    return $diskType;
}


#delete device from ssd.record
sub delDevFromSSDRecord {
    my ($devName, $recordPath) = (@_);

    my $in;
    my $out;
    open($in, "$recordPath/ssd.record") or die("ERROR:No such file $recordPath/ssd.record");
    open($out, ">$recordPath/ssd.record.bat") or die("ERROR:No such file $recordPath/ssd.record.bat");

    while ( <$in> ) {
        s/.*$devName\n$//g;
        print $out $_;
    }

    close $out;
    close($in);

    system("mv $recordPath/ssd.record.bat $recordPath/ssd.record");
}

# devPath maybe /dev/sdb or /dev/raw/raw1
# return: get size failed will return -1
sub getDiskSize {
    my ($devPath) = (@_);

    $log->debug("Try to get disk size for [$devPath]");
    my $blockDevCmd = "blockdev --getsize64 $devPath";
    my ($retCode, @retMsgLines) = execCmdAndGetRetLines($blockDevCmd);
    if (scalar(@retMsgLines) > 0) {
        my $diskSize = $retMsgLines[0];
        chomp($diskSize);
        $log->debug("get device[$devPath] size[$diskSize] by blockdev success");
        return $diskSize;
    }

    # use lsblk to get disk size
    #lsblk -i -b -n -p -o size /dev/sdb
    #35748052992
    my $devNamePath = mapRawPathToDevNamePath($devPath);
    my $lsblkCmd = "lsblk -i -b -n -p -o size $devNamePath";
    ($retCode, @retMsgLines) = execCmdAndGetRetLines($lsblkCmd);
    if (scalar(@retMsgLines) > 0) {
        my $diskSize = $retMsgLines[0];
        chomp($diskSize);
        $log->debug("get device[$devPath] devNamePath[$devNamePath] size[$diskSize] by lsblk success");
        return $diskSize;
    }

    # add debug info
    $log->debug("get device[$devPath] size failed. Show debug info.");
    execCmdAndGetRetLines("lsblk -i");
    execCmdAndGetRetLines("raw -aq");
    return "-1";
}

# map /dev/raw/raw1 to /dev/sdb
sub mapRawPathToDevNamePath {
    my ($rawDevPath) = (@_);
    $log->debug("Try to get [$rawDevPath] devName");

    if (!($rawDevPath =~ /\/dev\/raw\/raw/)) {
        # path not match pattern, return origin path
        $log->debug("[$rawDevPath] not starts with /dev/raw, return origin path [$rawDevPath]");
        return $rawDevPath;
    }

    # get major and minor number
    #raw -q /dev/raw/raw1
    #/dev/raw/raw1:  bound to major 8, minor 0
    my $rawQueryCmd = "raw -q $rawDevPath";
    my ($retCode, @retMsgLines) = execCmdAndGetRetLines($rawQueryCmd);

    if (scalar(@retMsgLines) == 0
        || !($retMsgLines[0] =~ /major (\d+), minor (\d+)/) ) {
        $log->debug("Get Major and minor failed, return origin path [$rawDevPath]");
        return $rawDevPath;
    }

    my $majorNum = $1;
    my $minorNum = $2;
    $log->debug("get device info: major:[$majorNum] minor:[$minorNum]");

    #lsblk -i -b -n -p -P -o name,maj:min
    #NAME="/dev/sda" MAJ:MIN="8:0"
    my $lsblkMajMinCmd = "lsblk -i -b -n -p -P -o name,maj:min";
    ($retCode, @retMsgLines) = execCmdAndGetRetLines($lsblkMajMinCmd);
    if (scalar(@retMsgLines) == 0) {
        $log->debug("Exec cmd:[$lsblkMajMinCmd] failed, return origin path [$rawDevPath]");
        return $rawDevPath;
    }

    foreach my $line (@retMsgLines) {
        if ($line =~ /NAME="(.+)"\s*MAJ:MIN="$majorNum:$minorNum"/i) {
            my $devNamePath = $1;
            $log->debug("Map [$rawDevPath] to [$devNamePath] success.");
            return $devNamePath;
        }
    }

    $log->debug("Find devName path failed for [$rawDevPath], return origin path [$rawDevPath]");
    return $rawDevPath;
}

# map raw1 to sdb by ssd.record
# return undef if raw1 not in ssd.record
sub mapRawNameToDevNameBySSDRecord {
    my ($rawName) = (@_);
    $log->debug("Try to get [$rawName] devName by ssd.record");

    my %ssdRecordMap = readSSDRecordFile();

    if (exists($ssdRecordMap{$rawName})) {
        my $devName = $ssdRecordMap{$rawName};
        $log->debug("Get devName:[$devName] for rawName:[$rawName] from ssd.record success.");
        return $devName;
    } else {
        $log->debug("Can't get devName for rawName:[$rawName] from ssd.record.");
        return undef;
    }
}

# return line array
# return () if cmd exec failed or there is no out put
sub execCmdAndGetRetLines {
    my ($cmd) = @_;
    my $retMsg = qx($cmd);
    my $retCode = $? >> 8;
    $log->debug("Exec cmd:[$cmd] retCode:[$retCode] retMsg:[$retMsg]");

    if ($retCode != 0) {
        $log->error("Exec cmd:[$cmd] failed. retCode:[$retCode] retMsg:[$retMsg]");
    }

    my @lines = split(/[\r\n]/, $retMsg);
    return $retCode, @lines;
}

# the function get raw max num
# parameter 1 raw disk path path
sub getMaxNFromDatanodeFolders{
    my @filenames = ();
    foreach my $subFolder (getAllDatanodeDiskSubFolders()) {
        push(@filenames, getAllFilenamesFromFolder("$storageDir/$subFolder"));
    }

    return getMaxRawNumFromFileNameList(@filenames);
}

sub getAllFilenamesFromFolder {
    my ($folder) = @_;

    my @filenames = ();
    if (!opendir(SYS_STORAGE_DIR, $folder)) {
        $log->error("Can't open dir:[$folder].");
        return @filenames;
    }

    foreach my $filename (readdir(SYS_STORAGE_DIR)) {
        if ($filename eq "." || $filename eq "..") {
            next;
        }
        push(@filenames, $filename);
    }
    closedir(SYS_STORAGE_DIR);

    $log->debug("get all filenames from folder:[$folder] filenames:[@filenames].");
    return @filenames;
}

sub getAllRawNamesInFolder {
    my ($folder) = (@_);

    my @rawNames = ();
    foreach my $filename (getAllFilenamesFromFolder($folder)) {
        if ($filename =~ /^raw\d+$/) {
            push(@rawNames, $filename);
        }
    }

    return @rawNames;
}

sub is_pcie{
    my ($rawName) = @_;

    $log->debug("Check rawName:[$rawName] whether pcie disk.");

    my $devName = mapRawNameToDevNameBySSDRecord($rawName);
    if (!$devName) {
        $log->debug("Check rawName:[$rawName] is not ssd according to ssd.record, so it isn't pcie disk.");
        return 0;
    }

    if (is_pcie_according_to_dev_name($devName)) {
        $log->debug("rawName:[$rawName] devName:[$devName] is pcie.");
        return 1;
    } else {
        $log->debug("rawName:[$rawName] devName:[$devName] isn't pcie.");
        return 0;
    }
}

sub get_dev_type {
    my ($rawName) = (@_);

    my $is_ssd = is_ssd($rawName);
    my $is_pcie = is_pcie($rawName);
    my $devType = undef;

    if ($is_pcie) {
        $devType = PCIE_NAME;
    } elsif ($is_ssd) {
        $devType = SSD_NAME;
    } else {
        $devType = SATA_NAME;
    }

    return $devType;
}

sub rawDiskArchiveInit {
    my ($linkFileList, $firstStart, $subDir) = (@_);

    $log->debug("ArchivesInit: rawFileList: @$linkFileList, subDir:$subDir");
    my $folderPath = "$storageDir/$subDir";

    foreach my $linkName (@$linkFileList) {
        my $linkFilePath = "$folderPath/$linkName";
        if (!(-e $linkFilePath)) {
            next;
        }

        my $rawName = link_name_to_raw_name($linkName);
        my $devType = get_dev_type($rawName);

        my ($devName, $serialNum) = getDevNameAndSerialByRawNameFrom60rule($rawName);

        if (!defined($devName) || !defined($serialNum)) {
            $log->error("Can't get devName serialNum by rawName:[$rawName] from 60rule file.");
            next;
        }

        $log->debug("==ArchivesInit: lineName:$linkName====rawName:$rawName");

        $log->debug("Raw disk init start: serialNum:[$serialNum], linkName:$subDir/$linkName, devName:[$devName],"
            ." rawName:[$rawName], envRoot:[$myEnvRoot], devType:[$devType] subDir:[$subDir].");
        runArchiveJavaProgram($myEnvRoot, $firstStart, $serialNum, "$subDir/$linkName", $devName, "false",
            $devType, $subDir);
    }
}

sub archiveInitByApplication {
    my ($firstStart, $subDir) = (@_);

    my $folderPath = "$storageDir/$subDir";

    my @fileList = scanFiles($folderPath);
    if (scalar(@fileList) <= 0) {
        $log->warn("There isn't any raw file in DIR $folderPath");
        return;
    }

    $log->debug("archiveInitByApplication begin: subDir:$subDir, fileList as: @fileList");

    rawDiskArchiveInit(\@fileList, $firstStart, $subDir);
}

sub link_name_to_raw_name{
    my ($linkName) = (@_);

    if ($linkName =~ /(\d+)$/) {
        return RAW_NAME.$1;
    } else {
        return undef;
    }
}

# parameter 1 device name, the method will only read the ssd.record file to check the device is ssd device.
sub is_ssd{
    my ($rawName) = (@_);

    my $devName = mapRawNameToDevNameBySSDRecord($rawName);
    if (defined($devName)) {
        $log->debug("rawName:[$rawName] is ssd according to ssd.record.");
        return 1;
    } else {
        $log->debug("rawName:[$rawName] is not ssd according to ssd.record.");
        return 0;
    }
}

# parameter 1 device path 
# parameter 2 is ssd regular expression
sub is_ssd_according_to_dev_name{
    my ($dev_path) = @_;

    my @ssdRegArray = getSSDReg();
    if (scalar(@ssdRegArray) <= 0) {
        $log->debug("ssd reg config is empty, can't check whether disk is ssd according to its name.");
        return 0;
    }

    $log->debug("check ssd storage path:[$dev_path], ssd regular:[@ssdRegArray]");

    foreach my $reg (@ssdRegArray) {
        chomp($reg);
        $log->debug("regular:$reg, device:$dev_path");
        if ($dev_path =~ /$reg/) {
            $log->info("this is a ssd:$dev_path");
            return 1;
        }
    }

    return 0;
}

sub getSsdMinThreshold {
    return readConfigItem("ssd.speed.min.threshold", 1000);
}

# parameter 1 device path
# parameter 2 the time for checking the speed of the storage, unit: second
# parameter 3 is ssd regular
sub check_if_is_ssd {
    my ($storage_path, $time_second) = @_;

    if (is_ssd_according_to_dev_name($storage_path)) {
        $log->debug("this is a ssd according device name:$storage_path");
        return 1;
    }

    #$result = is_ssd_according_hdparm($storage_path);
    #if ($result == 1) {
    #    log->debug("this is a ssd according hdparm for storage path:$storage_path");
    #    return $result;
    #}

    if (isSSDDetectBySpeed() eq "false") {
        $log->debug("this is not ssd devName:[$storage_path], because detect ssd by speed switch is off.");
        return 0;
    }

    if (is_ssd_according_speed($storage_path, $time_second)) {
        $log->info("this is a ssd according speed for storage path:$storage_path");
        return 1;
    } else {
        $log->info("this is not a ssd according speed for storage path:$storage_path");
        return 0;
    }
}

#parameter 1 device path
sub is_ssd_according_hdparm {
    if (length(`hdparm -I $dev_path | grep "Solid State Device"`)) {
        return 1;
    } else {
        return 0;
    }
}

# parameter 1 device full path
# parameter 2 last time for check the speed of the storage, unit:second
sub is_ssd_according_speed {
    my ($devPath, $timeSecond) = (@_);
    my $envRoot = get_actual_environment_root();
    my $minSpeedThreshold = getSsdMinThreshold();

    $log->debug("the SSD disk min speed threshold is $minSpeedThreshold");

    my $cmd = "java -noverify -cp :$envRoot/build/jar/*:$envRoot/lib/*:$envRoot/config py.datanode.StorageDetect --storagepath $devPath --timesecond $timeSecond --threshold $minSpeedThreshold";

    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);

    if ($cmdRetCode == 100) {
        $log->debug("this is a normal disk for $devPath");
        return 0;
    } elsif ($cmdRetCode == 101) {
        $log->debug("this is a ssd disk for $devPath");
        return 1;
    } else {
        $log->error("can not detect the storage:$devPath type, retCode: $cmdRetCode");
        saveRollbackFile(basename($devPath));
        return 0;
    }
}

sub is_pcie_according_to_dev_name{
    my ($devName) = @_;

    $log->debug("Check devName:[$devName] whether is pcie by pcie reg in config.");
    my @pcieRegArray = getPCIEReg();

    foreach my $reg (@pcieRegArray) {
        chomp($reg);
        if ($devName =~ /$reg/) {
            $log->debug("devName:[$devName] is pcie by pcie reg:[$reg] in config.");
            return 1;
        }
    }

    $log->debug("devName:[$devName] is not pcie according to pcie reg in config.");
    return 0;
}

sub touchFileIfNotExist {
    my ($filepath) = @_;

    $log->debug("Try to touch file:[$filepath] if it not exist.");

    if (-e $filepath) {
        $log->debug("File:[$filepath] exist, don't touch it.");
        return;
    }

    my $cmd = "touch $filepath";
    $log->warn("starting touch a file:[$filepath].");
    execCmdAndGetRetLines($cmd);
    $log->warn("finished touch a file:[$filepath].");
}

sub removeIfExist {
    my ($filepath) = @_;

    $log->debug("Try to remove [$filepath] if exist.");

    if (not -e $filepath) {
        $log->debug("[$filepath] not exist. Don't need remove.");
        return;
    }

    my $cmd = "rm $filepath";
    $log->warn("starting rm a file:[$filepath].");
    execCmdAndGetRetLines($cmd);
    $log->warn("finished rm a file:[$filepath].");
}

sub mountThenUpdateRuleAndSSDFile {
    my (@devList) = @_;

    $log->debug("Try to mount raw disk then update rule file and ssd record file.");

    my @ssdRecordContents = ();
    my @ruleContents = ();

    ### check the os and use different command to raw disk
    my $rawCmdPath = getRawCmdPathAccordingOSVersion();

    for (my $index = 0; $index < @devList; $index++) {
        my $serialNum = $devList[$index]->{serialNum};
        my $devName   = $devList[$index]->{devName};
        my $rawName   = $devList[$index]->{rawName};
        $log->debug("call raw program to map serialNum:[$serialNum], devName:[$devName], rawName:[$rawName]");

        my $devPath = "/dev/$devName";
        my $cmd = "$rawCmdPath /dev/raw/$rawName $devPath";
        my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);
        if ($cmdRetCode != 0) {
            $log->error("exec raw command failed, just ignore devName:[$devName] rawName:[$rawName].");
            saveRollbackFile($devName);
        }

        my $one60RuleLine = generateOne60RuleLine($rawName, $serialNum, $devName);
        push(@ruleContents, $one60RuleLine);

        if (checkExistInSsdRecordFile($rawName)) {
            next;
        }

        my $isSSD = check_if_is_ssd($devPath, 3);
        my $isPCIE = is_pcie_according_to_dev_name($devPath);
        $log->debug("device name:[$devPath], ssd:[$isSSD], pcie:[$isPCIE]");

        # the pcie card and SSD should be considered as SSD
        if ($isSSD || $isPCIE) {
            my $ssdContentLine = "$rawName, $devName\n";
            push(@ssdRecordContents, $ssdContentLine);
            $log->debug("insert to ssd record file, content:[$ssdContentLine]");
        }
    }

    $log->debug("overwrite save 60 rule file:[$sixZeroruleFilepath].");
    appendFileLines($sixZeroruleFilepath, @ruleContents);
    $log->debug("finished to write rule file");

    $log->debug("append save ssd record file:[$ssdRecordFile]");
    appendFileLines($ssdRecordFile, @ssdRecordContents);
    $log->debug("finished to write ssd record file");
}

# some disk may init failed, like raw command fail, java program init archive fail
# or disk like fd0 can't read
# when disk init fail, record it in rollback file, then at last will
sub saveRollbackFile {
    my ($devName) = @_;

    if ($devName) {
        $log->debug("Save devName:[$devName] to rollback file:[$rollbackFilepath].");
        appendFileLines($rollbackFilepath, ($devName));
    } else {
        $log->debug("devName is empty, don't save to rollback file.");
    }
}

sub checkIsInRollbackFile {
    my ($targetDevName) = @_;

    $log->debug("Check whether devName:[$targetDevName] is in rollback file.");

    my @devNames = readFileLines($rollbackFilepath);
    if (listContains($targetDevName, @devNames)) {
        $log->debug("devName:[$targetDevName] is in rollback file.");
        return 1;
    } else {
        $log->debug("devName:[$targetDevName] is not in rollback file.");
        return 0;
    }
}

# some disk may init failed, when fail just record the device, and this function will remove cached disk info
sub deleteDeviceByRollbackFile {
    $log->debug("Try to rollback init failed disks.");

    my @devNames = readFileLines($rollbackFilepath);
    foreach my $devName (@devNames) {
        $log->debug("Try to rollback by devName:[$devName].");
        my ($tmp, $serialNum, $rawName) = queryDevInfoFrom60ruleFile($devName, undef, undef);

        if ($serialNum && $rawName) {
            $log->debug("rollback by devName:[$devName] serialNum:[$serialNum] rawName:[$rawName].");
            unlinkDataNode("/dev/raw", ($rawName));
            removeRawDiskAndFileLineInfo(($serialNum => $rawName));
            $log->debug("rollback by devName:[$devName] success.");
        } else {
            $log->error("Query devInfo from 60rule by devName:[$devName] failed. rollback failed. Please check.");
        }
    }

    removeIfExist($rollbackFilepath);
    $log->debug("Rollback init failed disks finished.");
}

sub isDatanodeFirstTimeStart {
    my $firstStart = undef;
    if (-e $dataNodeStartTime) {
        $firstStart = "false";
    } else {
        $firstStart = "true";
    }

    return $firstStart;
}

sub getAllDatanodeDiskSubFolders {
    return (APP_RAW_SUBDIR, getCleanDiskSubFolder());
}

sub getAllDatanodeDiskFolderPaths {
    my @paths = ();
    foreach my $subFolder (getAllDatanodeDiskSubFolders()) {
        push(@paths, "$storageDir/$subFolder");
    }

    return @paths;
}

sub runPythonScriptAndExit {
    my ($cmd) = @_;

    $log->info("switch for use python script is on, so please goto see python.log");
    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);
    foreach my $line (@cmdRetLines) {
        chomp($line);
        print("$line\n");
    }

    exit($cmdRetCode)
}

sub isSSDDetectBySpeed {
    return readConfigItem("ssd.detect.by.speed", "true");
}

sub getCleanDiskSubFolder {
    return readConfigItem("unset.archive.directory", "unsetDisks");
}

sub getAutoDistributeDiskUsageSwitch {
    return readConfigItem("enable.auto.distribute.disk.usage", "true");
}

sub getUsePythonScript {
    return readConfigItem("use.python.script", "true");
}

# type definition
# Common::typeDefinition::oldDevList
#     oldDevList{key:serialNum}=value:rawName, serialsNum like 350014ee65ad6e8b2, rawName like raw1
# Common::typeDefinition::newDevList
#     newDevList{key:serialNum}=value:devName, serialsNum like 350014ee65ad6e8b2, devName like sda
# Common::typeDefinition::existDiskList
#     existDiskList array(map1, map2)
#         map = (
#             serialNum => "350014ee65ad6e8b2",
#             devName   => "sda",
#             rawNum    => "raw1"
#         )
# Common::typeDefinition::rawInfoList
#     rawInfoList array(map1, map2)
#         map = (
#             rawName  => "raw1",
#             devType  => enum(pcie, ssd, sata)
#             appType  => enum(APP_UNKNOWN, APP_KNOWN)
#             diskSize => 2000398934016
#         )
# Common::typeDefinition::oldDevRawList
#     oldDevRawList{key:devName}=value:rawName, devName like sda, rawName like raw1
# Common::typeDefinition::rawInfoList
#     rawInfoList array(map1, mapw)
#         map = (
#             rawName   => "raw1"
#             devType   => $enum(pcie, ssd, sata)
#             appType   => enum(APP_UNKNOWN, APP_KNOWN)
#             diskSize  => 2000398934016
#             devName   => "sda",
#             serialNum => "350014ee65ad6e8b2",
#         );


1;
