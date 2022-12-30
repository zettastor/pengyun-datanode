#!/usr/bin/perl
use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0).'/bin';
use osversion;
use EnvironmentUtils;
use Common;

# As device name is assigned by the kernel dynamically, 
# so we use the udev rule based on the device serial numer to 
# bundle raw disk.
#
# step 1:the program gets all the rules in the 60-rules.d first.
# one physical raw disk one rule based on its serial number.
# step 2:gets all the raw disk in the os.
# step 3:compare the result of step 1 and step 2 and then know the 
# new installed disk and write its rule in 60-raw.rules.

#sometimes raw module might not be loaded into kernel. OSConfig will load the raw module if necessary
my $my_script_dir = $RealBin;
my $envRoot = get_actual_environment_root();

my $log = Log::Log4perl->get_logger("RawDiskInit");
$log->debug("starting");
#OSConfig load the raw module
system("/usr/bin/perl $my_script_dir/OSConfig.pl");

#if 60-raw.rules not exists,then create it
my $ruleFile = "$envRoot/var/storage/60-raw.rules";
if (not -e $ruleFile) {
    my $cmd = "touch $ruleFile";
    system($cmd);
}

my %oldDevList;
my %newDevList;

# gets all the disk rule in the 60-raw.rules and store it in a hash table.
# key:serial number value:rawN the device bundle to 
open(OLDDEVLIST, "$ruleFile") || exitProcess(__FILE__, __LINE__, "fail to open the file:$ruleFile");
while(my $perLine = <OLDDEVLIST>){
    my @oldDevArray = split(/,/, $perLine);
    my @tempKey = split(/==/, $oldDevArray[3]);
    $tempKey[1] =~ s/(^")|("$)//g;
    my $str = substr($oldDevArray[4], 25);
    my @tempValue = split(/\s/, $str);
    $oldDevList{$tempKey[1]} = $tempValue[0];
    my $cmd = `raw /dev/raw/$tempValue[0] 0 0`;
    $log->debug("unmap the raw device: $tempValue[0], result:$cmd");
}
# gets N of the last rawN
my $lastRawNum = 0;
while(($key, $value) = each %oldDevList){
    if (substr($value, 3) > $lastRawNum) {
        $lastRawNum = substr($value, 3);
    }
}
# gets all the disk in system
my $allDev = `fdisk -l 2>/dev/null | grep "Disk \/dev\/*" | grep -v "\/dev\/md" | awk ' {print \$2}' | sed -e 's/://g' | awk -F "[/:]" ' {print \$NF}'`;
my @devArray = split(/[\r\n]/, "$allDev");

open(LINE, "/proc/partitions") || exitProcess(__FILE__, __LINE__, "fail to open the file:/proc/partitions");
<LINE>;
<LINE>;
@parArray = <LINE>;

open(RULE_FILE, ">>$ruleFile") || exitProcess(__FILE__, __LINE__, "fail to open the rules file:$ruleFile");
# gets all raw disk in the system
for ($index = 0; $index <= $#devArray; $index++) {
    my $devName = $devArray[$index];
    $flag = 0;
    for ($ind = 0; $ind <= $#parArray; $ind++) {
        @temp = split(/ +/, "$parArray[$ind]");
        if ($temp[4] =~ /$devName[0 - 255]/) {
            # if we can find a partition for the dev. Jump out of the loop
            $flag = 1;
            last;

        }
    }
    if ($flag == 0) {
        # No partition was found
        my $seri = `/lib/udev/scsi_id -g -u /dev/$devName`;
        $seri =~ s/[\n\r]//g;
        $newDevList{"$seri"} = $devName;
    }
}

# check if the host has raw disk or exit
my $newDevListSize = keys%newDevList;
if ($newDevListSize == 0) {
    $log->warn("no raw disk in this host");
    exit 1;
}

# judge the system if it is supported 
if (osversion::is_supported() == 0) {
    $log->error("system is not supported");
    exit 1;
}

# compare step 1 and step 2 and then writes rules of the new device
my $rawN;
while(($key, $value) = each %newDevList){
    unless (exists $oldDevList{$key}) {
        $lastRawNum++;
        $rawN = "raw"."$lastRawNum";
        $log->debug("write new raw device:$rawN");
        if (osversion::is_ubuntu()) {
            print RULE_FILE "ACTION==\"change\",KERNEL==\"sd*\",PROGRAM==\"/lib/udev/scsi_id -g -u /dev/%k\",RESULT==\"$key\",RUN+=\"/sbin/raw /dev/raw/$rawN %N\" \n";
        } elsif (osversion::is_redhat() || osversion::is_centos()) {
            print RULE_FILE "ACTION==\"add\",KERNEL==\"sd*\",PROGRAM==\"/lib/udev/scsi_id -g -u /dev/%k\",RESULT==\"$key\",RUN+=\"/bin/raw /dev/raw/$rawN %N\" \n";
        } else {
            $log->error(" it cannot happen");
            exit 1;
        }
    }
}

# reboot the udev
system("udevadm trigger");

close(OLDDEVLIST);
close(LINE);
close(RULE_FILE);
$log->debug("finished");
