#!/usr/bin/perl
# so we use the udev rule based on the device serial numer to
# bundle raw disk.
#
# step 1:the program gets all the rules in the 60-rules.d first.
# one physical raw disk one rule based on its serial number.
# step 2:gets all the raw disk in the os.
# step 3:compare the result of step 1 and step 2 and then know the
# new installed disk and write its rule in 60-raw.rules.

#use strict;
use warnings;
use Tie::File;
use File::Path qw(make_path);
use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use EnvironmentUtils;

my $envRoot = get_actual_environment_root();
my $wipeoutSize = 16;
if (@ARGV >= 1) {
    $wipeoutSize = $ARGV[0];
}
print "wipe out disk size: $wipeoutSize MB from the zero position \n";
#sometimes raw module might not be loaded into kernel. run "modprobe raw" anyway
my $cmd = "modprobe raw";
(system($cmd) == 0) or die "ERROR: can not modprobe raw module to the kernel \n";

print "modprobe raw module is loaded to the kernel \n";
sleep 2;

$cmd = "python $envRoot/bin/zero_raw_disk.py --dd_size $wipeoutSize";
runPythonScriptAndExit($cmd);

sub runPythonScriptAndExit {
    my ($cmd) = @_;

    my ($cmdRetCode, @cmdRetLines) = execCmdAndGetRetLines($cmd);
    foreach my $line (@cmdRetLines) {
        chomp($line);
        print("$line\n");
    }

    exit($cmdRetCode)
}

sub execCmdAndGetRetLines {
    my ($cmd) = @_;
    my $retMsg = qx($cmd);
    my $retCode = $? >> 8;

    my @lines = split(/[\r\n]/, $retMsg);
    return $retCode, @lines;
}

#if 60-raw.rules not exists,then create it
my $rule_dir = "$envRoot/var/storage/60-raw.rules";
if (not -e $rule_dir) {
    my $cmd = "touch $rule_dir";
    system($cmd);
}

my %oldDevList;
my %newDevList;

#check 60rule file
check60RuleFile($rule_dir);

# gets all the disk rule in the 60-raw.rules and store it in a hash table.
# key:serial number value:rawN the device bundle to
open(OLDDEVLIST, "$rule_dir") || die("ERROR:open the file failed");
while(my $perLine = <OLDDEVLIST>){
    my @oldDevArray = split(/,/, $perLine);
    my @tempKey = split(/==/, $oldDevArray[3]);
    $tempKey[1] =~ s/(^")|("$)//g;
    my $str = substr($oldDevArray[4], 25);
    my @tempValue = split(/\s/, $str);
    $oldDevList{$tempKey[1]} = $tempValue[0];
    my $cmd = "raw /dev/raw/$tempValue[0] 0 0";
    system($cmd);
}

# gets all the disk in system without partion.
my $devArryTmp = getDisksWithoutPartionInfo();
my @devArray = @$devArryTmp;

open(RULE_FILE, ">>$rule_dir") || die("ERROR:open the rules file failed");
# gets all raw disk in the system
for (my $index = 0; $index <= $#devArray; $index++) {
    my $devName = $devArray[$index];

    # No partition was found
    my $seri = `/lib/udev/scsi_id -g -u /dev/$devName`;
    $seri =~ s/[\n\r]//g;
    $newDevList{"$seri"} = $devName;
}

# check if the host has raw disk or exit
my $newDevListSize = keys%newDevList;
if ($newDevListSize == 0) {
    print "no raw disk in this host \n";
    exit 1;
}
my $rawKey;
foreach $rawKey (keys(%newDevList)) {
    my $rawDisk = $newDevList{$rawKey};
    print "zero the first $wipeoutSize MB space of the disk (key=$rawKey, disk=$rawDisk) \n";
    system("dd if=/dev/zero of=/dev/$rawDisk bs=1M count=$wipeoutSize");
}

close(OLDDEVLIST);
close(RULE_FILE);

# the function check 60rule
# parameter 1 is 60rule path
# ACTION=="add",KERNEL=="sdj",PROGRAM=="/lib/udev/scsi_id -g -u /dev/%k",RESULT=="350014ee0aea7e69c",RUN+="/bin/raw /dev/raw/raw9 %N"
sub check60RuleFile{
    my ($fileName) = (@_);

    my $action = "ACTION==";
    my $kernel = ",KERNEL==";
    my $program = ",PROGRAM==";
    my $result = ",RESULT==";
    my $run = ",RUN";

    my @lines;
    tie(@lines, 'Tie::File', $fileName) or die;

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

sub getDisksWithoutPartionInfo {
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

