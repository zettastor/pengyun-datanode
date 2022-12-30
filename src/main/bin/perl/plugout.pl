#!/usr/bin/perl

use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use EnvironmentUtils;
use CheckRamSize;
use Common;
use osversion;

use constant  RC_OK      => 0;
use constant  PARA_ERROR => 1;
use constant  IO_OPPER_ERROR => 2;
use constant  FOUND_FAILED   => 3;

my $envRoot = get_actual_environment_root();
exitProcess(__FILE__, __LINE__, "This script must be run from the datanode environment.") unless get_environment_alias() =~ /datanode.*/;

my $systemRawDiskDir = "/dev/raw";
my $perlLog = "$envRoot/config/perl_log4j.properties";

Log::Log4perl->init($perlLog);
my $log = Log::Log4perl->get_logger("plugout");

sub getRawNameBySerialNumFromRuleFile {
    my($targetSerialNum) = (@_);

    my ($devName, $serialNum, $rawName) = queryDevInfoFrom60ruleFile(undef, $targetSerialNum, undef);

    if ($rawName) {
        $log->debug("get rawName:[$rawName] by serialNum:[$targetSerialNum] from 60rule file.");
        return $rawName;
    } else {
        $log->error("serialNum:[$targetSerialNum] doesn't exist in 60rule file.");
        return undef;
    }
}

sub existDisk {
    my($serialNum)=(@_);

    $log->debug("The serialNum is $serialNum");

    my $cmd=`ls -l /dev/disk/by-id`;

    $log->debug("cmd result as: \n $cmd\n");

    if($cmd =~ /$serialNum/) {
        return "true";
    }

    return "false";
}


#AGV[0] serial number from Application.
my $serialNum = $ARGV[0];

unless ( defined $serialNum ) {
   my $name = basename($0);
   exitProcess(__FILE__, __LINE__, "there is no $name system_raw_disk_dir");
   exit PARA_ERROR;
}

if (getUsePythonScript() eq "true") {
    my $cmd = "python $envRoot/bin/plugout_archive.py --serial_num $serialNum";
    runPythonScriptAndExit($cmd);
}

$log->warn("+++++++++++++++The plugout work begin ARGV[0] is $serialNum+++++++++++++++++++");

#Check whether the disk is exist by serialNumber.
#my $diskExist = existDisk($serialNum);
#if($diskExist eq "true") {
#} else {
#    $log->error("the disk of serialNumber : $serialNum is not exist");
#
#    #TODO: exist with a ErrorCode, to tell the service the result.
#}

my $rawName = getRawNameBySerialNumFromRuleFile($serialNum);
if (!defined($rawName)) {
    $log->error("Can't find rawName by serialNum:[$serialNum] plugout disk failed.");
    exit FOUND_FAILED;
}

$log->debug("plugged out disk: serial num:[$serialNum] and rawName:[$rawName]");

#unlink the plugouted disk.
unlinkDataNode($systemRawDiskDir, ($rawName));

#remove raw and link
removeRawDiskAndFileLineInfo(($serialNum => $rawName));

$log->debug("plugout.pl finished successfully");

exit RC_OK;

