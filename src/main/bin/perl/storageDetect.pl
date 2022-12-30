#!/usr/bin/perl
use strict;
use warnings;

use Tie::File;
use File::Basename;
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';

use EnvironmentUtils;
#use Common;

my $envRoot = get_actual_environment_root();
my $configFile = "$envRoot/config/datanode.properties";

sub getSsdMinThreshold {

    my $ssdMinThreshold = 1000;
    open(CONFIGFILE, "$configFile") or exitProcess(__FILE__, __LINE__, "no such file : $configFile");
    while(my $perLine = <CONFIGFILE>){
        if ($perLine =~ /ssd.speed.min.threshold/) {
            my @array = split(/=/, $perLine);
            $ssdMinThreshold = $array[1];
            last;
        }
    }
    close(CONFIGFILE);

    if ($ssdMinThreshold >= 10) {
        return $ssdMinThreshold;
    } else {
        return 1000;
    }
}


sub exitProcess {
    my ($fileName, $fileLine, $msg) = (@_);
    print "exit fileName:$fileName, fileLine:$fileLine, msg:$msg\n";
    die "exit";
}


my $storagePath = $ARGV[0];
my $timeSecond = $ARGV[1];
my @reg;

if (defined $ARGV[2]) {
    @reg = split(/ /,$ARGV[2]);
}

print "starting, regular: @reg, path:$storagePath, timesecond:$timeSecond\n";

my $minSpeedThreshold = getSsdMinThreshold();

print "the SSD disk min speed threshold is $minSpeedThreshold\n";

my $cmd = "java -noverify -cp :$envRoot/build/jar/*:$envRoot/lib/*:$envRoot/config py.datanode.StorageDetect --storagepath $storagePath --timesecond $timeSecond --threshold $minSpeedThreshold";

print "excuting the cmd as $cmd \n";

system($cmd);

my $value = 0;
if ($? == - 1) {
    $value = $!;
    print "return value:$!, execute successfully for storage path:$storagePath, timeSecond:$timeSecond\n";
} else {
    $value = $? >> 8;
    print "return value:$?, execute unsuccessfully, real code: $value for storage path:$storagePath, timeSecond:$timeSecond \n";
}

if ($value == 100) {
    print "this is a normal disk for $storagePath \n";
} elsif ($value == 101) {
    print "this is a ssd disk for $storagePath \n";
} else {
    exitProcess("can not detect the storage:$storagePath type, value: $value");
}

print "finished\n";
