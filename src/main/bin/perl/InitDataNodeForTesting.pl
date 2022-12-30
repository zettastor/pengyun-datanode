#!/usr/bin/perl

use strict;
use warnings;
use File::Path qw(make_path);
use FindBin '$RealBin';
use File::Spec;
use Getopt::Long;
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use EnvironmentUtils;

my $my_script_dir = $RealBin;
$my_script_dir = get_my_script_dir();
die "This script must be run from the datanode environment.\n" unless get_environment_alias() =~ /.*datanode.*/;

my $env_root = get_actual_environment_root();
my $var_dir = "$env_root/var";
my $raw_disk_dir = "$env_root/var/storage/rawDisks";
my $cache_disk_dir = "$env_root/var/storage/l2Cache";

my $enableLevelTwoCache=0;
my $notWipeout = 0;
my $verbose = 0;
my $number_testing_raw_disks = 1; # the number of raw disks
my $rawDiskSizeInMb = 32; # 32Mb 
GetOptions ( "notWipeout"  => \$notWipeout,
	    "rawDiskSize=i"  => \$rawDiskSizeInMb,
	    "numberTestingRawDisks=i"  => \$number_testing_raw_disks,
	    "enableLevelTwoCache"  => \$enableLevelTwoCache,
	    "verbose"  => \$verbose)
 or die("Error in command line arguments. \n\nThe Format is InitDataNodeForTesting --rawDiskSize size_in_mb --notWipeout --verbose \n");

unless ($notWipeout) {
  my $cmd = "$my_script_dir/WipeoutTestingDNEnv.pl";
  if ($verbose) {
    $cmd = $cmd . " --verbose";
  }
  print "$cmd\n" if $verbose;
  (system($cmd) == 0) or die "ERROR: can't wipe out the testing data node  $?\n";
}

unless( -e $raw_disk_dir) {
   make_path($raw_disk_dir);
   print "Create $number_testing_raw_disks raw files, the size of which are $rawDiskSizeInMb Mb each\n";
   my $count = ($rawDiskSizeInMb * 1024 * 1024) / 512;
   my $index = 0;
   for($index = 1 ; $index <= $number_testing_raw_disks ; $index++) {
      my $rawDiskFileName = "raw$index";
      system("dd if=/dev/zero of=$raw_disk_dir/$rawDiskFileName count=$count> /dev/null 2>&1 ");
   }
}


print "This is for testing, no need to load raw disks\n";

######## formatting raw disks ############
print "initializing raw file\n";
my $index = 0;
for($index = 1 ; $index <= $number_testing_raw_disks ; $index++) {
    my $rawDiskFileName = "$raw_disk_dir/raw$index";
    # Since this sets the testing environment, we want to start from scratch
    my $serialNum = int(rand(100000000));
    my $cmd = "java -cp :$env_root/build/jar/*:$env_root/lib/*:$env_root/config py.datanode.archive.ArchiveInitializer --storage $rawDiskFileName --serialNumber $serialNum --devName /dev/sd$index --mode overwrite --runInRealTime false --firstTimeStart true --devType SATA 2>&1 > /dev/null";
    print "$cmd\n" if $verbose;
    (system($cmd) == 0) or die "ERROR: can't initialize raw file $?\n";
}

if ($enableLevelTwoCache) {
  print "creating directory for SSD cache:",$cache_disk_dir,"\n";
  mkdir $cache_disk_dir unless -d $cache_disk_dir;

  my $cacheSize;
  for($index = 1 ; $index <= $number_testing_raw_disks ; $index++) {
      my $levelCacheFileName = "$cache_disk_dir/raw$index";
      $cacheSize = 1024*1024*$index;
      system("dd if=/dev/zero of=$levelCacheFileName bs=$cacheSize count=64"); 
  }
}

print "Done\n";
exit 0;
