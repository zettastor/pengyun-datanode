#!/usr/bin/perl

use strict;
use warnings;
use File::Path qw(make_path);
use FindBin '$RealBin';
use Getopt::Long;
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';

use EnvironmentUtils;
use CheckRamSize;

my $my_script_dir = $RealBin;

die "This script must be run from the datanode environment.\n" unless get_environment_alias() =~ /pengyun-datanode.*/;

my $env_root = get_actual_environment_root();
my $var_dir = "$env_root/var";
my $raw_disk_dir = "$env_root/var/storage/rawDisks";
my $testing_storage_dir = "$env_root/var/testing";
my $testing_ram_disk_dir = "$testing_storage_dir/mnt/ramdisk";

my $verbose = 0;
GetOptions ( "verbose"  => \$verbose)
 or die("Error in command line arguments. \n\nThe Format is InitDataNodeForTesting --verbose \n");


###### umount ram disk #############
# mount the ramdisk there
print "umount testing raw disk\n";
my $cmd = "umount $testing_ram_disk_dir";
print "$cmd\n" if $verbose;
(system($cmd) == 0) or print "WARN: umounting $testing_ram_disk_dir $?\n";

######## delete var directory completely ######
$cmd = "rm -Rf $var_dir";
print "$cmd\n" if $verbose;
(system($cmd) == 0) or print "WARN: deleting $var_dir $?\n";
