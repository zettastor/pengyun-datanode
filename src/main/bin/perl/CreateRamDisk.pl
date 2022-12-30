#!/usr/bin/perl
# Sets up a ram-based file mount in persistence root to serve
# as the file backing data node 's page pool. The ramdisk is mounted to
# /mnt/ramdisk and symlinked into the data node environment.
#
# Does nothing if the directory already exists and is properly mounted
# as tmpfs. 
#
# Errors if the directory exists, is non-empty, and is not mounted on tmpfs

use strict;
use warnings;

my $env_root = $ARGV[0];
my $mount_point = $ARGV[1];
my $ram_disk_size = $ARGV[2]; # example: 1234m or 1234M
my $datanode_config_file = "$env_root/config/datanode.properties";

unless ( defined $env_root && defined $mount_point && defined $ram_disk_size) {
   die "ERROR: mount size is not specified. Should specified like 100m\n";
}
 
# make sure the mount point exists and is a directory
unless( -e $mount_point ) {
    system("mkdir -p " . $mount_point);
}

unless( -d $mount_point ) {
    die "ERROR: page pool directory $mount_point either doesn't exists or is not directory!\n";
}

my $os_name = `uname -s`;
chomp $os_name;
if( (`df $mount_point | grep -c tmpfs`) > 0 ) {
    warn "$mount_point already mounted as ramdisk tmpfs\n";
} else {
    my $cmd = "mount -t tmpfs -o size=$ram_disk_size tmpfs $mount_point";
    if (lc($os_name) eq "darwin") {
      print "This is Mac\n";
      $cmd = "$env_root/bin/CreateRamDiskForMac.sh $mount_point $ram_disk_size";
    }
    print "$cmd\n";
    (system($cmd) == 0) or die "ERROR: mounting ramdisk $?\n";
}

if (lc($os_name) ne "darwin") {
  # check that the mount is the size we expect
  my $tmpfs_size = (`df --block-size=1M | grep $mount_point | awk '{print \$2}'`);
  chomp $tmpfs_size;
  $tmpfs_size = $tmpfs_size . "m";
  # resize if not
  unless( $tmpfs_size eq $ram_disk_size ) {
      print "$mount_point has size $tmpfs_size, expected $ram_disk_size\n";
      my $cmd = "mount -o remount -o size=$ram_disk_size $mount_point";
      print "$cmd\n";
      (system($cmd) == 0) or die "ERROR: resizing ramdisk $?\n";
  }
}

# now create a symlink in datanode 's environment 
my $storage_dir = "$env_root/var/storage/";
my $mmpp_dir = $storage_dir;
my $mmpp_file = $mmpp_dir . "pagePool";
unless( -e $mmpp_dir ) {
    (system("mkdir  -p $mmpp_dir") == 0) or die "ERROR: can't mkdir for  $mmpp_dir $?\n";
}

# if mmpp_file exist, remove it and then create an symbol link to it
if (-e $mmpp_file) {
	my $cmd = "rm -rvf $mmpp_file";
	print "$cmd\n";
    (system($cmd) == 0) or die "ERROR: can`t remve the old $mmpp_file \n"; 
} 
    
my $cmd = "ln -s $mount_point $mmpp_file";
print "$cmd\n";
(system($cmd) == 0) or die "ERROR: can`t ln -s $mount_point $mmpp_file $?\n";

# the mmpp file already exists, just make sure it points to the right thing
unless( -l $mmpp_file ) {
    die "ERROR: $mmpp_file exists, but is not a symlink - cannot link ramdisk!\n";
}

my $link = readlink $mmpp_file;
if( $link ne $mount_point ) {
    die "ERROR: $mmpp_file is a symlink but to the wrong directory. expected: $mount_point actual: $link\n";
}

# update the ram disk size in the datanode configuration file
$ram_disk_size =~ s/(\d+)[m|M]/$1/g;
my $ram_disk_size_bytes = $ram_disk_size * 1024 * 1024;
&modify_line_of_file($datanode_config_file, "ramdisk.size.bytes", $ram_disk_size_bytes);

# accept three parameter, first: config file path, second: the key of configuration, third: the value of configuration

sub modify_line_of_file {
    my ($file, $config_key, $config_value) = @_;

    my @file_content = ();
    my $index = 0;
    my $find_key = 0;

    open (FILEHANDLER, "<$file") or die "cannot open file $file";
    while (<FILEHANDLER>) {
        my $line_in_file = $_;
        if (index($line_in_file, $config_key) != -1) {
            $line_in_file = "$config_key=$config_value";
            $find_key = 1;
        }

        $file_content[$index] = $line_in_file;
        $index++;
    }

    close (FILEHANDLER);

    if ($find_key == 0) {
        $file_content[$index] = "$config_key=$config_value";
    }

    open (FILEHANDLER, ">$file") or die "cannot open file $file";
    for ($index=0; $index < @file_content; $index++) {
        print FILEHANDLER $file_content[$index];
    }

    close(FILEHANDLER);
}
