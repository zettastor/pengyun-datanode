#!/usr/bin/perl
#
# This module provides method to check if the archive is used for log cache.
#
# @auther zjm
#

use Exporter qw(import);
use FindBin qw($RealBin);
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use FDisk;
use EnvironmentUtils;

my $env_root = get_actual_environment_root();

use constant DEVICE_ROOT        =>  "/dev";
use constant APPLICATION_KEY    =>  "archiveApplication";
use constant APPLICATION_VALUE  =>  "LOG_CACHE";

#
# Read archive info if exists to check if the device is for log cache application,
# The archive info is written by java program named 'ArchiveInitializer'.
#
sub is_log_cache_application {
    my $device_name = shift;
    my $archive_info = _read_archive_info($device_name);
    
    my $log_cache_application_regex = APPLICATION_KEY.".*:.*".APPLICATION_VALUE;
    foreach my $archive_info_line(split(/[\r\n]/, $archive_info)) {
        print $archive_info_line."\n";
        return 1 if $archive_info_line =~ /$log_cache_application_regex/;
    }

    return 0;
}

#
# Read archive info out from device if exists.
# The archive info is written by java program named 'ArchiveInitializer'.
#
sub _read_archive_info {
    my $device_name = shift;
    my $device_path = File::Spec->catfile(DEVICE_ROOT, $device_name);

    my $archive_info = `java -cp "$env_root/lib/*:$env_root/config" py.datanode.archive.ArchivesReader $device_path`;
    sleep 2;

    return $archive_info;
}

1;
