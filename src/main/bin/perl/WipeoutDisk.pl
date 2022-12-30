#!/usr/bin/perl
#
# Wipeout all used disk by datanode.
#
# @auther zjm
#

use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0) . '/bin';
use MKFS;
use FDisk;
use EnvironmentUtils;
use ArchiveForLogCache;

use constant DD_SIZE_MB         =>  16;

my $env_root = get_actual_environment_root();

# ***Wipeout all disk for datanode general application***
system("$env_root/bin/ZeroRawDisk.pl ".DD_SIZE_MB);
