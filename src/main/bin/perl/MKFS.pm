#!/usr/bin/perl
#
# This module provides multiple utility for file system relative
# operation.
#
# @auther zjm
#
use Exporter qw(import);

use constant FS_TABLE   =>  "/etc/fstab";

sub mkfs_ext4 {
    my $device_path = shift;

    system("dmsetup remove_all; mkfs.ext4 $device_path") and die $!;
}

sub mount_fs_to {
    my $from = shift;
    my $to = shift;

    return (system("dmsetup remove_all; mount $from $to") == 0);
}

sub has_fs_mounted {
    my $target_dir = shift;
    my $mount_records_in_str = `mount -l`;

    my $device_path = undef;
    foreach my $mount_record(split(/[\r\n]/, $mount_records_in_str)) {
        chomp($mount_record);
        ($device_path = $mount_record) =~ s/(.*)\s+on\s+.*/$1/g and return $device_path if $mount_record =~ /on\s+$target_dir/;
    }

    return $device_path;
}

# 
# Scan /etc/fstab file to check if there is a record of specified directory.
# If the record exists, the directory is mounted already.
# If the record doesn't, the directory is not mounted.
#
sub has_fs_permanent_mounted {
    my $target_dir = shift;

    open my $fs_tab, "<".FS_TABLE or die $!;
    my @fs_tab_records = <$fs_tab>;
    close $fs_tab;

    my %permanent_record = (
        file_system     => undef,
        mount_point     => undef,
        type            => undef,
        options         => undef,
        dump            => undef,
        pass            => undef
    );

    foreach my $record(@fs_tab_records) {
        chomp($record);
        next if $record =~ /^#/;
        if ($record =~ /$target_dir/) {
            my @record_parts = split(/\s+/, $record);
            $permanent_record{file_system} = $record_parts[0];
            $permanent_record{mount_point} = $record_parts[1];
            $permanent_record{type}        = $record_parts[2];
            $permanent_record{options}     = $record_parts[3];
            $permanent_record{dump}        = $record_parts[4];
            $permanent_record{pass}        = $record_parts[5];
            return \%permanent_record;
        }
    }
}

# 
# Record mount condition to /etc/fstab
#
sub make_fs_permanent_mounted {
    my $device_path = shift;
    my $mount_point = shift;
    my $type = shift;

    my %permanent_record = (
        file_system     => "UUID=".get_fs_uuid_for_device_path($device_path),
        mount_point     => $mount_point,
        type            => $type,
        options         => "defaults",
        dump            => 0,
        pass            => 0
    );

    my $record_builder = "";
    $record_builder = $record_builder.$permanent_record{file_system}."\t";
    $record_builder = $record_builder.$permanent_record{mount_point}."\t";
    $record_builder = $record_builder.$permanent_record{type}."\t";
    $record_builder = $record_builder.$permanent_record{options}."\t";
    $record_builder = $record_builder.$permanent_record{dump}."\t";
    $record_builder = $record_builder.$permanent_record{pass}."\n";

    open my $fs_tab, ">>".FS_TABLE or die $!;
    print $fs_tab $record_builder;
    close $fs_tab;
}

sub remove_permanent_mounted_fs {
    my $mount_point = shift;

    open my $fs_tab, "<".FS_TABLE or die $!;
    my @fs_tab_records = <$fs_tab>;
    close $fs_tab;

    open $fs_tab, ">".FS_TABLE or die $!;
    foreach my $record(@fs_tab_records) {
        next if $record =~ /$mount_point/;
        print $fs_tab $record;
    }
    close $fs_tab;
}

# 
# device with file system always record in /etc/fstab indentified by an uuid,
# from this uuid, we can find out corresponding device
#
sub get_fs_uuid_for_device_path {
    my $device_path = shift;

    my $blk_records_in_str = `blkid`;
    foreach my $blk_record(split(/[\r\n]/, $blk_records_in_str)) {
        chomp($blk_record);
        if ($blk_record =~ /$device_path/) {
            (my $fs_uuid = $blk_record) =~ s/.*\s+UUID="(.*)"\s+TYPE.*/$1/g;
            return $fs_uuid;
        }
    }
}


#
# After create file system on some device, an uuid will be generate automatically,
# thid function get corresponding uuid for the file system
#
sub get_device_path_with_fs_uuid {
    my $fs_uuid = shift;

    my $blk_records_in_str = `blkid`;
    foreach my $blk_record(split(/[\r\n]/, $blk_records_in_str)) {
        chomp($blk_record);
        if ($blk_record =~ /$fs_uuid/) {
            (my $device_path = $blk_record) =~ s/(.*):\s*.*/$1/g;
            #my $device_path = substr($blk_record, 0, index($blk_record, ':'));
            return $device_path;
        }
    }
}

sub is_empty {
    my $target_dir = shift;

    opendir(DIR, "$target_dir") or die "Cant open $target_dir $!\n";
    my @files = readdir(DIR);
    closedir DIR;

    foreach $file(@files) {
        next if $file =~ /^\.\.?$/; 
        return 0;
    }

    return 1;
}

1;
