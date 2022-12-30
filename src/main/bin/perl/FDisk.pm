#!/usr/bin/perl
#
# This module provides some method to create a partition and delete a partition on
# some disk.
# 
# Also this module provides some method relative to disks.
#
# auther zjm
# 
use Exporter qw(import);

use constant DEFAULT    =>  "";
use constant DEVICE_ROOT        =>  "/dev";
use constant PARTITION_RECORD   =>  "/proc/partitions";

my $raw_disk_path = undef;

my %new_par_params = (
    new_partition   =>  "n",
    partition_type  =>  "p",
    partition_num   =>  DEFAULT,
    first_sector    =>  DEFAULT,
    last_sector     =>  DEFAULT,
    write_to_disk   =>  "w"
);

my %del_par_params = (
    del_partition   =>  "d",
    partition_num   =>  "1",
    write_to_disk   =>  "w"
);

my %sync_par_params = (
    write_to_disk   =>  "w"
);

#
# Create new partition with specified size on specified disk, If the size is not given,
# all remaining size of disk will be used for this partition.
#
sub new_partition {
    $raw_disk_path = shift;
    $new_par_params{partition_num} = shift;
    my $partition_size_gb = shift;

    $new_par_params{last_sector} = DEFAULT;
    $new_par_params{last_sector} = "+$partition_size_gb"."G" if $partition_size_gb;

    my $new_par_cmd = _build_new_par_cmd();

    print "command: $new_par_cmd\n";

    system($new_par_cmd);
}

#
# Delete specified partition who has specified num on specified disk.
#
sub del_partition {
    $raw_disk_path = shift;
    $del_par_params{partition_num} = shift;

    my $del_par_cmd = _build_del_par_cmd(); 

    print "command: $del_par_cmd\n";

    system($del_par_cmd);
}

sub sync_partition {
    $raw_disk_path = shift;

    my $sync_par_cmd = _build_sync_par_cmd();

    print "command: $sync_par_cmd\n";

    system($sync_par_cmd);
}

#
# Use tool scsi_id to get device scsi id to indentify the device
#
sub get_seri_number {
    my $device_path = shift;
    my $seri_number = `/lib/udev/scsi_id -g -u $device_path`;
    chomp($seri_number);
    
    if ($seri_number eq "") {
        (my $devName = $device_path) =~ s/.*\/(.*)/$1/g;
        $seri_number = "/dev/$devName";
    }

    return $seri_number;
}

#
# Scan all devices in system to pick out a device having the specified seri_number
# which is scsi id
#
sub pickout_device_with_seri_num {
    my $seri_number = shift;

    foreach my $device_name(@{get_all_devices()}) {
        my $device_path = File::Spec->catfile(DEVICE_ROOT, $device_name);
        return $device_name if $seri_number eq get_seri_number($device_path);
    }
}

#
# To check if specified device has partitions on it.
#
sub has_partition {
    my $device_name = shift;
    #
    # ***Read system partition records***
    open $partition_record, "<".PARTITION_RECORD or die "Cant open ".PARTITION_RECORD.$!;
    my @all_partitions_in_sys = <$partition_record>;
    close $partition_record;

    foreach my $partition(@all_partitions_in_sys) {
        return 1 if $partition =~ /$device_name[1-9]+/;
    }

    return 0;
}

#
# Get all ssds from system
#
sub get_all_ssds {
    my $pall_devices = get_all_devices();
    return unless $pall_devices;

    my @ssds = ();
    foreach my $device(@{$pall_devices}) {
        push @ssds, $device if is_ssd($device);
    }

    return \@ssds;
}

# 
# Get all devices from system
#
sub get_all_devices {
    my $all_devices_in_str = `fdisk -l 2>/dev/null | grep "Disk \/dev\/*" | grep -v "\/dev\/md" | awk ' {print \$2}' | sed -e 's/://g' | awk -F "[/:]" ' {print \$NF}'`;
	my @all_devices = split(/[\r\n]/, $all_devices_in_str);

    return \@all_devices;
}

#
# Check if the device is ssd
#
sub is_ssd {
    my $device_name = shift;
    my $device_path = File::Spec->catfile(DEVICE_ROOT, $device_name);

    return length(`hdparm -I $device_path | grep "Solid State Device"`);
}

sub _build_new_par_cmd {
    my @inputs = (
        $new_par_params{new_partition},
        $new_par_params{partition_type}, 
        $new_par_params{partition_num},
        $new_par_params{first_sector},
        $new_par_params{last_sector},
        $new_par_params{write_to_disk}
    );

    return _build_partition_cmd(\@inputs);
}

sub _build_del_par_cmd {
    my @inputs = (
        $del_par_params{del_partition},
        $del_par_params{partition_num},
        $del_par_params{write_to_disk}
    );

    return _build_partition_cmd(\@inputs);
}

sub _build_sync_par_cmd {
    my @inputs = (
        $sync_par_params{write_to_disk}
        );

    return _build_partition_cmd(\@inputs);
}

sub _build_partition_cmd {
    my $inputs = shift;

    my $command_builder = "echo \"";
    foreach my $param_input(@{$inputs}) {
        $command_builder = $command_builder.$param_input."\n";
    }
    $command_builder = $command_builder."\" | fdisk $raw_disk_path";

    return $command_builder;
}

1;
