#!/usr/bin/perl
use Exporter qw(import);

sub get_system_ram_size {
    my $ram = (`cat /proc/meminfo |  grep "MemTotal" | awk '{print \$2}'`);

    # default page pool size is 4096 pages and 64 swapping page, or 100m
    # ram disk size should be a little larger than page pool size
    my $mount_size_mb = "100";

    if( $ram >= 200000000 ) {
        # for boxes with more than 128g we want a 85g page pool
        $mount_size_mb = "85000";
    } elsif( $ram >= 120000000) {
        # for boxes with 128g we want a 67g page pool
        $mount_size_mb = "67000";
    } elsif( $ram >= 60000000 ) {
        # for boxes with 64g we want a 35g page pool
        $mount_size_mb = "24000";
    } elsif( $ram >= 30000000 ) {
        # for boxes with 32g we want a 18g page pool
        $mount_size_mb = "18000";
    } elsif( $ram >= 14000000 ) {
        # for boxes with 16g we want a 9.5g page pool
        $mount_size_mb = "9500";
    } elsif( $ram >= 7000000) {
        # for boxes with 8g we want a 3.5g page pool
        $mount_size_mb = "3500";
    } elsif( $ram >= 3000000) {
		# for boxes with 4g we want a 0.5g page pool
		$mount_size_mb = "200";
	} else {
        print "ram size is $ram, and very small";
    }

    return $mount_size_mb;
}

1;

