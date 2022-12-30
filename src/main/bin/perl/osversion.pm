#!/usr/bin/perl
package osversion;
use Exporter qw(import);

use warnings;
use strict;

use Common;
my $log = Log::Log4perl->get_logger("osversion");

# those system that are supported, the key is the system name, the value is major version 
# number, you can add a item that system is not support before, or add major version number
# at the map list. 
our %os_map = (
	"Ubuntu" => "12 14 16 17",
	"Red Hat" => "5 6 7",
	"CentOS" => "5 6 7"
	);

# judge the current operate system is supported 
sub is_supported {
	my $os_name;
	my $os_info = &get_os_info();

	if ( $os_info =~ "Ubuntu" ) {
		$os_name = "Ubuntu";
	}
	elsif( $os_info =~ "Red Hat" ) {
		$os_name = "Red Hat";
	}
	elsif( $os_info =~ "CentOS" ) {
		$os_name = "CentOS";
	}
	else {
		return 0;
	}

	my $os_versions = $os_map{$os_name};
	if (not defined($os_versions)) {
		return (0);
	}
	
	my($os_major, $os_minor);
	($os_major, $os_minor) = &get_os_version();

	my @versions = split(/\s+/, $os_versions);
	foreach my $temp (@versions) {
		if ($temp eq $os_major) {
			return (1);
		}
	}		
	return (0);
}

sub is_ubuntu {
	my $os_name = &get_os_info();
	if ($os_name =~ "Ubuntu") {
		$log->debug("The current version is Ubuntu");
		return (1);
	}
	return (0);
}

sub is_redhat {
	my $os_name = &get_os_info();
	if ($os_name =~ "Red Hat") {
		$log->debug("The current version is Red Hat");
		return (1);
	}
	return (0);
}

sub is_centos {
    my $os_name = &get_os_info();
    my $centosversion;
    if ($os_name =~ "CentOS") {
		$log->debug("The current version is CentOS");
        return (1);
    }
    return (0);
}


sub get_os_info {
	my $os_info = `cat /etc/*-release`;
	#$log->debug("The current infor of OS as following:\n $os_info");
    return $os_info;
}

# this function will return two value, the first is major version number, the second is minor version
# number
sub get_os_version {
    my $os_info = `cat /etc/issue`;
    $os_info =~ /-?(\d+)\.?(\d+)/;
    
    if (defined($1)) {
    	return ($1,$2);
    }
    
    ### if not match, try to match centos 7
    #$os_info = `rpm -q centos-release`;
    #$os_info =~ /-?(\d+)\-?(\d+)/;

    ### if not match, try to match redhat 7
    $os_info = `cat /etc/redhat-release`;
    $os_info =~ /\s?(\d+)\.+(\d+)/;
    if (defined($1)) {
        return ($1,$2);
    }

    return ($1,$2);
}

1;
