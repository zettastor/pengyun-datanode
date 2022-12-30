#!/usr/bin/perl
#
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# please note, this script has not been maintained by anyone for a long time, it it outdated.
# If we want use this script again, we need spend much time to fix problems in it, maybe rewrite it.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#
# To improve speed of data log persistent, we use ssd as a cache for datalog file system
# and file buffer. The ssd is splitted to two parts, one of them just a partition who is
# used for file buffer, the other of them has a file system who is used for datalog.
#
# When first time to deploy datanode, the script pick one of a completely pure ssd for
# log cache application, and persist the datalog file system to /etc/fstab. The file
# system record in this file will be mounted to specified mount point when machine reboot.
#
# When deployed datanode before this time of deployment, the script check if /etc/fstab
# has specified record of datalog. If it has, the script just link device to file buffer
# for use. If it doesn't have, the script scan all partitioned ssd first to search used ssd
# before or just use a new pure ssd if exists.
#
# Of course, if the system doesn't have ssd, we just use system disk as datalog file system
# and disable file buffer.
#
# @auther zjm
#
use File::Spec;
use FindBin '$RealBin';
use Cwd;
use Cwd 'abs_path';
use File::Basename qw(basename);
use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname( dirname abs_path $0) . '/bin';
use FDisk;
use MKFS;
use EnvironmentUtils;
use ArchiveForLogCache;
use Common;

use constant DEVICE_ROOT   => "/dev";
use constant DATALOG_SRC   => "/var/pengyun_cache";
use constant DATALOG       => "var/storage/datalog";
use constant FILEBUFFER    => "var/storage/filebuffer";
use constant L2CACHE       => "var/storage/l2Cache";
use constant DATANODE_CONF => "config/datanode.properties";
use constant DATANODE_ARCHIVE_CONF => "config/archive.properties";

my $env_root = get_actual_environment_root();
my $datanode_conf = File::Spec->catfile( $env_root, DATANODE_ARCHIVE_CONF );

my $log = Log::Log4perl->get_logger("InitArchiveForLogCache");
$log->warn("starting");

# ***Check if it is enable to use ssd as cache for datalog and file buffer***
if ( not _is_enable_ssd_for_data_log() and not _is_enable_file_buffer() ) {
	$log->warn("Neither datalog or filebuffer using ssd! Do nothing");
	exit 0;
}

my $filebuffer = File::Spec->catfile( cwd(), FILEBUFFER );
my $datalog    = File::Spec->catfile( cwd(), DATALOG );
my $datalog_src = DATALOG_SRC;

# ***Select a ssd by 3 steps***

$log->debug("SSD cache for either datalog or file buffer is enabled. Let's select a ssd");
my $ssd_for_log_cache_application = undef;

log->debug("Step 1: Check if there is a ssd used for datalog before.");
if ( my $fs_record = has_fs_permanent_mounted($datalog_src) ) {
	$log->debug("$datalog_src mount state is written to /etc/fstab before: $fs_record.");

	# ***Just use ssd used before***
	my $fs_uuid = substr( $fs_record->{file_system},
		index( $fs_record->{file_system}, '=' ) + 1 );
	my $device_path_for_datalog = get_device_path_with_fs_uuid($fs_uuid);
	$log->debug("Use disk $device_path_for_datalog with uuid $fs_uuid for log cache");
	( $ssd_for_log_cache_application = $device_path_for_datalog ) =~
		s/\/.*\/(.*)\d$/$1/g;
}
else {
	$log->debug("Unable to find it.");
	$log->debug("Step 2: Check if there is a ssd used for file buffer before.");

	# ***Find out the ssd used as log cache before first, if not found,
	# pick one from l2cache***
	foreach my $ssd ( @{ _get_all_ssds() } ) {
		$ssd_for_log_cache_application = $ssd
			and last
			if has_partition($ssd)
				and is_log_cache_application( $ssd . "1" );
		$ssd_for_log_cache_application = $ssd and last
			if is_log_cache_application($ssd);
	}

	if ( not defined $ssd_for_log_cache_application ) {
		$log->debug("Unable to find it.");
		$log->debug("Step 3: Check if there is a ssd in directory @{[L2CACHE]}.");
		$ssd_for_log_cache_application = _pick_one_ssd_not_l2cache_used();
	}
}

$log->debug("Selected ssd $ssd_for_log_cache_application for log cache");
$log->warn("No ssd could be used as log cache, use system disk without file buffer")
	and exit 0
	unless $ssd_for_log_cache_application;

# ***Partition the ssd if necessary***
my $ssd_path =
	File::Spec->catfile( DEVICE_ROOT, $ssd_for_log_cache_application );
if ( not has_partition($ssd_for_log_cache_application) ) {
	$log->warn("$ssd_path has no partition on it, let's partition it for two. One for datalog if enable, the other for file buffer if enable.");

	new_partition( $ssd_path, 1, _get_file_buffer_size_gb() );
	my $ssd_path1 = $ssd_path . "1";
	foreach my $tryTime ( 1 .. 5 ) {
		last if -e $ssd_path1;
		sleep 1;
		$log->warn("Partition $ssd_path1 has not be written to disk yet, let's try one more time ...");
		sync_partition($ssd_path);
	}

	new_partition( $ssd_path, 2 );
	my $ssd_path2 = $ssd_path . "2";
	foreach my $tryTime ( 1 .. 5 ) {
		last if -e $ssd_path2;
		sleep 1;
		$log->warn("Partition $ssd_path2 has not be written to disk yet, let's try one more time ...");
		sync_partition($ssd_path);
	}

	# to make sure the partition operation is applied to /proc/partitions memory mapping file
	system("partprobe");
}

# ***Format a fs to the ssd if necessary***
$log->debug("heck if it is necessary to prepare ssd for datalog.");

if ( _is_enable_ssd_for_data_log()
	and not has_fs_permanent_mounted($datalog_src) )
{
	$log->warn("SSD for datalog is enabled, prepare it...");

	if ( not -e $datalog or is_empty($datalog) ) {

		# ***clean datalog environment***
		system("rm -rf $datalog; umount $datalog_src; rm -rf $datalog_src;");

		exitProcess("Unable to clean $datalog")      if -e $datalog;
		exitProcess("Unable to umount $datalog_src") if has_fs_mounted($datalog_src);
		exitProcess("Unable to clean $datalog_src")  if -e $datalog_src;

		system("mkdir -p $datalog_src")
			and exitProcess("Unable to create directory $datalog_src $!");

		my $ssd_p2_path = $ssd_path . "2";
		$log->debug("Use ssd $ssd_p2_path for data log");

		$log->debug("Make file system on $ssd_p2_path.");
		mkfs_ext4($ssd_p2_path);

		$log->debug("Mount $ssd_p2_path to $datalog_src and permanetly record the mount info to /etc/fstab.");
		mount_fs_to( $ssd_p2_path, $datalog_src )
			and make_fs_permanent_mounted( $ssd_p2_path, $datalog_src, "ext4" );

		if ( my $fs_record = has_fs_permanent_mounted($datalog_src) ) {
			$log->debug("Successfully write mount record for $datalog_src to /etc/fstab: $fs_record");
			system("ln -s $datalog_src $datalog");
		}
	}
	else {
		$log->debug("$datalog is in used. Do nothing preparing ssd $ssd_path for datalog.");
	}
}

my $ssd_p1_path = $ssd_path . "1";
if ( _is_enable_file_buffer() and not -e $filebuffer ) {

	# ***Use partition 1 as file buffer***
	$log->debug("Use ssd $ssd_p1_path for file buffer");
	system("ln -s $ssd_p1_path $filebuffer");
}

# ***Initialize the selected ssd as archive***
$log->debug("Initialize the selected ssd $ssd_p1_path as archive.");

system("mkdir -p $env_root/logs") unless -e "$env_root/logs";
my $seri_number = get_seri_number($ssd_path);
my $cmd =
	"java -cp :$env_root/build/jar/*:$env_root/lib/*:$env_root/config py.datanode.archive.ArchiveInitializer --mode append --firstTimeStart true --serialNumber $seri_number --devName $ssd_path --devType SSD --runInRealTime false --storage $ssd_p1_path --application LOG_CACHE 2>&1 >> $env_root/logs/launch.log";

system($cmd) and exitProcess("Unable to run cmd $cmd $!");

# === Done ===
exit 0;

#
# Read config/datanode.properties to get specified file buffer size
#
sub _get_file_buffer_size_gb {
	open $datanode_conf, "<" . File::Spec->catfile( $env_root, DATANODE_CONF )
		or exitProcess("Unable to read datanode conf $datanode_conf $!");
	my @properties = <$datanode_conf>;
	close $datanode_conf;

	foreach my $prop (@properties) {
		chomp($prop);
		if ( $prop =~ /file\.buffer\.size\.gb/ ) {
			( my $file_buffer_size = $prop ) =~
				s/file\.buffer\.size\.gb\s*=\s*(\d+)/$1/g;
			return $file_buffer_size;
		}
	}
}

sub _is_enable_ssd_for_data_log {
	open $datanode_conf, "<" . File::Spec->catfile( $env_root, DATANODE_CONF )
		or exitProcess("Unable to read datanode conf $datanode_conf $!");
	my @properties = <$datanode_conf>;
	close $datanode_conf;

	foreach my $prop (@properties) {
		chomp($prop);
		if ( $prop =~ /enable\.ssd\.for\.data\.log\s*=\s*true/ ) {
			return 1;
		}
	}
	return 0;
}

sub _is_enable_file_buffer {
	open $datanode_conf, "<" . File::Spec->catfile( $env_root, DATANODE_CONF )
		or exitProcess("Unable to read datanode conf $datanode_conf $!");
	my @properties = <$datanode_conf>;
	close $datanode_conf;

	foreach my $prop (@properties) {
		chomp($prop);
		if ( $prop =~ /enable\.file\.buffer\s*=\s*true/ ) {
			return 1;
		}
	}

	return 0;
}

#
# Pick the first ssd exists under directory l2cache, and remove corresponding
# symbolic link to prevent from using by other component.
#
sub _pick_one_ssd_from_l2cache {
	my $l2cache = File::Spec->catfile( cwd(), L2CACHE );

	opendir( DIR, "$l2cache" ) or log->warn("Cant open $l2cache $!") and return;
	my @ssds = readdir(DIR);
	closedir DIR;

	my $num_files = @ssds;

	# print "We should preserve l2cache used ssd, and no more for datalog or file buffer.\n" and return undef unless $num_files > 3; # ".", "..", ssd

	foreach $ssd (@ssds) {
		next if ( $ssd =~ /^\.\.?$/ );

		my $ssd_path = File::Spec->catfile( $l2cache, $ssd );

		#my $ssd_abs_path_in_raw = abs_path($ssd_path);
		#my $ssd_seri_num = get_seri_number($ssd_abs_path_in_raw);
		#my $ssd_name = pickout_device_with_seri_num($ssd_seri_num);
		my $ssd_name = _get_devname_by_rawNum($ssd);

		if ( not $ssd_name ) {
			$log->debug("$ssd_path may not be a real ssd!");
			next;
		}

		# remove the device symbolic link from l2cache
		$ssd_name =~ s/^\s+|\s+$//g;
		$log->debug("find a ssd: ssd path is $ssd_path and ssd name is $ssd_name");
		system("rm -f $ssd_path") and exitProcess("can not remove file:$ssd_path, err:$!");
		return $ssd_name;
	}

	return undef;
}

sub _pick_one_ssd_not_l2cache_used {
	# first read from l2cache dir
	my $l2cache = File::Spec->catfile( cwd(), L2CACHE );

	opendir( DIR, "$l2cache" ) or $log->warn("Cant open $l2cache $!") and return;
	my @ssds = readdir(DIR);
	closedir DIR;

	my $pick_ssd = undef;

	# find all ssd in ssd.record
	foreach my $ssd_in_system ( @{ _get_all_ssds() } ) {
		$log->debug("get ssd:$ssd_in_system in system");
		next if has_partition($ssd_in_system);
		# run loop in l2cache
		foreach $ssd_in_l2cache (@ssds) {
			$log->debug("get ssd:$ssd_in_l2cache in l2cache");
			next if ( $ssd_in_l2cache =~ /^\.\.?$/ );

			# just for print log
			my $ssd_path = File::Spec->catfile( $l2cache, $ssd_in_l2cache );

			# get link file
			my $linked_raw_dev = readlink($ssd_path);
			$log->debug("get linked raw dev:$linked_raw_dev");

			# filt "/dev/raw/"
			my @splitArray = split( /\//, $linked_raw_dev );
			my $filt_raw_dev = @splitArray[3];
			$log->debug("get filt raw dev:$filt_raw_dev");
			my $ssd_name = _get_devname_by_rawNum($filt_raw_dev);

			if ( not $ssd_name ) {
				$log->warn("$ssd_path <==>  may not be a real ssd!");
				next;
			}

			$ssd_name =~ s/^\s+|\s+$//g;
			$log->debug("get ssd name:$ssd_name");
			# if not used in l2cache, we can use it
			if ( $ssd_in_system !~ /$ssd_name/ ) {
				$pick_ssd = $ssd_in_system;
				$log->debug("get l2cache unused ssd:$pick_ssd");
				return $pick_ssd;
			}
		}
	}
	return $pick_ssd;
}

sub _get_devname_by_rawNum {
	my ($rawNum) = (@_);
	my $ssdFile = File::Spec->catfile( cwd(), "var/storage/ssd.record" );
	open( SSDFILE, "<$ssdFile" );

	my $devName;

	while ( my $perLine = <SSDFILE> ) {
		my @rawNumAndDevName = split( /,/, $perLine );
		my $rawNameInFile = $rawNumAndDevName[0];
		if ( $rawNum eq $rawNameInFile ) {
			$devName = $rawNumAndDevName[1];
			close(SSDFILE);
			return $devName;
		}
	}

	close(SSDFILE);
	return undef;
}

sub _get_all_ssd_devname_from_ssd_record {
	my $ssdFile = File::Spec->catfile( cwd(), "var/storage/ssd.record" );
	open( SSDFILE, "<$ssdFile" );
	my @all_ssd_devname = ();
	my $devName;
	$log->debug("try to read $ssdFile");
	while ( my $perLine = <SSDFILE> ) {
		my @rawNumAndDevName = split( /,/, $perLine );
		$log->debug("get ssd record:@rawNumAndDevName");
		$devName = $rawNumAndDevName[1];
		chomp($devName);
		$devName =~ s/^\s+|\s+$//g;
		$log->debug("get ssd dev name:$devName");
		push @all_ssd_devname, $devName;
	}

	close(SSDFILE);
	return \@all_ssd_devname;
}

#
# Scan all devices in system to pick out a device having the specified seri_number
# which is scsi id
#
sub _pickout_device_with_seri_num {
	my $seri_number = shift;

	foreach my $device_name ( @{ _get_all_devices() } ) {
		my $device_path = File::Spec->catfile( DEVICE_ROOT, $device_name );
		return $device_name if $seri_number eq _get_seri_number($device_path);
	}
}

#
# Read archive info if exists to check if the device is for log cache application,
# The archive info is written by java program named 'ArchiveInitializer'.
#
sub _is_log_cache_application {
	my $device_name  = shift;
	my $archive_info = _read_archive_info($device_name);

	my $log_cache_application_regex =
		APPLICATION_KEY . ".*:.*" . APPLICATION_VALUE;
	foreach my $archive_info_line ( split( /[\r\n]/, $archive_info ) ) {
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
	my $device_path = File::Spec->catfile( DEVICE_ROOT, $device_name );
	my $archive_info =
		`java -cp "$env_root/lib/*:$env_root/config" py.datanode.archive.ArchivesReader $device_path`;
	sleep 2;

	return $archive_info;
}

#
# To check if specified device has partitions on it.
#
sub _has_partition {
	my $device_name = shift;

	foreach my $partition (@all_partitions_in_sys) {
		return 1 if $partition =~ /$device_name[1-9]+/;
	}

	return 0;
}

#
# Get all ssds from system
#
sub _get_all_ssds {
	my $pall_devices = _get_all_devices();
	return unless $pall_devices;

	my @ssds = ();
	foreach my $device ( @{$pall_devices} ) {
		push @ssds, $device if _is_ssd($device);
	}

	return \@ssds;
}

#
# Get all devices from system
#
sub _get_all_devices {
	my $allDisk =
		`fdisk -l 2>/dev/null | grep "Disk \/dev\/*" | grep -v "\/dev\/md" | grep -v "\/dev\/mapper" | awk ' {print \$2}' | sed -e 's/://g' | sed -e 's/[/]dev[/]//g'`;
	my @all_devices = split( /[\r\n]/, $allDisk );

	return \@all_devices;
}

#
# Check if the device is ssd
#
sub _is_ssd {
	my $device_name = shift;
	my $device_path = File::Spec->catfile( DEVICE_ROOT, $device_name );

	# in Common.pm
	return check_if_is_ssd( $device_path, 3);
}

$log->warn("finished");
