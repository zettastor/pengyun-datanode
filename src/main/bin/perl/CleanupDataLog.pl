#!/usr/bin/perl
# DataNode data log clean up task

# Data log records operations to segments.
# So the log size grows quickly.
# so need a task to clean up data log when its size above a given size.
# The task deletes log file upon its created time in order to make the whole data log size at a normal level.

use strict;
use warnings;
use File::Find;
use FindBin '$RealBin';
use File::Basename;



my $env_root = dirname($RealBin);
$env_root = &get_datanode_var_parent($env_root);
my $log_par_dir = "$env_root/var/storage/datalog";

my $limit_log_size = 100;
my $MB_size = 1024 * 1024;
my $current_log_size = 0;


#get datalog size
if (-e $log_par_dir) {
	$current_log_size = &get_dir_size($log_par_dir);
}

$limit_log_size = $limit_log_size * $MB_size;

if ($limit_log_size < $current_log_size) {
	&cleanup_datalog;
}

# function that clean up data log to below the limit size
sub cleanup_datalog {
	opendir DH, $log_par_dir;
	my @sub_log_dirs = readdir DH;
	closedir DH;
	@sub_log_dirs = grep { $_ ne "." and $_ ne ".." } @sub_log_dirs;
	my @log_file_array;
	foreach my $sub_log_dir (@sub_log_dirs) {
		$sub_log_dir = "$log_par_dir/$sub_log_dir";
		my @sub_log_file_array = &get_dir_files($sub_log_dir);
		my $sub_log_file_array_size = @sub_log_file_array;
		if($sub_log_file_array_size > 0) {
			push @log_file_array, @sub_log_file_array;
		}
	}
	my @sorted_log_file_array = sort { &get_log_file_number(basename($a)) <=> &get_log_file_number(basename($b)) } @log_file_array;
	&cleanup_log_file(@sorted_log_file_array);
}

# function that clean up log files
sub cleanup_log_file {
	my @to_rm_log_files = @_;
	foreach my $file_name (@to_rm_log_files) {
		if(&get_dir_files(dirname($file_name)) > 1) {
			system("rm $file_name");
		}
		$current_log_size = &get_dir_size($log_par_dir);
		if($current_log_size < $limit_log_size) {
			return ;
		}
	}
}

# function that get dir size by a given dir
sub get_dir_size {
	my $size = 0;
	my $to_calu_dir = shift;
	find(sub { $size += -s if -f $_ }, $to_calu_dir);
	return $size;
}

# function get last time that the log file modified
sub get_last_modify_time {
	my $file = shift;
	my @file_pro = stat($file);
	return $file_pro[9];
}

# function that get a array of all files of a dir
sub get_dir_files {
	my $to_get_dir = shift;
	opendir (DIR, $to_get_dir) || die "Can't open directory $to_get_dir";
	my @file_array = readdir(DIR);
	closedir DIR;
	@file_array = grep { $_ ne "." and $_ ne ".." } @file_array;
	foreach my $file_dir (@file_array) {
		$file_dir = "$to_get_dir/$file_dir";
	}
	return @file_array;
}

# function that get id number of log file name
sub get_log_file_number {
	my $file_name = shift;
	my @name_array = split(/_/, $file_name);
	my $name_array_size = @name_array;
	if($name_array_size != 3 ) {
		return 0;
	} else {
		return $name_array[2];
	}
}

# because the directory is link symblic, and we can not get the script path by `pwd`
# so we do like this
sub get_datanode_var_parent {
	my $package_name = "packages";
	my $pengyun_name = "pengyun";
	my $new_path = "";
	my @new_dirs;

	my @dirs = split(/\//, $_[0]);
	for (my $index = 0; $index < @dirs; $index++) {
		my $dir = $dirs[$index];
		if ($dir =~ /$package_name/) {
			$dir = $package_name;
		} elsif ($dir =~ /$pengyun_name/) {
			my @dir_split = split(/-/, $dir);
			pop @dir_split;
			$dir = "";
			foreach my $temp (@dir_split) {
				if ($dir eq "") {
					$dir = $temp;
				} else {
					$dir = "$dir-$temp";
				}
			}
		}
		push @new_dirs, $dir;
	}
	foreach my $temp (@new_dirs) {
		if ($temp ne "") {
			$new_path = "$new_path/$temp";
		}
	}
	return ($new_path);
}

