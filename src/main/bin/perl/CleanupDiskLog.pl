#!/usr/bin/perl

use warnings;
use strict;
use FindBin '$RealBin';
use File::Basename;

my $env_root = dirname($RealBin);
$env_root = &get_datanode_var_parent($env_root);
my $disk_log_dir = "$env_root/var/storage/disklog";

# get all files in disk log directory
opendir(TEMP, $disk_log_dir) || die "cannot open $disk_log_dir:$!";
my @files = readdir TEMP;
@files = grep { $_ ne "." and $_ ne ".." } @files;

my $max_files_count = 3;
my $total_count = scalar(@files);

if ($total_count <= $max_files_count) {
    die "current file count is $total_count\n";
}

# sorted the file list by file name
my @sorted_files = sort { $b cmp $a } @files;

for (my $index = $total_count -1; $index >= $max_files_count; $index--) {
    my $temp = "$disk_log_dir/$sorted_files[$index]";
    system("rm $temp");
}

sub getAllFilenamesFromFolder {
    my ($folder) = @_;

    my @filenames = ();
    if (!opendir(FOLDER, $folder)) {
        printf("Can't open dir:[$folder].\n");
        return @filenames;
    }

    foreach my $filename (readdir(FOLDER)) {
        if ($filename eq "." || $filename eq "..") {
            next;
        }

        printf("find file:[$filename] in folder:[$folder]\n");
        push(@filenames, $filename);
    }
    closedir(FOLDER);

    return @filenames;
}

# because the directory is link symblic, and we can not get the script path by `pwd`
# so we do like this
sub get_datanode_var_parent() {
    my ($old_base) = @_;    # like /var/testing/_packages/datanode-2.4.0
    my $packages_path = dirname(dirname($old_base))."/packages";    # like /var/testing/packages

    my @filenames = getAllFilenamesFromFolder($packages_path);

    foreach my $name (@filenames) {
        if ($name =~ /datanode/) {
            my $project_path = "$packages_path/$name"; # like /var/testing/packages/datanode
            printf("get datanode project folder:[$project_path]\n");
            return $project_path;
        }
    }

    die "can't find datanode project folder, exit\n";
    return "";
}

