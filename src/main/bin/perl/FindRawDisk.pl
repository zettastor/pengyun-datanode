#!/usr/bin/perl

use EnvironmentUtils;

my $envRoot = get_actual_environment_root();
my $raw_name = $ARGV[0];
die usage() unless ($raw_name =~ m/raw\d+/);

my $found = 0;
my $rule_dir = "$envRoot/var/storage/60-raw.rules";
open(OLDDEVLIST,"$rule_dir") or die("ERROR:open the file failed");
while(my $perLine = <OLDDEVLIST>){
    my @oldDevArray = split(/,/,$perLine);
    my @tempKey = split(/==/,$oldDevArray[3]);
    $tempKey[1]=~s/(^")|("$)//g;
    my $str = substr($oldDevArray[4],25);
    my @tempValue = split(/\s/,$str);
    print ("to compare $raw_name with ($tempValue[0], $tempKey[1])\n");
    if ($tempValue[0] eq $raw_name) {
        my $allDev = `fdisk -l 2>/dev/null | grep "Disk \/dev\/*" | grep -v "\/dev\/md" | awk ' {print \$2}' | sed -e 's/://g' | awk -F "[/:]" ' {print \$NF}'`;
        my @devArray = split(/[\r\n]/,"$allDev");
        foreach $devName(@devArray) {
            my $seri = `/lib/udev/scsi_id -g -u /dev/$devName`;
            chomp($seri);
            print ("to compare $tempKey[1] with $seri\n");
            if ($seri eq $tempKey[1]) {
                $found = 1;
                last;
            }
        }
        last;
    }
}
if ($found eq 0) {
    print "$raw_name is not found.\n";
} else {
    print "$raw_name is found.\n";
}
sub usage {
    print "Usage:\n\tperl FindRawDisk.pl raw1 | raw2 | raw3 ...\n";
}
