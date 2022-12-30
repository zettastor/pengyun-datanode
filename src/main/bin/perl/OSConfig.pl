#!/usr/bin/perl
use FindBin '$RealBin';
use File::Basename qw(dirname);
use Cwd  qw(abs_path);
use lib dirname(dirname abs_path $0).'/bin';
use osversion;
use Common;

#sometimes raw module might not be loaded into kernel. run "modprobe raw" anyway
my $my_script_dir = $RealBin;
my $log = Log::Log4perl->get_logger("OSConfig");
$log->debug("starting");
if (osversion::is_ubuntu()) {
    my $cmd = "modprobe raw";
    (system($cmd) == 0) or exitProcess(__FILE__, __LINE__, "fail to modprobe raw module to the kernel");
    $log->debug("modprobe raw module is loaded to the kernel");
    sleep 2;
}


# change the max map count for pages
$cmd = "echo 67107840 > /proc/sys/vm/max_map_count";
(system($cmd) == 0) or exitProcess(__FILE__, __LINE__, "fail to change max_map_count, cmd:$cmd");
$log->debug("finished");

