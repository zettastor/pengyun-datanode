#!/usr/bin/perl

use Log::Log4perl qw(get_logger);

my $conf = q(
    log4perl.category.Bar.Twix = WARN, Logfile
    log4perl.appender.Logfile = Log::Log4perl::Appender::File
    log4perl.appender.Logfile.filename = test.log
    log4perl.appender.Logfile.layout = Log::Log4perl::Layout::PatternLayout
    log4perl.appender.Logfile.layout.ConversionPattern = %d %F{1}(%L) > %m %n
);
    
Log::Log4perl::init(\$conf);
    
my $logger = get_logger("Bar::Twix");
$logger->error("Blah");
