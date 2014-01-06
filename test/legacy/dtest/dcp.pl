#!/usr/bin/perl
use warnings;

my @nodes = (1);
my @size = ('100G', '1T');
my $files = 1000;
my $DATA;
open($DATA, '>', "data.csv") or die "can't open file for writing";
print $DATA "Throughput DCP Atlas $files files\n";
foreach my $size (@size){
foreach my $node (@nodes){
	print "running test with $size of files on $node nodes.\n";
	my $procs = $node*16;
	@output = qx(python dtest --stock --sparse -s $size -c $files dcp_test/dcp);
        print $DATA "$size,@output";	
}
}
