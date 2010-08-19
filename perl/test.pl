#!/usr/bin/perl -w -I.
use CounterProcessing; $|=1; print "\n" x 10;
my $cp = CounterProcessing->new('period'=>60,
				'permit_coverage'=>0.999,
				'max_delta_t' => 1800,
				'max_rate' => 5*10^9);
my $c = $cp->get_counter('name' => 'test'); # ? 'period' => 60, 'permit_coverage' => 0.999);
#my $c = Counter->new('period' => 60, 'permit_coverage' => 0.999);
my @data = (
    [0,    10],
    [1,    10],
    [30,   60],
    [41,   60],
    [60,   130],
    [130,  280],
    [190,  460],
    [240,  460],
    [250,  710],
    [305,  840],
    [470, 1034],
    [900, 1630]
    );

foreach my $d (@data) {
    my ($d_t, $d_v) = ($d->[0], $d->[1] );
    print "Submitting ($d_t,$d_v)\n";
    my ($ret1, $ret2) = $c->new_count ($d_t, $d_v);
    my $serial = $c->to_string();
    print "\nSerialized c: $serial\n\n";
    my $new_c = Counter->from_string($serial);
#    print "reloading into new counter: " . $new_c->to_string() . "\n";
    $cp->load_counter("test", $new_c);
    if ($ret1) {
	print "  Got return val: $ret1,  ret2=$ret2\n";
	while ($ret1-- > 0) {
	    my ($t, $v) = @{ shift(@{ $ret2 })};
	    print " POPPED: $t, $v\n";
	}
    }
}
print "\nSerialized c: " . $c->to_string() . "\n\n";
my ($r1, $r2) = $c->results();
foreach my $d (@{ $r2 }) {
    print join (", ", @{ $d }) . "\n";
}

print "Done\n";
# [(0, 1.9999999999999998), (60, 2.1428571428571428), (120, 2.8571428571428572), (180, 0.5), (240, 6.1363636363636358), (300, 1.2747474747474747), (360, 1.1757575757575758), (420, 1.2108057317359644), (480, 1.386046511627907), (540, 1.386046511627907), (600, 1.386046511627907), (660, 1.386046511627907), (720, 1.386046511627907), (780, 1.386046511627907), (840, 1.386046511627907)]

# Next, check out the memory usage for 2,000,000 counter objects.
# uninitialized object sizes...
# 263 bytes per object when counter are hashrefs
# 210 bytes per object w/ arrayrefs. 20.2% smaller with arrayrefs. worthwhile IMHO
#exit(0);
print "BEFORE making a zillion counters:\n";
show_mem();
my $x=0;
my $max = 10000;
while ($x++ < $max) {
    my $c = $cp->get_counter('name' => "counter_$x");
    $c->new_count ( 0, $x );
    $c->new_count ( 120, $x*2 );
    $c->new_count ( 239.5, $x*3 );
    if ($x % ($max/20) == 0) { print "  building (x=$x) "; show_mem(); }
}
print "AFTER:\n";
show_mem();
print "Persistence:\n";
print $cp->to_filedesc(STDOUT);
exit(0);

sub show_mem {
  # cheesy
  system ("grep VmRSS /proc/$$/status");
}
