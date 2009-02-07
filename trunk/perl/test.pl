#!/usr/bin/perl -w -I.
use CounterProcessing; $|=1; print "\n" x 10;
my $cp = CounterProcessing->new('period'=>60, 'permit_coverage'=>0.999);
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
    if ($ret1) { print "  Got return val: $ret1,  ret2=$ret2\n"; }
}
my ($r1, $r2) = $c->results();
foreach my $d (@{ $r2 }) {
    print join (", ", @{ $d }) . "\n";
}

print "Done\n";
# [(0, 1.9999999999999998), (60, 2.1428571428571428), (120, 2.8571428571428572), (180, 0.5), (240, 6.1363636363636358), (300, 1.2747474747474747), (360, 1.1757575757575758), (420, 1.2108057317359644), (480, 1.386046511627907), (540, 1.386046511627907), (600, 1.386046511627907), (660, 1.386046511627907), (720, 1.386046511627907), (780, 1.386046511627907), (840, 1.386046511627907)]


# Next, check out the memory usage for 1,000,000 counter objects.
# 3.91549 KB per object (hashes based)
# converting to array based..
print "BEFORE making a million counters:\n";
show_mem();
my $x=0;
while ($x++ < 1000000) {
    my $c = $cp->get_counter('name'=>$x);
    if ($x % 100000 == 0) { print "  building (x=$x) "; show_mem(); }
}
print "AFTER:\n";
sleep (5);
show_mem();
exit(0);

sub show_mem {
    system ("grep VmRSS /proc/$$/status");
}    
