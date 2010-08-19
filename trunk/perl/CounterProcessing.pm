package Counter;

# I'm using an array ref for the base object type to keep memory usage low
my $C_PERIOD = 0;
my $C_PERMIT_COVERAGE = 1;
my $C_MAX_DELTA_T = 2;
my $C_MAX_RATE = 3;
my $C_LAST_T = 4;
my $C_LAST_V = 5;
my $C_LAST_BUCKET_START = 6;
my $C_BUCKET = 7; # listref
my $C_RESULTS = 8; # listref

sub new {
  my $this = shift;
  my %args = @_;
  my $class = ref($this) || $this;
  my $self = []; # using an array reference to lessen memory usage

  # not much constructor validation here
  $self->[$C_PERIOD] = delete($args{'period'});
  $self->[$C_PERMIT_COVERAGE] = delete($args{'permit_coverage'});
  $self->[$C_MAX_DELTA_T] = delete($args{'max_delta_t'});
  $self->[$C_MAX_RATE] = delete($args{'max_rate'});
	
  bless ($self, $class);
  return ($self);
}

# parse a serialized counter and return a new Counter object initialized
# to the frozen state
sub from_string {
  my $this = shift;
  my ($str) = @_;
  my $class = ref($this) || $this;
  my $self = [];

  my ($init, $bucket_str, $results_str) = split(/\^/, $str);
  my @parts = split(/\,/, $init);

  # not much validation here
  # copy the scalar values first
  my $i = 0;
  while ($i <= $C_LAST_BUCKET_START) {
    my $val = $parts[$i];
    #if ($val eq "") { $val = undef; }
    $self->[$i] = $val;
    if (defined($val)) { print "  storing $val in index $i\n"; }
    $i++;
  }
  # then process the bucket and the results lists
  # bucket:
  $self->[$C_BUCKET] = [];
  my @bucket_list  = split(/,/, $bucket_str);
  foreach my $item (@bucket_list) {
    my ($a, $b) = split (/\//, $item);
    push (@{ $self->[$C_BUCKET] }, [ $a, $b ] );
  }
  # results:
  $self->[$C_RESULTS] = [];
  my @results_list = split(/,/, $results_str);
  foreach my $item (@results_list) {
    my ($a, $b) = split (/\//, $item);
    push (@{ $self->[$C_RESULTS] }, [ $a, $b ] );
  }

  bless ($self, $class);
  return ($self);
}

sub _store_last_sample {
  my ($self, $timestamp, $val) = @_;
  $self->[$C_LAST_T] = $timestamp;
  $self->[$C_LAST_V] = $val;
}

sub _new_bucket {
  my ($self, $bucket_start) = @_;
  $self->[$C_BUCKET] = [];
  $self->[$C_LAST_BUCKET_START] = $bucket_start;
}

sub _bucket_append {
  my ($self, $rate, $percent) = @_;
  #print "Bucket add: $rate covers " . 100.0*$percent . " percent of interval\n";
  push (@{ $self->[$C_BUCKET] }, [ $rate, $percent ] );
}

sub _keep_result {
  my ($self, $timestamp, $val) = @_;
  if (! defined ($self->[$C_RESULTS])) {
    $self->[$C_RESULTS] = [];
  }
  push (@{ $self->[$C_RESULTS] }, [ $timestamp, $val ]);
}

# return a string that is parsable by from_string() that represents
# the complete current state of this counter, for persistence
# Serialized c: 60,0.999,,^2.36363636363636/0.0833333333333333^240/6.13636363636364
sub to_string {
  my ($self) = @_;

  my $last_t = $self->[$C_LAST_T] || '';
  my $last_v = $self->[$C_LAST_V] || '';
  my $last_bucket_start = $self->[$C_LAST_BUCKET_START] || '';
  my $init = join (',',
		   $self->[$C_PERIOD],
		   $self->[$C_PERMIT_COVERAGE],
		   $self->[$C_MAX_DELTA_T] || '',
		   $self->[$C_MAX_RATE] || '',
		   $last_t, $last_v, $last_bucket_start);
  my $bucket_ref = $self->[$C_BUCKET];
  my @_blist = ();
  foreach my $item (@{ $bucket_ref }) {
    my ($a, $b) = @{ $item };
    push (@_blist, "$a/$b");
  }
  my $bucket = join (',', @_blist);
  my $results_ref = $self->[$C_RESULTS];
  my @_reslist = ();
  foreach my $item (@{ $results_ref }) {
    my ($a, $b) = @{ $item };
    push (@_reslist, "$a/$b");
  }
  my $results = join (',', @_reslist);
  my $str = join ('^', $init, $bucket, $results);
  return ($str);
}

sub results {
  my ($self) = @_;
  my $res_ref;
  if (! defined ($res_ref = $self->[$C_RESULTS])) {
    return (0, undef);
  }
  return (scalar (@{ $res_ref }), $res_ref);
}

sub new_count {
  my ($self, $timestamp, $val, $stats_ref) = @_;
  my $period = $self->[$C_PERIOD];
  my @results = $self->results();

  my $this_bucket_start = $timestamp - ($timestamp % $period);

  $stats_ref->{'count_samples'}++;
  if (! defined($self->[$C_LAST_T])) {
    $self->[$C_LAST_BUCKET_START] = $this_bucket_start;
    $self->_store_last_sample($timestamp, $val);
    $stats_ref->{'count_initialized'}++;
    return (@results);
  }
  my $delta_t = $timestamp - $self->[$C_LAST_T];
  my $delta_v = $val - $self->[$C_LAST_V];
  if ($delta_t <= 0) {
    $stats_ref->{'count_bad_timestamps'}++;
    return (@results);
  }
  if (defined (my $max_delta_t = $self->[$C_MAX_DELTA_T]) &&
      $delta_t > $max_delta_t) {
    $stats_ref->{'skipped_polls_total_delta_t'} += $delta_t;
    #$stats_ref->{'skipped_total_delta_v'} += $delta_v;
    $stats_ref->{'count_skipped_polls_max_delta_t'}++;
    $self->_store_last_sample($timestamp, $val);
    $self->_new_bucket($this_bucket_start);
    return (@results);
  }
  if ($delta_v < 0) {
    $stats_ref->{'count_wraps'}++;
    $self->_store_last_sample($timestamp, $val);
    $self->_new_bucket($this_bucket_start);
    return (@results);
  }
  my $this_rate = $delta_v / $delta_t;
  #print "THIS RATE: $this_rate\n";
  if (defined (my $max_rate = $self->[$C_MAX_RATE]) &&
      $rate > $max_rate) {
    $stats_ref->{'count_skipped_polls_max_rate'}++;
    $self->_store_last_sample($timestamp, $val);
    $self->_new_bucket($this_bucket_start);
    return ($results, $res_ref);
  }
  # should be a "while" loop here I think... to convert from generator semantics
  if ($this_bucket_start != $self->[$C_LAST_BUCKET_START]) {
    my $overlap = $period - ($self->[$C_LAST_T] - $self->[$C_LAST_BUCKET_START]);
    my $bucket_coverage;
    if ($overlap > 0) {
      $bucket_coverage = $overlap / $period;
      $self->_bucket_append( $this_rate, $bucket_coverage);
    }
    my $sum = 0;
    my $sum_percent = 0;
    foreach my $b (@{ $self->[$C_BUCKET] }) {
      my ($b_val, $b_percent) = @{ $b };
      my $weighted_val = $b_val * $b_percent;
      $sum += $weighted_val;
      $sum_percent += $b_percent;
    }
    if ($sum_percent >= $self->[$C_PERMIT_COVERAGE]) {
      $self->_keep_result($self->[$C_LAST_BUCKET_START], $sum);
    } else {
      $stats_ref->{'count_skipped_results_coverage_too_low'}++;
      #    print "Skipped bucket " . $self->[$C_LAST_BUCKET_START] . " which summed to only " . $sum_percent . " percent\n";
    }
    $self->_new_bucket($self->[$C_LAST_BUCKET_START] + $period);
    while ($self->[$C_LAST_BUCKET_START] != $this_bucket_start) {
      $self->_keep_result ($self->[$C_LAST_BUCKET_START], $this_rate);
      $self->[$C_LAST_BUCKET_START] += $period;
    }
    $overlap = $timestamp - $this_bucket_start;
    if ($overlap > 0) {
      $bucket_coverage = $overlap / $period;
      $self->_bucket_append( $this_rate, $bucket_coverage );
    }
    $self->_store_last_sample($timestamp, $val);
    return ($self->results());
  }

  if ($this_bucket_start == $self->[$C_LAST_BUCKET_START]) {
    my $bucket_coverage = ($timestamp - $self->[$C_LAST_T]) / $period;
    $self->_bucket_append( $this_rate, $bucket_coverage );
  }
  $self->_store_last_sample($timestamp, $val);
  return ($self->results());
}

1;

#-----------------------------------------------------------------------
package CounterProcessing;

# Choices here:
# 1A: One Object per counter (heavy?)
# 1B: Multiple counters per object (slimmer, needs name or ID hashing)
# 1C: use inner class?


sub new {
  my $this = shift;
  my %args = @_;
  my $class = ref($this) || $this;
  my $self = {};

  my %Stats;
  my %defaults = ( 'permit_coverage' => 0.999,
		   'ignore_zeroes' => 0,
		   'max_delta_t' => undef,
		   'max_rate' => undef,
		   'period' => undef,
		   'stats' => \%Stats
		 );

  # Copy either a passed-in value or use the defaults hash for initialization
  foreach my $k (keys %defaults) {
    if (defined ($args{$k})) {
      $self->{$k} = delete ($args{$k});
    } else {
      $self->{$k} = delete ($defaults{$k});
    }
  }

  if (! defined ($self->{'period'})) {
    warn __PACKAGE__ . ": You must specify \"period\" in the constructor, units are seconds.\n";
    return (undef);
  }
  if ($self->{'period'} <= 0 ||
      $self->{'period'} != int($self->{'period'})) {
    warn __PACKAGE__ . ": Bad period (" . $self->{'period'} ."): Period must be an integer larger than zero, units are seconds.\n";
  }

  my %Counters;
  $self->{'counters'} = \%Counters;

  bless ($self, $class);
  return ($self);
}

sub get_counter {
  my $self = shift;
  my %args = @_;
  my $name = delete ($args{'name'});
  if (! defined ($name)) {
    warn __PACKAGE__ . ": Missing required argument, counter name. May be a string or integer or obejct reference.\n";
    return (undef);
  }
  my $c_ref = $self->{'counters'}->{$name};
  if (defined ($c_ref)) {
    # should this perhaps be an error? or trigger a counter reset?
    return ($c_ref);
  }
  my $max_rate = delete($args{'max_rate'}) || $self->{'max_rate'};
  my $max_delta_t = delete($args{'max_delta_t'}) || $self->{'max_delta_t'};
  my $permit_coverage = delete($args{'permit_coverage'}) || $self->{'permit_coverage'};
  my $period = delete($args{'period'}) || $self->{'period'};

  my $c = Counter->new('period'          => $period,
		       'permit_coverage' => $permit_coverage,
		       'max_delta_t'     => $max_delta_t,
		       'max_rate'        => $max_rate
		      );
  $self->{'counters'}->{$name} = $c;
  return ($c);
}

sub load_counter {
  my ($self, $name, $counter) = @_;
  $self->{'counters'}->{$name} = $counter;
}

sub counter_exists {
  my ($self, $name) = @_;
  if (defined ($self->{'counters'}->{$name})) {
    return (1);
  }
  return (0);
}

sub update_counter {
  my ($self, $name, $timestamp, $value) = @_;
  my $c = $self->get_counter('name'=>$name);
  if (! defined ($c)) {
    return (undef);
  }
  my $stats_ref = $self->{'stats'};
  return ($c->new_count($timestamp, $value, $stats_ref));
}

sub to_filedesc {
  my ($self, $fh) = @_;
  my @counters;
  foreach my $name (sort keys %{ $self->{'counters'} }) {
    my $cref = $self->{'counters'}->{$name};
    my $serial = $cref->to_string();
    print $fh "$name>>$serial\n";
  }
}

1;
