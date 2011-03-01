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
my $C_UNWRAP32_MAX_RATE = 9;
my $C_UNWRAP32_WRAP_T = 10;
my $C_UNWRAP32_WRAP_VAL = 11;
my $C_STATS_REF = 12;

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

  $self->[$C_UNWRAP32_MAX_RATE] = delete($args{'unwrap32_max_rate'});
  $self->[$C_UNWRAP32_WRAP_T] = delete($args{'unwrap32_wrap_t'});
  $self->[$C_UNWRAP32_WRAP_VAL] = delete($args{'unwrap32_wrap_val'});
  $self->[$C_STATS_REF] = delete($args{'stats_ref'});

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
    #if (defined($val)) { print "  storing $val in index $i\n"; }
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

  my $init = join (',',
		   $self->[$C_PERIOD],
		   $self->[$C_PERMIT_COVERAGE],
		   $self->[$C_MAX_DELTA_T] || '',
		   $self->[$C_MAX_RATE] || '',
		   $self->[$C_LAST_T] || '',
		   $self->[$C_LAST_V] || '',
		   $self->[$C_LAST_BUCKET_START] || '');
  # serialize bucket
  my $bucket_ref = $self->[$C_BUCKET];
  my @_blist = ();
  foreach my $item (@{ $bucket_ref }) {
    my ($a, $b) = @{ $item };
    push (@_blist, "$a/$b");
  }
  my $bucket = join (',', @_blist);
  # serialize results
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
  my ($self, $timestamp, $val) = @_;
  my $period = $self->[$C_PERIOD];
  my @results = $self->results();

  my $int_timestamp = int($timestamp);
  my $this_bucket_start = $int_timestamp - ($int_timestamp % $period);

  my $stats_ref = $self->[$C_STATS_REF];
  if (! defined ($stats_ref) ) {
    my %local_stats;
    $stats_ref = \%local_stats;
  }

  $stats_ref->{'count_samples'}++;
  # handle initialization
  if (! defined($self->[$C_LAST_T])) {
    $self->[$C_LAST_BUCKET_START] = $this_bucket_start;
    $self->_store_last_sample($timestamp, $val);
    $stats_ref->{'count_initialized'}++;
    return (@results);
  }
  my $delta_t = $timestamp - $self->[$C_LAST_T];
  my $delta_v = $val - $self->[$C_LAST_V];

  # handle bad timestamps
  if ($delta_t <= 0) {
    $stats_ref->{'count_bad_timestamps'}++;
    return (@results);
  }

  my $max_delta_t = $self->[$C_MAX_DELTA_T];
  if (defined ($max_delta_t) &&
      $delta_t > $max_delta_t) {
    $stats_ref->{'skipped_polls_total_delta_t'} += $delta_t;
    $stats_ref->{'count_skipped_polls_max_delta_t'}++;
    # and store this new (t,v) but do not calculate a rate
    $self->_store_last_sample($timestamp, $val);
    $self->_new_bucket($this_bucket_start);
    return (@results);
  }

  # handle counter wraps and resets (counter appears to go negative)
  if ($delta_v < 0) {
    $stats_ref->{'count_wraps'}++;
    # if no unwrap32 defined, then just reset the counter
    if (!$self->[$C_UNWRAP32_MAX_RATE]) {
      # TODO: refactor
      $self->_store_last_sample($timestamp, $val);
      $self->_new_bucket($this_bucket_start);
      return (@results);
    }
    # otherwise we want to unwrap, but test delta_t against wrap_t
    if ($delta_t > $self->[$C_UNWRAP32_WRAP_T]) {
      $stats_ref->{'count_unwrap_failed_double_wrap'}++;
      print STDERR "Failed: delta_t is $delta_t which double wraps with wrap_t of " . $self->[$C_UNWRAP32_WRAP_T] . "\n";
      # TODO: refactor
      $self->_store_last_sample($timestamp, $val);
      $self->_new_bucket($this_bucket_start);
      return (@results);
    }
    # otherwise, we will try to unwrap the counter
    # add the value lost due to wrap
    if (defined ($self->[$C_UNWRAP32_WRAP_VAL])) {
      $delta_v += $self->[$C_UNWRAP32_WRAP_VAL];
    } else {
      $delta_v += 4294967295;
    }
    my $test_rate = $delta_v / $delta_t;
    my $test_percent = $test_rate / $self->[$C_UNWRAP32_MAX_RATE] * 100.0;
    if ($test_percent > 103 || $test_rate < 0) {
      print STDERR "Failed: test_percent=$test_percent, (delta_v=$delta_v, delta_t=$delta_t) test_rate=$test_rate, unwrap max rate: " . $self->[$C_UNWRAP32_MAX_RATE] . "\n";
      $stats_ref->{'count_unwrap_failed_max_rate'}++;
      # TODO: refactor
      $self->_store_last_sample($timestamp, $val);
      $self->_new_bucket($this_bucket_start);
      return (@results);
    }
    # otherwise, the value is good
    # allow $delta_v to pass through with its new unwrapped value
    $stats_ref->{'count_unwrap_okay'}++;
    #print STDERR "Unwrapped counter at $timestamp $val, rate = $test_rate\n";
  }
  my $this_rate = $delta_v / $delta_t;
  #print "THIS RATE: $this_rate\n";
  my $max_rate = $self->[$C_MAX_RATE];
  if (defined ($max_rate) &&
      $this_rate > $max_rate) {
    $stats_ref->{'count_skipped_polls_max_rate'}++;
    $self->_store_last_sample($timestamp, $val);
    $self->_new_bucket($this_bucket_start);
    return (@results); # nothing
  }
  # should be a "while" loop here I think... to convert from generator semantics
  # handle case where we crossed into a new bucket
  if ($this_bucket_start != $self->[$C_LAST_BUCKET_START]) {
    my $overlap = $period - ($self->[$C_LAST_T] - $self->[$C_LAST_BUCKET_START]);
    my $bucket_coverage;
    if ($overlap > 0) {
      $bucket_coverage = $overlap / $period;
      $self->_bucket_append( $this_rate, $bucket_coverage);
    }
    # calculate total bucket value
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
    # advance to next bucket
    $self->_new_bucket($self->[$C_LAST_BUCKET_START] + $period);
    while ($self->[$C_LAST_BUCKET_START] != $this_bucket_start) {
      # generate rates for intermediate missing buckets
      $self->_keep_result ($self->[$C_LAST_BUCKET_START], $this_rate);
      $self->[$C_LAST_BUCKET_START] += $period;
    }
    $overlap = $timestamp - $this_bucket_start;
    if ($overlap > 0) {
      $bucket_coverage = $overlap / $period;
      $self->_bucket_append( $this_rate, $bucket_coverage );
    }
    # ok, handled previous buckets, whether they had data in them or not
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

