package CounterProcessing;
sub new {
    my $this = shift;
    my %args = @_;
    my $class = ref($this) || $this;
    my $self = {};

    $self->{'period'} = delete($args{'period'});
    $self->{'permit_coverage'} = delete($args{'permit_coverage'});

    bless ($self, $class);
    return ($self);
}

sub _store_last_sample {
    my ($self, $timestamp, $val) = @_;
    $self->{'last_t'} = $timestamp;
    $self->{'last_v'} = $val;
}

sub _new_bucket {
    my ($self, $bucket_start) = @_;
    $self->{'bucket'} = [];
    $self->{'last_bucket_start'} = $bucket_start;
}

sub _bucket_append {
    my ($self, $rate, $percent) = @_;
    print "Bucket add: $rate covers " . 100.0*$percent . " percent of interval\n";
    push (@{ $self->{'bucket'} }, [ $rate, $percent ] );
}

sub _keep_result {
    my ($self, $timestamp, $val) = @_;
    if (! defined ($self->{'results'})) {
	$self->{'results'} = [];
    }
    #print "KEPT RESULT: $timestamp, $val\n";
    push (@{ $self->{'results'} }, [ $timestamp, $val ]);
}

sub results {
    my ($self) = @_;
    my $res_ref;
    if (! defined ($res_ref = $self->{'results'})) {
	return (0, undef);
    }
    return (scalar (@{ $res_ref }), $res_ref);
}

sub new_count {
    my ($self, $timestamp, $val) = @_;
    my $period = $self->{'period'};
    my ($results, $res_ref) = $self->results();

    my $this_bucket_start = $timestamp - ($timestamp % $period);

    if (! defined($self->{'last_t'})) {
	$self->{'stats'}->{'count_samples'}++;
	$self->{'last_bucket_start'} = $this_bucket_start;
	$self->_store_last_sample($timestamp, $val);
	return ($results, $res_ref);
    }
    my $delta_t = $timestamp - $self->{'last_t'};
    my $delta_v = $val - $self->{'last_v'};
    if ($delta_t <= 0) {
	$self->{'stats'}->{'count_bad_timestamps'}++;
	return ($results, $res_ref);
    }
    if (defined (my $max_delta_t = $self->{'max_delta_t'}) &&
	$delta_t > $max_delta_t) {
	$self->{'stats'}->{'skipped_delta_t'} += $delta_t;
	$self->{'stats'}->{'skipped_delta_v'} += $delta_v;
	$self->_store_last_sample($timestamp, $val);
	$self->_new_bucket($this_bucket_start);
	return ($results, $res_ref);
    }
    if ($delta_v < 0) {
	$self->{'stats'}->{'count_wraps'}++;
	$self->_store_last_sample($timestamp, $val);
	$self->_new_bucket($this_bucket_start);
	return ($results, $res_ref);
    }
    $self->{'count_samples'}++;
    my $this_rate = $delta_v / $delta_t;
    #print "THIS RATE: $this_rate\n";
    if (defined (my $max_rate = $self->{'max_rate'}) &&
	$rate > $max_rate) {
	$self->{'stats'}->{'count_rate_too_large'}++;
	$self->_store_last_sample($timestamp, $val);
	$self->_new_bucket($this_bucket_start);
	return ($results, $res_ref);
    }
    # should be a "while" loop here I think... to convert from generator semantics
    if ($this_bucket_start != $self->{'last_bucket_start'}) {
	my $overlap = $period - ($self->{'last_t'} - $self->{'last_bucket_start'});
	my $bucket_coverage;
	if ($overlap > 0) {
	    $bucket_coverage = $overlap / $period;
	    $self->_bucket_append( $this_rate, $bucket_coverage);
	}
	my $sum = 0;
	my $sum_percent = 0;
	foreach my $b (@{ $self->{'bucket'} }) {
	    my ($b_val, $b_percent) = @{ $b };
	    my $weighted_val = $b_val * $b_percent;
	    $sum += $weighted_val;
	    $sum_percent += $b_percent;
	}
	if ($sum_percent >= $self->{'permit_coverage'}) {
	    $self->_keep_result ($self->{'last_bucket_start'}, $sum);
	} else {
	    print "Skipped bucket " . $self->{'last_bucket_start'} . " which summed to only " . $sum_percent . " percent\n";
	}
	$self->_new_bucket($self->{'last_bucket_start'} + $period);
	while ($self->{'last_bucket_start'} != $this_bucket_start) {
	    $self->_keep_result ($self->{'last_bucket_start'}, $this_rate);
	    $self->{'last_bucket_start'} += $period;
	}
	$overlap = $timestamp - $this_bucket_start;
	if ($overlap > 0) {
	    $bucket_coverage = $overlap / $period;
	    $self->_bucket_append( $this_rate, $bucket_coverage );
	}
	$self->_store_last_sample($timestamp, $val);
	return ($self->results());
    }
    if ($this_bucket_start == $self->{'last_bucket_start'}) {
	my $bucket_coverage = ($timestamp - $self->{'last_t'}) / $period;
	$self->_bucket_append( $this_rate, $bucket_coverage );
    }
    $self->_store_last_sample($timestamp, $val);
    return ($self->results());
}

1;

#-----------------------------------------------------------------------
package CounterProcessing2;

# Choices here:
# 1A: One Object per counter (heavy?)
# 1B: Multiple counters per object (slimmer, needs name or ID hashing)
# 1C: use inner class?


sub new {
    my $this = shift;
    my %args = @_;
    my $class = ref($this) || $this;
    my $self = {};

    my %defaults = ( 'permit_coverage' => 0.999,
		     'ignore_zeroes' => 0,
		     'max_delta_t' => undef,
		     'max_rate' => undef,
		     'period' => undef
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

    my %Stats;
    $self->{'stats'} = \%Stats;


    bless ($self, $class);
    return ($self);
}




1;
