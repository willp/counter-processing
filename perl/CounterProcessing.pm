package CounterProcessing;

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
    return ($c_ref);
  }
  my $max_rate = delete($args{'max_rate'}) || $self->{'max_rate'};
  my $max_delta_t = delete($args{'max_delta_t'}) || $self->{'max_delta_t'};
  my $permit_coverage = delete($args{'permit_coverage'}) || $self->{'permit_coverage'};
  my $period = delete($args{'period'}) || $self->{'period'};

  # unwrap 32 bit counter parameters here
  my $unwrap32_max_rate = delete($args{'unwrap32_max_rate'}) || 0;
  my $wrap_t = undef;
  if ($unwrap32_max_rate) {
    # compute wrap_t, once per counter
    $wrap_t = (2**32) / $unwrap32_max_rate;
  }

  my $c = Counter->new('period'          => $period,
		       'permit_coverage' => $permit_coverage,
		       'max_delta_t'     => $max_delta_t,
		       'max_rate'        => $max_rate,
		       # counter unwrapping parameters here
		       'unwrap32_max_rate' => $unwrap32_max_rate,
		       'unwrap32_wrap_t'   => $wrap_t
		      );
  $self->{'counters'}->{$name} = $c if (defined ($c));
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
  my $c = $self->get_counter('name' => $name);
  if (! defined ($c)) {
    return (undef);
  }
  my $stats_ref = $self->{'stats'};
  return ($c->new_count($timestamp, $value, $stats_ref));
}

sub to_filedesc {
  my ($self, $fh) = @_;
  foreach my $name (sort keys %{ $self->{'counters'} }) {
    my $cref = $self->{'counters'}->{$name};
    my $serial = $cref->to_string();
    print $fh "$name >> $serial\n";
  }
}

1;
