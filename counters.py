#!/usr/bin/python
from __future__ import division
import sys
from random import random

# pollute local namespace to save time
#from othercounters import *
from testcounters import *

def debug_print(str,  format_tuple):
    print str % format_tuple

def no_print(*args):
    pass

def update_debug(state):
    if (state):
        dprint = debug_print
    else:
        dprint = no_print
    return dprint

dprint = update_debug(True)


class Counter_NoInterpolation(object):
    def __init__(self,  period, skip_dupes=False,  permit_coverage=None,  ignore_zeroes=False):
        #deal with args
        self.period = period
        self.ignore_zeroes = ignore_zeroes
        self.skip_dupes=skip_dupes
        # Set up intitial state
        self.data = []
        self.last_t = None
        self.last_v = None
        # init stats
        self.count_samples = 0
        self.count_wraps = 0
        self.count_bad_timestamps = 0

    def new_count(self,  timestamp,  value):
        # shorthand names for input args, and grab period into local var here
        t = timestamp
        v = value
        if (v==0 and self.ignore_zeroes):
            return len(self.data)
        period = self.period
        this_bucket_start = t - (t  % period)
        dprint ("C2:Timestamp: %d   Value: %d   ... this bucket is [[%d]]",  (t,  v,  this_bucket_start))
        # handle initialization
        if self.last_t is None:
            self.last_t = t
            self.last_v = v
            self.count_samples += 1
            self.last_bucket_start = this_bucket_start
            return len(self.data)
        # skip bad data
        delta_t = t - self.last_t
        delta_v = v - self.last_v
        if delta_t <= 0:
            print "  C2: Warning, timestamp %d is invalid coming after previous timestamp %d. Ignoring this sample.",  (t,  self.last_t)
            self.count_bad_timestamps += 1
            return len(self.data)
        self.last_t = t
        self.last_v = v
        if delta_v < 0:
            print "  C2: Warning, counter reset or wrapped from value %d to %d in %d seconds", (self.last_v,  v,  delta_t)
            self.last_bucket_start =this_bucket_start
            self.count_wraps += 1
            return len(self.data)
        self.count_samples += 1
        this_rate = delta_v / delta_t
        dprint ("  C2: this rate is %.2f",  (this_rate))
        self.data.append( (this_bucket_start,  this_rate)) # demonstrably wrong!
        return (len(self.data))

    def get_rates(self):
        d = self.data
        self.data = []
        last_t = None
        for i in d:
            (t, v)=i
            if last_t is not None:
                if self.skip_dupes and t == last_t:
                    continue
                else:
                    last_t = t
            else:
                last_t = t
            yield (i)


class Counter(object):
    def __init__(self,
                 period,
                 permit_coverage=0.999,
                 ignore_zeroes=False,
                 max_delta_t=None,
                 max_rate=None):
        #deal with args
        self.period = period
        self.permit_coverage = permit_coverage
        self.ignore_zeroes = ignore_zeroes
        self.max_dt = max_delta_t
        self.max_rate = max_rate
        # Set up intitial state
        self.last_t = None
        self.last_v = None
        self.last_bucket_start = None
        self.bucket = []
        # init stats
        self.count_samples = 0
        self.count_wraps = 0
        self.count_bad_timestamps = 0

    def _store_last_sample(self,  timestamp,  value):
        self.last_t = timestamp
        self.last_v = value

    def _new_bucket(self,  bucket_start):
        self.bucket =[]
        self.last_bucket_start = bucket_start

    def new_count(self,  timestamp,  value):
        # shorthand names for input args, and grab period into local var here
        t = timestamp
        v = value
        if (v==0 and self.ignore_zeroes):
            # this has to evolve more
            raise StopIteration
        period = self.period
        this_bucket_start = t - (t  % period)
        dprint ("Timestamp: %d   Value: %d   ... this bucket is [[%d]]",  (t,  v,  this_bucket_start))
        # handle initialization
        if self.last_t is None:
            self.count_samples += 1
            self._store_last_sample (t, v)
            self.last_bucket_start = this_bucket_start
            raise StopIteration
        # skip bad data
        delta_t = t - self.last_t
        delta_v = v - self.last_v
        if delta_t <= 0:
            dprint ("  Warning, timestamp %d is invalid coming after previous timestamp %d. Ignoring this sample.",  (t,  self.last_t))
            self.count_bad_timestamps += 1
            raise StopIteration
        # possibly store last  t and v here? instead of everywhere else?
        if self.max_dt and (delta_t > self.max_dt):
            self.count_bad_timestamps += 1
            dprint ("  Warning, timestamp %d has delta_t of %d, which is larger than max permitted.",  (t,  delta_t))
            # and store this new (t,v) but do not calculate a rate
            self._store_last_sample (t, v)
            self._new_bucket (this_bucket_start)
            raise StopIteration
        # initialized now, but handle counter wraps and resets (counter appears to go negative)
        if delta_v < 0:
            self.count_wraps += 1
            dprint ("  Warning, counter reset or wrapped from value %d to %d in %d seconds",  (self.last_v,  v,  delta_t))
            self._store_last_sample(t,  v)
            self._new_bucket (this_bucket_start)
            raise StopIteration
        self.count_samples += 1
        this_rate = delta_v / delta_t
        dprint ("  this rate is %.2f",  (this_rate))
        if self.max_rate and this_rate > self.max_rate:
            dprint ("ERROR: rate calculated at time %d is %.2f which is larger than max permitted rate of %.2f",  (t,  this_rate,  self.max_rate))
            self._store_last_sample (t, v)
            self._new_bucket (this_bucket_start)
            raise StopIteration
        # handle case where we crossed into a new bucket
        if this_bucket_start != self.last_bucket_start:
            # first, determine how much coverage we have on the last bucket
            overlap = period - (self.last_t - self.last_bucket_start)
            if (overlap > 0):
                dprint ("  overlap on old bucket [%d] is %d seconds",  (self.last_bucket_start,  overlap))
                bucket_coverage = overlap / period
                self.bucket.append ( (this_rate,  bucket_coverage) ) # could just be an accumulator
                dprint ("  Poll at timestamp %d covers %.2f%% of OLD bucket %d with rate: %.2f",  (t,  (100*bucket_coverage),  self.last_bucket_start,  this_rate))
            # calculate total bucket value
            sum = 0
            sum_percent = 0
            for (b_val,  b_percent) in self.bucket:
                weighted_val = b_val * b_percent
                sum += weighted_val
                sum_percent += b_percent
                ###dprint ("  [%d] B_percent: %.2f   B_val: %.2f  (weighted=%.2f)",  (self.last_bucket_start,  b_percent,  b_val,  weighted_val))
            dprint ("  [[%d]] rates summed to %.2f,  percentages summed to %.3f",  (self.last_bucket_start,  sum,  sum_percent))
            if (sum_percent >= self.permit_coverage):
                yield ( self.last_bucket_start,  sum )
            else:
                dprint ("ERROR: Skipped bucket [[%d]] which summed only to %.3f%%!",  (self.last_bucket_start,  sum_percent))
            # advance forward one bucket...
            self._new_bucket (self.last_bucket_start + period)
            while self.last_bucket_start != this_bucket_start:
                # generate rates for intermediate missing buckets
                yield ( self.last_bucket_start,  this_rate )
                ###self.data.append ( (self.last_bucket_start,  this_rate) )
                dprint ("   flat-interpolated bucket %d had overall rate of %.2f (flat-interpolation/counters)",  (self.last_bucket_start,  this_rate))
                self.last_bucket_start += period
            overlap = t - this_bucket_start
            dprint ("  left over overlap = %d, in bucket %d (t=%d)",  (overlap,  this_bucket_start,  t))
            bucket_coverage = overlap / period
            self.bucket.append ( (this_rate,  bucket_coverage)) # could just be an accumulator
            # ok, handled previous buckets, whether they had data in them or not
            self._store_last_sample (t,  v)
            raise StopIteration
        if this_bucket_start == self.last_bucket_start:
            bucket_coverage = (t - self.last_t) / period
            dprint ("  Poll at timestamp %d (delta_t=%d) covers %.2f%% of NEW bucket %d with rate: %.2f",  (t,  delta_t,  (100*bucket_coverage),  this_bucket_start,  this_rate))
            self.bucket.append ( (this_rate,  bucket_coverage) )
        self._store_last_sample (t,  v)
        raise StopIteration




# data that is supposed to be interval 60, but varies under-polled and overpolled
# list: [ (timestamp, value), ... ]
polled_data = [
               (0,  10),
               (1,  10),
               (30,  60),
               (41, 60),
               (60,  130),
               (130,  280),
               (190,  460),
               (240, 460),
               (241,  460),
               (250,  710),
               (305,  840),
               (470,  1034),
               (900,  1630)
               ]

# Set debug output state
dprint = update_debug(True)
print
c=Counter(period=60)
c_nointerp = Counter_NoInterpolation(period=60,  skip_dupes=False)
rd=[]
rd2=[]
for t, v in polled_data: # this turns into the function call to generate this dataset
        for result in c.new_count (t,  v):
            rd.append (result)
        ret2 = c_nointerp.new_count(t, v)
        if ret2:
            for t, v in c_nointerp.get_rates():
                rd2.append ( [t, v])


# check the integrated value
sum_rate = 0
sum_rate2 = 0
for time, rate in rd:
    sum_rate += (c.period*rate)
for time, rate in rd2:
    sum_rate2 += (c_nointerp.period*rate)
print "Summed rate1: %.2f" % sum_rate
print rd
print "\n"
print "Summed rate2: %.2f" % sum_rate2
print rd2
print "\n"

dprint = update_debug(False)
print "Dataset 1 - ideal"
# This is boilerplate for the most part!
period = 60
dataset1 = TestValues (num=400,  period=period,  avg_time_variance=100, time_variance="both",  gap_odds=0.05,  gap_avg_width=period*60, avg_rate=144,  avg_rate_variance=0.5,  random_seed=143)# ,  avg_rate_variance=0.20)
c1 = Counter(period=period)
r1 = []
integrated_sum = 0
count_samples = 0
count_rates = 0
for t, v in dataset1:
    count_samples+=1
    for result_t,  result_rate in c1.new_count (t,  v):
        this_isum = result_rate * period
        integrated_sum += this_isum
        count_rates+=1
        # not really needed:  r1.append (result)
        print "RATE: %20d: %8.2f" % (result_t, result_rate)
counter_sum = dataset1.counter
print "Completed processing %d input samples, and generated %d output rates." % (count_samples,  count_rates)
print "Total counter rise: %d, and integrated sum is: %.4f" % (counter_sum,  integrated_sum)
abs_error = abs(counter_sum - integrated_sum)
if counter_sum > 0:
    print "Absolute Error is: %.25f, and percent error is: %.25f%%" % (abs_error,  abs_error/counter_sum * 100.0)
else:
    print "Counter did not increase!  Error is zero.  (absolute error=%.25f)" % abs_error
print

sys.exit()

print "Data set 8 simulation(?)"
dataset8 = TestValues(num=40,  period=60,  avg_rate=100, avg_time_variance=0,    gap_odds=0.15, gap_avg_width=60*20)
c8 = Counter(period=60)
r8 = []
for t, v in dataset8:
    for result in c8.new_count (t,  v):
        r8.append (result)
        print "R8: %20d: %8.2f" % result
print


# Now perform the same experiment but using a random generator
dprint = update_debug(False)

sum_count = 0
v_sum = 0
t = 0
this_period = 60
c = Counter(period=this_period)
c2 = Counter_NoInterpolation(period=this_period)
output = []
output2 = []
max_runs = 200000
for i in xrange(max_runs):
    # for first and last datapoint, do not increase v_sum (i.e. dont add to it)
    if (max_runs-1) > i > 0:
        v_increase = int(200*random())
        v_sum += v_increase
    ret = c.new_count(timestamp=t,  value=v_sum)
    ret2 = c2.new_count(timestamp=t,  value=v_sum)
    # if we generated a datapoint(s) (ret=True) then pull them out and put them in output[]
    if (ret2):
        for t1,  v1 in c2.get_rates():
            output2.append ( [t1, v1])
    # make the last datapoint land on a period-interval exactly to force a final sample
    if i < (max_runs-1):
        t_increase = 1 + int(this_period+3-3*random())
    else:
        t_increase = this_period - (t % this_period)
    t += t_increase
extra_sum = 0
for b_r,  b_c in c.bucket:
    extra_sum += b_c * b_r
extra_sum *= c.period
print "C1: done with %d random counter increases, extra_sum = %.2f" % (max_runs, extra_sum)
sum_rate = 0
count_rate =0
for time, rate in output:
    sum_rate += (c.period * rate)
    count_rate += 1
avg_rate = sum_rate / count_rate
print "C1: queued up rates (count_rate=%d) add up to %.25f, but avg rate is %.25f" % (count_rate,  sum_rate,  avg_rate)
sum_rate += extra_sum
error = abs(sum_rate - v_sum)
error_rate = error / max_runs * 100
print "C1: Total converted rate sum is %.3f over %d counter updates and total counter sum is %d, Error=%.25f, error percent is: %.20f%%" % (sum_rate,  c.count_samples,  v_sum,  error,  error_rate)

print

# make function!
# no bucket for c2, skip extra_sum
sum_rate2 = 0
count_rate2 =0
for time, rate in output2:
    sum_rate2 += (c2.period * rate)
    count_rate2 += 1
avg_rate2 = sum_rate2 / count_rate2
print "C2: queued up rates (count_rate=%d) add up to %.25f, but avg rate is %.25f" % (count_rate2,  sum_rate2,  avg_rate2)
error2 = abs(sum_rate2 - v_sum)
error_rate2 = error2 / max_runs * 100
print "C2: Total converted rate sum is %.3f over %d counter updates and total counter sum is %d, Error=%.25f, error percent is: %.20f%%" % (sum_rate2,  c2.count_samples,  v_sum,  error2,  error_rate2)
