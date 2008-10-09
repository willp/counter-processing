#!/usr/bin/python
from __future__ import division
import sys

# pollute local namespace to save time
from othercounters import *
from testcounters import *

def debug_print(str,  format_tuple):
    print str % format_tuple

def no_print(*args):
    pass

def update_debug(state):
    if state:
        dprint = debug_print
    else:
        dprint = no_print
    return dprint

dprint = update_debug(True)

class Counter(object):
    def __init__(self,
                 period,
                 permit_coverage = 0.999,
                 ignore_zeroes = False,
                 max_delta_t = None,
                 max_rate = None):
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
        self.skipped_delta_t = 0
        self.skipped_delta_v = 0
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
        if v==0 and self.ignore_zeroes:
            # this has to evolve more
            raise StopIteration
        period = self.period
        this_bucket_start = t - (t  % period)
        dprint("Timestamp: %d   Value: %d   ... this bucket is [[%d]]",  (t,  v,  this_bucket_start))
        # handle initialization
        if self.last_t is None:
            self.count_samples += 1
            self.last_bucket_start = this_bucket_start
            self._store_last_sample(t, v)
            raise StopIteration
        # skip bad data
        delta_t = t - self.last_t
        delta_v = v - self.last_v
        if delta_t <= 0:
            dprint("  Warning, timestamp %d is invalid coming after previous timestamp %d. Ignoring this sample.",  (t,  self.last_t))
            self.count_bad_timestamps += 1
            raise StopIteration
        # possibly store last  t and v here? instead of everywhere else?
        if self.max_dt and (delta_t > self.max_dt):
            self.count_bad_timestamps += 1
            self.skipped_delta_t += delta_t # track for later verification
            self.skipped_delta_v += delta_v
            dprint("  Warning, timestamp %d has delta_t of %d, which is larger than max permitted.",  (t,  delta_t))
            # and store this new (t,v) but do not calculate a rate
            self._store_last_sample(t, v)
            self._new_bucket(this_bucket_start)
            raise StopIteration
        # handle counter wraps and resets (counter appears to go negative)
        if delta_v < 0:
            self.count_wraps += 1
            dprint("  Warning, counter reset or wrapped from value %d to %d in %d seconds",  (self.last_v,  v,  delta_t))
            self._store_last_sample(t, v)
            self._new_bucket(this_bucket_start)
            raise StopIteration
        self.count_samples += 1
        this_rate = delta_v / delta_t
        dprint("  this rate is %.2f",  (this_rate))
        if self.max_rate and this_rate > self.max_rate:
            dprint("ERROR: rate calculated at time %d is %.2f which is larger than max permitted rate of %.2f",  (t,  this_rate,  self.max_rate))
            self._store_last_sample(t, v)
            self._new_bucket(this_bucket_start)
            raise StopIteration
        # handle case where we crossed into a new bucket
        if this_bucket_start != self.last_bucket_start:
            # first, determine how much coverage we have on the last bucket
            overlap = period - (self.last_t - self.last_bucket_start)
            if overlap > 0:
                dprint("  overlap on old bucket [%d] is %d seconds",  (self.last_bucket_start,  overlap))
                bucket_coverage = overlap / period
                self.bucket.append((this_rate,  bucket_coverage)) # could just be an accumulator
                dprint("  Poll at timestamp %d covers %.2f%% of OLD bucket %d with rate: %.2f",  (t,  (100 * bucket_coverage),  self.last_bucket_start,  this_rate))
            # calculate total bucket value
            sum = 0
            sum_percent = 0
            for (b_val,  b_percent) in self.bucket:
                weighted_val = b_val * b_percent
                sum += weighted_val
                sum_percent += b_percent
                dprint("  [%d] B_percent: %.4f   B_val: %.2f  (weighted=%.2f)",  (self.last_bucket_start,  b_percent,  b_val,  weighted_val))
            dprint("  [[%d]] percentages summed to %.4f,   rates summed to %.2f",  (self.last_bucket_start, sum_percent, sum))
            if sum_percent >= self.permit_coverage:
                yield (self.last_bucket_start,  sum)
            else:
                dprint("ERROR: Skipped bucket [[%d]] which summed only to %.3f%%!",  (self.last_bucket_start,  sum_percent))
            # advance forward one bucket...
            self._new_bucket(self.last_bucket_start + period)
            while self.last_bucket_start != this_bucket_start:
                # generate rates for intermediate missing buckets
                yield (self.last_bucket_start,  this_rate)
                dprint("   flat-interpolated bucket %d had overall rate of %.2f (flat-interpolation/counters)",  (self.last_bucket_start,  this_rate))
                self.last_bucket_start += period
            overlap = t - this_bucket_start
            if overlap > 0:
                dprint("  left over overlap = %d, in bucket %d (t=%d)",  (overlap,  this_bucket_start,  t))
                bucket_coverage = overlap / period
                self.bucket.append((this_rate,  bucket_coverage)) # could just be an accumulator
            # ok, handled previous buckets, whether they had data in them or not
            self._store_last_sample(t, v)
            raise StopIteration
        if this_bucket_start == self.last_bucket_start:
            bucket_coverage = (t - self.last_t) / period
            dprint("  Poll at timestamp %d (delta_t=%d) covers %.2f%% of NEW bucket %d with rate: %.2f",  (t,  delta_t,  (100 * bucket_coverage),  this_bucket_start,  this_rate))
            self.bucket.append((this_rate, bucket_coverage))
        self._store_last_sample(t, v)
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
c = Counter(period = 60)
c_nointerp = Counter_NoInterpolation(period = 60,  skip_dupes = False)
rd = []
rd2 = []
for t, v in polled_data: # this turns into the function call to generate this dataset
        for result in c.new_count(t, v):
            rd.append(result)
        for result in c_nointerp.new_count(t, v):
            rd2.append(result)

# check the integrated value
sum_rate = 0
sum_rate2 = 0
for time, rate in rd:
    sum_rate += (c.period * rate)
for time, rate in rd2:
    sum_rate2 += (c_nointerp.period * rate)
print "Summed rate1: %.2f" % sum_rate
print rd
print
print "Summed rate2: %.2f" % sum_rate2
print rd2
print

dprint = update_debug(False)
# This is boilerplate for the most part!  Turn it into a test framework

def perform_test(data_generator, test_name):
    period = data_generator.period
    c = Counter(period = period)
    integrated_sum = 0
    count_samples = 0
    count_rates = 0
    for t, v in data_generator:
        count_samples += 1
        for result_t, result_rate in c.new_count(t, v):
            this_isum = result_rate * period
            integrated_sum += this_isum
            count_rates += 1
        # send counter to any additional processing algorithms here! -JWP
    counter_sum = data_generator.get_rise()
    print "%s] Processing total %d input samples generated %d output rates." % (test_name,  count_samples,  count_rates)
    print "%s] Observed total counter rise: %d, and integrated-sum is: %.25f" % (test_name,  counter_sum,  integrated_sum)
    absolute_error = abs(counter_sum - integrated_sum)
    percent_error = 0
    if counter_sum > 0:
        percent_error = absolute_error / counter_sum * 100
        print "%s] Absolute Error is: %.25f.  Percent Error is: %.25f%%" % (test_name,  absolute_error,  percent_error)
    else:
        print "%s] Counter did not increase!  Error is zero.  (absolute error=%.25f)" % (test_name,  absolute_error)
    print
    return (absolute_error,  percent_error)

# Now perform tests in the various contexts using the following constants
period = 60
num = 25000
avg_rate = 13
rs = 143

# i'm still looking for the worst-case input, something that will exercise the most IEEE rounding
# in the generation or in the result processing
dataset=TestData(num * 100, period, avg_rate,
                            time_variance = "both",  max_time_variance = 0.99,
                            gap_odds = 0.05,  gap_avg_width = 50,
                            avg_rate_variance = 120002.5,
                            random_seed = rs)
perform_test(dataset,  test_name = "Data Set EVIL")

dataset = TestData(num, period, avg_rate, random_seed = rs)
perform_test(dataset,  test_name = "Data Set 1")

dataset = TestData(num, period, avg_rate, fixed_time_offset = int(period / 2), random_seed = rs)
perform_test(dataset,  test_name = "Data Set 2")

#dprint = update_debug(True)
dataset = TestData(num, period, avg_rate, time_variance = "positive",  max_time_variance = 0.20, random_seed = rs)
perform_test(dataset,  test_name = "Data Set 3")
#dprint = update_debug(False)

dataset = TestData(num, period, avg_rate, time_variance = "positive",  max_time_variance = 2.0, random_seed = rs)
perform_test(dataset,  test_name = "Data Set 4")

dataset = TestData(num, period, avg_rate, time_variance = "both",  max_time_variance = 0.50, random_seed = rs)
perform_test(dataset,  test_name = "Data Set 5")

dataset = TestData(num, period, avg_rate, time_variance = "negative",  max_time_variance = 0.20, random_seed = rs)
perform_test(dataset,  test_name = "Data Set 6")

print "Data Set 7]  -- not implemented yet\n"

dataset = TestData(num, period, avg_rate, time_variance = "negative",  max_time_variance = 0.10,
                   gap_odds = 0.05,  gap_avg_width = period * 45, random_seed = rs)
perform_test(dataset,  test_name = "Data Set 8")
