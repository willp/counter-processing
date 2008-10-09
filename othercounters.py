from __future__ import division


# Must factor this out- it's also in the main counters.py
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


# This class needs to DIE - it's supposed to be the NAIVE implementation...
class Counter_NoInterpolation(object):
    def __init__(self,  period, skip_dupes=False,  permit_coverage=None,  ignore_zeroes=False):
        #deal with args
        self.period = period
        self.ignore_zeroes = ignore_zeroes
        self.skip_dupes = skip_dupes
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
        if v==0 and self.ignore_zeroes:
            raise StopIteration
        period = self.period
        this_bucket_start = t - (t  % period)
        # handle initialization
        if self.last_t is None:
            self.last_t = t
            self.last_v = v
            self.count_samples += 1
            self.last_bucket_start = this_bucket_start
            raise StopIteration
        # skip bad data
        delta_t = t - self.last_t
        delta_v = v - self.last_v
        if delta_t <= 0:
            self.count_bad_timestamps += 1
            raise StopIteration
        self.last_t = t
        self.last_v = v
        if delta_v < 0:
            self.last_bucket_start = this_bucket_start
            self.count_wraps += 1
            raise StopIteration
        self.count_samples += 1
        this_rate = delta_v / delta_t
        yield (this_bucket_start,  this_rate)
        raise StopIteration
