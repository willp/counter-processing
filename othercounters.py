
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
