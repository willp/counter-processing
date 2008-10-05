import random

# An enum
VARIANCE_BOTH=0
VARIANCE_NEGATIVE=1
VARIANCE_POSITIVE=2
# and helper dictionary
VARIANCE_ENUM = dict ( positive=VARIANCE_POSITIVE,  negative=VARIANCE_NEGATIVE,  both=VARIANCE_BOTH)

class TestValues(object):
    def __init__(self, num,  period, avg_rate,
                 avg_time_variance=0, avg_rate_variance=0, time_variance="both", random_seed=None,
                 start_time=0, fixed_time_offset=0, gap_odds=0,  gap_avg_width=None, counter_start=0):
        self.num=num
        self.period=period
        self.avg_time_variance=avg_time_variance
        self.time_variance=VARIANCE_ENUM[time_variance]
        self.avg_rate=avg_rate
        self.avg_rate_variance=avg_rate_variance
        self.gap_odds=gap_odds
        if gap_odds > 0 and gap_avg_width is None:
            raise ValueError,  "Error, \"gap_odds\" is set to %.3f in constructor, but \"gap_avg_width\" is not specified!"
        self.gap_avg_width=gap_avg_width
        # additional internal state
        self.count=0
        if random_seed is not None:
            random.seed(random_seed)
        # initialize time
        self.time = start_time
        self.time += fixed_time_offset
        # initialize counter
        self.counter=counter_start

    def next(self):
        if (self.count > self.num):
            raise StopIteration
        self.count += 1
        if self.count == 1:
            print "TV DEBUG[%d]:  timestamp=%d, counter=%d" % (self.count,  self.time,  self.counter)
            return (self.time,  self.counter)
        if (self.count > self.num):
            # make last sample land perfectly on a multiple of the period
            last_time = self.time
            time_increment = self.period - (last_time % self.period)
            print "FINAL TIMESTAMP(1) has time: %d, increment=%d.  counter=%d" % (self.time + time_increment,  time_increment,  self.counter)
        else:
            time_increment = self.period
            if self.gap_odds > 0 and (random.random() < self.gap_odds):
                #we hit a gap!
                gap_width = int(self.gap_avg_width * random.random())
                time_increment += gap_width
                print "GAP OCCURRED: WIDTH: %d seconds  (from %d to %d)" % (gap_width,  self.time,  self.time+time_increment)
            # Now handle polling variance
            if self.avg_time_variance > 0:
                random_variance = random.random()
                if self.time_variance == VARIANCE_BOTH:
                    if (random_variance < 0.5):
                        random_variance = -random_variance
                if self.time_variance == VARIANCE_NEGATIVE:
                    random_variance = -random_variance
                time_variance = int(self.period * random_variance * self.avg_time_variance)
                if (time_variance <= -self.period): # clip negative time variance
                    time_variance = self.period - 1 # ensure time variance never moves us backwards in time
                time_increment += self.period + time_variance
                #print "TDEBUG: time_increment is %d vs period of %d (variance=%d)" % (time_increment,  self.period,  time_variance)
        self.time += time_increment
        # Deal with counter value increasing
        if self.avg_rate_variance > 0:
            random_variance = 1 + self.avg_rate_variance * random.random()
            this_inc = self.period * self.avg_rate
            this_inc *= random_variance
            #print "DEBUG: Random_variance=%.3f  vs. this_inc= %f" % (random_variance,  this_inc)
            counter_increment = int(this_inc)
            #print "DEBUG: counter_increment=%d (vs %d)" % (counter_increment,  (self.avg_rate * time_increment ) )
        else:
            counter_increment = int(self.period * self.avg_rate)
        print "TVdebug: counter increment is: %d, time increment is: %d" % (counter_increment,  time_increment)
        self.counter += counter_increment
        if (self.count > self.num):
            print "FINAL timestamp(2) time=%d and counter=%d" % (self.time,  self.counter)
        print "TV DEBUG[%d]:  timestamp=%d, counter=%d" % (self.count,  self.time,  self.counter)
        return (self.time,  self.counter)

    def __iter__(self):
        return self

if __name__ == "__main__":
    drift_poll = TestValues (num=1000,  period=60,  avg_time_variance=0,  avg_rate=100,  gap_odds=0.05, gap_avg_width=60*20 )
    for t, c in drift_poll:
        print "%20d: %6d" % (t,  c)

