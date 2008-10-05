from random import random

class TestValues(object):
    def __init__(self, num,  period, avg_period_variance,  avg_rate,
                 start_time=0, fixed_time_offset=0, gap_odds=0,  gap_avg_width=None, counter_start=0):
        self.num=num
        self.period=period
        self.avg_period_variance=avg_period_variance
        self.avg_rate=avg_rate
        self.gap_odds=gap_odds
        if gap_odds > 0 and gap_avg_width is None:
            raise ValueError,  "Error, \"gap_odds\" is set to %.3f in constructor, but \"gap_avg_width\" is not specified!"
        self.gap_avg_width=gap_avg_width
        # additional internal state
        self.count=0
        # initialize time
        self.time = start_time
        self.time += fixed_time_offset
        # initialize counter
        self.counter=counter_start

    def next(self):
        if (self.count == self.num):
            raise StopIteration
        self.count += 1
        if (self.count == self.num):
            # make last sample land perfectly on a multiple of the period
            last_time = self.time
            time_increment = self.period - (last_time % self.period)
            print "FINAL TIMESTAMP has time: %d" % (self.time + time_increment)
        else:
            time_increment = self.period
        if self.gap_odds > 0 and (random() < self.gap_odds):
            #we hit a gap!
            gap_width = int(self.gap_avg_width * random())
            time_increment += gap_width
            print "GAP OCCURRED: WIDTH: %d seconds  (from %d to %d)" % (gap_width,  self.time,  self.time+time_increment)
        # Now handle polling variance
        if self.avg_period_variance > 0:
            time_variance = self.period - 2 * int(self.avg_period_variance * random())
            time_increment += time_variance
        self.time += time_increment
        # Deal with counter value increasing
        counter_increment = int( self.avg_rate * self.period * random())
        self.counter += counter_increment
        return (self.time,  self.counter)

    def __iter__(self):
        return self

if __name__ == "__main__":
    drift_poll = TestValues (num=1000,  period=60,  avg_period_variance=0,  avg_rate=100,  gap_odds=0.05, gap_avg_width=60*20 )
    for t, c in drift_poll:
        print "%20d: %6d" % (t,  c)

