use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// MergeTimestamp is a globally unique value that is pretty much ordered by wall clock time (but
///  obviously not guaranteed to be since it works in a distributed system without central
///  coordination).
///
/// Merge timestamps consist four parts, in order of significance from highest to lowest:
/// * epoch_millis is the number of milliseconds since _HT_ epoch, i.e. Jan 1 2020. This is an
///    unsigned 41 bit value - there can be no writes before this time after all :-)  This is
///    only part of MergeTimestamp that is actually time related. There are about 31*10^9
///    milliseconds in a year, so 41 bits cover about 70 years (i.e. until 2090) which should be
///    sufficient for this specific purpose.
/// * a 10 bit counter. This allows 1024 unique timestamps in each millisecond or roughly a
///    million unique timestamps per second which should be plenty on average. If the counter
///    overflows (a rare occurrance), timestamps overflow into the next (future) millisecond,
///    and creation logic ensures that these values are skipped when that millisecond arrives.
/// * a 10 bit 'unique context' for disambiguation of values across running application processes.
///    HT is a distributed system without central coordination. so every node has its own counter.
///    Adding 10 bits that are unique per node ensures unique values across nodes. Note that in
///    order for this to work, every node must be assigned a unique value (e.g. via configuration).
/// * 3 bits for 'time travel resilience'. System clocks can go backwards in time, and while that
///    is rare, it can create timestamp collisions. To mitigate these collisions, merge timestamps
///    have an additional 3 bit counter that is increased whenever the system call returns a
///    timestamp that is strictly earlier than the previous one.
///    Note that backwards movement of the system clock can affect merge timestamp uniqueness even
///    if it happens while no HT instance is running. So this 'time travel resilience' part should
///    be persisted across application restarts, and incremented on each start.
///
/// MergeTimestamp are unique only when instantiated on the server side. The 'unique context' part
///  is far too small to avoid collisions globally in a reliable way. Creating them uniquely
///  is pretty complex and involves _persisting_ unique context and time travel part across
///  application restarts. A single WallClock instance should be shared across an entire node.
#[derive(Copy, Clone, PartialOrd, Ord, Eq, PartialEq, Debug)]
pub struct MergeTimestamp {
    pub ticks: u64
}

const HT_EPOCH_SECONDS: u64 = 24 * 60 * 60 * (365 * 50 + 12);
const HT_EPOCH_MILLIS: u64 = HT_EPOCH_SECONDS * 1000;

impl MergeTimestamp {
    pub fn from_ticks(ticks: u64) -> MergeTimestamp {
        MergeTimestamp { ticks }
    }

    pub fn new(epoch_millis: u64, counter_part: u64, unique_context: u64, time_travel_part: u64) -> MergeTimestamp {
        // counter may be >= 1024 to deal with overflow, in which case it is the creator's responsibility
        //  to ensure uniqueness
        assert!(unique_context < 1024);
        assert!(time_travel_part < 8);

        let ticks = (epoch_millis << 23) +
            (counter_part << 13) +
            (unique_context << 3) +
            time_travel_part;
        MergeTimestamp { ticks }
    }

    fn epoch_millis(&self) -> u64 {
        self.ticks >> 23
    }
    fn counter_part(&self) -> u64 {
        (self.ticks >> 13) & 0x3ff
    }
    fn unique_context(&self) -> u64 {
        (self.ticks >> 3) & 0x3ff
    }
    fn time_travel_part(&self) -> u64 {
        self.ticks & 7
    }

    pub fn as_system_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
            + Duration::from_millis(HT_EPOCH_MILLIS)
            + Duration::from_millis(self.epoch_millis())
    }
}

#[derive(Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Debug)]
pub struct TtlTimestamp {
    pub epoch_seconds: u32
}

impl TtlTimestamp {
    pub fn new(epoch_seconds: u32) -> TtlTimestamp {
        TtlTimestamp { epoch_seconds }
    }

    pub fn as_system_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
            + Duration::from_secs(HT_EPOCH_SECONDS)
            + Duration::from_secs(self.epoch_seconds as u64)
    }
}

pub trait HtClock {
    fn now(&self) -> MergeTimestamp;
    fn ttl_timestamp(&self, ttl_seconds: u32) -> TtlTimestamp;
}


struct WallClockCounter {
    cur_epoch_millis: u64,
    counter: u64,
    time_travel_counter: u64,
}

pub trait TimeTravelCallback {
    fn on_time_travel(&self, cur_millis: u64, prev_millis: u64, new_time_travel_counter: u8);
}

struct NoTimeTravelCallback {}

impl TimeTravelCallback for NoTimeTravelCallback {
    fn on_time_travel(&self, _cur_millis: u64, _prev_millis: u64, _new_time_travel_counter: u8) {}
}

pub struct WallClock {
    counter: Mutex<WallClockCounter>,
    unique_context: u64,
    time_travel_callback: Box<dyn TimeTravelCallback>,
}

impl WallClock {
    //TODO bump up counter on restart

    /// * unique_context serves to disambiguate 'identical' time stamps between processes.
    /// * time_travel_counter serves to disambiguate 'backwards time travel'. Both should be stored
    ///    and reused to really ensure time stamp uniqueness
    pub fn new(unique_context: u64, time_travel_counter: u64, time_travel_callback: Box<dyn TimeTravelCallback>) -> WallClock {
        assert!(unique_context < 1024);
        assert!(time_travel_counter < 8);

        WallClock {
            counter: Mutex::new(WallClockCounter {
                cur_epoch_millis: 0,
                counter: 0,
                time_travel_counter,
            }),
            unique_context,
            time_travel_callback,
        }
    }

    #[allow(dead_code)]
    pub fn new_without_callback(unique_context: u64, time_travel_counter: u64) -> WallClock {
        WallClock::new(unique_context, time_travel_counter, Box::new(NoTimeTravelCallback {}))
    }

    fn ht_epoch_millis() -> u64 {
        let unix_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("'now()' appears to be before 1970-01-01")
            .as_millis() as u64;

        assert!(unix_millis >= HT_EPOCH_MILLIS, "now() appears to be before 2020-01-01");
        unix_millis - HT_EPOCH_MILLIS
    }
}

impl HtClock for WallClock {
    fn now(&self) -> MergeTimestamp {
        let millis = WallClock::ht_epoch_millis();

        let mut lock = self.counter.lock().unwrap();

        if millis < lock.cur_epoch_millis {
            // backwards time travel - move to a different 'reality' by incrementing the time travel counter
            lock.time_travel_counter = (lock.time_travel_counter + 1) & 7;

            self.time_travel_callback.on_time_travel(millis, lock.cur_epoch_millis, lock.time_travel_counter as u8);

            lock.counter = 0;
            lock.cur_epoch_millis = millis;
        } else {
            let diff_millis = millis - lock.cur_epoch_millis;

            if diff_millis != 0 {
                lock.cur_epoch_millis = millis;

                if lock.counter < diff_millis * 1024 {
                    lock.counter = 0;
                } else {
                    lock.counter -= diff_millis * 1024;
                }
            }
        }

        lock.counter += 1;

        MergeTimestamp::new(millis, lock.counter, self.unique_context, lock.time_travel_counter)
    }

    fn ttl_timestamp(&self, ttl_seconds: u32) -> TtlTimestamp {
        let epoch_seconds = WallClock::ht_epoch_millis() / 1000;
        TtlTimestamp::new(epoch_seconds as u32 + ttl_seconds)
    }
}

#[allow(dead_code)]
pub struct ManualClock {
    ts: Mutex<MergeTimestamp>
}

impl ManualClock {
    #[allow(dead_code)]
    pub fn new(initial: MergeTimestamp) -> ManualClock {
        ManualClock {
            ts: Mutex::new(initial)
        }
    }

    #[allow(dead_code)]
    pub fn set(&self, ts: MergeTimestamp) {
        *self.ts.lock().unwrap() = ts;
    }
}

impl HtClock for ManualClock {
    fn now(&self) -> MergeTimestamp {
        *self.ts.lock().unwrap()
    }

    fn ttl_timestamp(&self, ttl_seconds: u32) -> TtlTimestamp {
        let epoch_seconds = self.now().epoch_millis() / 1000;
        TtlTimestamp::new(epoch_seconds as u32 + ttl_seconds)
    }
}


#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use crate::time::{HT_EPOCH_MILLIS, HtClock, ManualClock, MergeTimestamp, WallClock};

    #[test]
    pub fn test_wallclock_time() {
        let wall_clock = WallClock::new_without_callback(7, 3);

        let t1 = wall_clock.now();
        let st1 = t1.as_system_time();

        assert_eq!(t1.time_travel_part(), 3);
        assert_eq!(t1.unique_context(), 7);
        assert_eq!(t1.ticks & 0b1_1111_1111_1111, 7 * 8 + 3);

        let diff1 = SystemTime::now().duration_since(st1).unwrap();
        assert!(diff1 < Duration::from_secs(1));

        let diff2 = st1.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() - t1.epoch_millis() as u128;
        assert_eq!(diff2, HT_EPOCH_MILLIS as u128);
    }

    #[test]
    pub fn test_wallclock_strictly_monotonous() {
        let wall_clock = WallClock::new_without_callback(2, 5);

        let mut t1 = wall_clock.now();
        let mut t2 = wall_clock.now();
        for _ in 0..100_000 {
            assert!(t1 < t2);
            t1 = t2;
            t2 = wall_clock.now();
        }
    }

    #[test]
    pub fn test_manual_clock() {
        let clock = ManualClock::new(MergeTimestamp::from_ticks(12345));
        assert_eq!(clock.now(), MergeTimestamp::from_ticks(12345));

        clock.set(MergeTimestamp::from_ticks(9876543));
        assert_eq!(clock.now(), MergeTimestamp::from_ticks(9876543));
    }
}
