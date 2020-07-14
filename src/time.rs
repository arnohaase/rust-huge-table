use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// HtTimestamp holds ticks of a tenth of a nanosecond since 'ht epoch' (midnight Jan 1 2020). It
///  can represent only timestamps after that day which is ok since they are used for representing
///  times in data items' lifecycle.
///
/// There are 10^9 * 86.400 * 365 * 10 = 315 * 10^15 tenths of nanoseconds in a year, so given u64's
///  upper limit of 18 * 10^18 HtTimestamp is good for about fifty years.
///
/// HtTimestamps have nanosecond resolution but typically not nanosecond precision, i.e. some of the
///  least significant bits are used to ensure uniqueness and strictly monotonous creation instead
///  of representing real time.
#[derive(Copy, Clone, PartialOrd, Ord, Eq, PartialEq, Debug)]
pub struct HtTimestamp {
    pub ticks: u64
}

impl HtTimestamp {
    const EPOCH_MILLIS: u64 = 24 * 60 * 60 * (365 * 50 + 12) * 1000;

    pub fn new(ticks: u64) -> HtTimestamp {
        HtTimestamp { ticks }
    }

    pub fn as_system_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
            + Duration::from_millis(HtTimestamp::EPOCH_MILLIS)
            + Duration::from_nanos(self.ticks / 10)
    }
}

pub trait HtClock {
    fn now(&self) -> HtTimestamp;
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
    /// * unique_context serves to disambiguate 'identical' time stamps between processes.
    /// * time_travel_counter serves to disambiguate 'backwards time travel'. Both should be stored
    ///    and reused to really ensure time stamp uniqueness
    pub fn new(unique_context: u64, time_travel_counter: u64, time_travel_callback: Box<dyn TimeTravelCallback>) -> WallClock {
        assert!(unique_context < 1000);
        assert!(time_travel_counter < 10);

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
}

impl HtClock for WallClock {
    fn now(&self) -> HtTimestamp {
        let unix_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("'now()' appears to be before 1970-01-01")
            .as_millis() as u64;

        assert!(unix_millis >= HtTimestamp::EPOCH_MILLIS, "now() appears to be before 2020-01-01");
        let millis = unix_millis - HtTimestamp::EPOCH_MILLIS;

        let mut lock = self.counter.lock().unwrap();

        if millis < lock.cur_epoch_millis {
            // backwards time travel - move to a different 'reality' by incrementing the time travel counter
            lock.time_travel_counter = (lock.time_travel_counter + 1) % 10;

            self.time_travel_callback.on_time_travel(millis, lock.cur_epoch_millis, lock.time_travel_counter as u8);

            lock.counter = 0;
            lock.cur_epoch_millis = millis;
        } else {
            let diff_millis = millis - lock.cur_epoch_millis;

            if diff_millis != 0 {
                lock.cur_epoch_millis = millis;

                if lock.counter < diff_millis * 1000 {
                    lock.counter = 0;
                } else {
                    lock.counter -= diff_millis * 1000;
                }
            }
        }

        let counter = lock.counter;
        lock.counter += 1;

        let ticks = millis * 10_000_000 + (counter * 10_000) + self.unique_context * 10 + lock.time_travel_counter;
        HtTimestamp::new(ticks)
    }
}

#[allow(dead_code)]
pub struct ManualClock {
    ts: Mutex<HtTimestamp>
}

impl ManualClock {
    #[allow(dead_code)]
    pub fn new(initial: HtTimestamp) -> ManualClock {
        ManualClock {
            ts: Mutex::new(initial)
        }
    }

    #[allow(dead_code)]
    pub fn set(&self, ts: HtTimestamp) {
        *self.ts.lock().unwrap() = ts;
    }
}

impl HtClock for ManualClock {
    fn now(&self) -> HtTimestamp {
        *self.ts.lock().unwrap()
    }
}


#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use crate::time::{HtClock, HtTimestamp, ManualClock, NoTimeTravelCallback, WallClock};

    #[test]
    pub fn test_wallclock_time() {
        let wall_clock = WallClock::new_without_callback(7, 3);

        let t1 = wall_clock.now();
        let st1 = t1.as_system_time();

        assert_eq!(t1.ticks % 10_000, 73);

        let diff1 = SystemTime::now().duration_since(st1).unwrap();
        assert!(diff1 < Duration::from_secs(1));

        let diff2 = st1.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() - (t1.ticks / 10) as u128;
        assert_eq!(diff2, (HtTimestamp::EPOCH_MILLIS * 1_000_000) as u128);
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
        let clock = ManualClock::new(HtTimestamp::new(12345));
        assert_eq!(clock.now(), HtTimestamp::new(12345));

        clock.set(HtTimestamp::new(9876543));
        assert_eq!(clock.now(), HtTimestamp::new(9876543));
    }
}
