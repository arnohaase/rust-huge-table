use std::time::{SystemTime, Duration};
use std::sync::Mutex;

/// HtTimestamps are used for merging values. They have millisecond precision, with a counter value
///  added to give them microsecond resolution. They start counting at midnight Jan 1 2020 UTC,
///  representing only timestamps after that epoch.
#[derive(Copy, Clone, PartialOrd, Ord, Eq, PartialEq, Debug)]
pub struct HtTimestamp {
    pub micros: u64
}
impl HtTimestamp {
    pub fn new(micros: u64) -> HtTimestamp {
        HtTimestamp {micros}
    }

    pub fn as_system_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
            + Duration::from_millis(WallClock::EPOCH_MILLIS)
            + Duration::from_micros(self.micros)
    }
}

pub trait HtClock {
    fn now(&self) -> HtTimestamp;
}


struct WallClockCounter {
    cur_epoch_millis: u64,
    counter: u32,
}
pub struct WallClock {
    counter: Mutex<WallClockCounter>,
}
impl WallClock {
    const EPOCH_MILLIS: u64 = 24*60*60*(365*50 + 12)*1000;

    pub fn new() -> WallClock {
        WallClock {
            counter: Mutex::new(WallClockCounter {
                cur_epoch_millis: 0,
                counter: 0
            })
        }
    }
}
impl HtClock for WallClock {
    fn now(&self) -> HtTimestamp {
        let unix_mills = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
            .expect("'now()' appears to be before 1970-01-01")
            .as_millis() as u64;

        let millis = unix_mills - WallClock::EPOCH_MILLIS;

        let mut lock = self.counter.lock().unwrap();
        if lock.cur_epoch_millis != millis {
            lock.cur_epoch_millis = millis;
            lock.counter = 0;
        }
        let counter = lock.counter;
        if lock.counter < 1000 { //TODO is there a better way to do this?
            lock.counter += 1;
        }

        HtTimestamp::new (millis * 1000 + counter as u64)
    }
}

pub struct ManualClock {
    ts: Mutex<HtTimestamp>
}
impl ManualClock {
    pub fn new(initial: HtTimestamp) -> ManualClock {
        ManualClock {
            ts: Mutex::new(initial)
        }
    }

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
    use crate::time::{WallClock, HtClock, ManualClock, HtTimestamp};
    use std::time::{SystemTime, Duration};

    #[test]
    pub fn test_wallclock_time() {
        let wall_clock = WallClock::new();

        let t1 = wall_clock.now();
        let st1 = t1.as_system_time();

        let diff1 = SystemTime::now().duration_since(st1).unwrap();
        assert!(diff1 < Duration::from_secs(1));

        let diff2 = st1.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() - t1.micros as u128;
        assert_eq!(diff2, (WallClock::EPOCH_MILLIS*1000) as u128);
   }

    #[test]
    pub fn test_wallclock_strictly_monotonous() {
        let wall_clock = WallClock::new();

        let mut t1 = wall_clock.now();
        let mut t2 = wall_clock.now();
        for i in 0..1000 {
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
