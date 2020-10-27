use std::io::Error;

pub type HtResult<T> = std::result::Result<T, HtError>;

#[derive(Debug)]
pub enum HtError {
    Io(std::io::Error),
    Misc(String),
}
impl HtError {
    pub fn misc(msg: &str) -> HtError {
        HtError::Misc(msg.to_string())
    }
}

impl From<std::io::Error> for HtError {
    fn from(e: Error) -> Self {
        HtError::Io(e)
    }
}

macro_rules! ordered {
    ($t:ty) => {
        impl Ord for $t {
            #[inline]
            fn cmp(&self, other: &Self) -> Ordering {
                <$t>::compare(self, other)
            }
        }
        impl PartialOrd for $t {
            #[inline]
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(<$t>::compare(self, other))
            }
        }
        impl PartialEq for $t {
            #[inline]
            fn eq(&self, other: &Self) -> bool {
                <$t>::compare(self, other) == Ordering::Equal
            }
        }
        impl Eq for $t {}
    }
}

