use std::io::Error;

pub type HtResult<T> = std::result::Result<T, HtError>;

#[derive(Debug)]
pub enum HtError {
    Io(std::io::Error),
    FileIntegrity(String),
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