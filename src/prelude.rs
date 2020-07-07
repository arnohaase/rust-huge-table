use std::io::Error;

pub type HtResult<T> = std::result::Result<T, HtError>;

pub enum HtError {
    Io(std::io::Error),
    FileIntegrity(String),
}
impl From<std::io::Error> for HtError {
    fn from(e: Error) -> Self {
        HtError::Io(e)
    }
}