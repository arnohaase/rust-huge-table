use std::io::Error;

pub type HtResult<T> = std::result::Result<T, HtError>;

#[derive(Debug)]
pub enum HtError {
    Io(std::io::Error),
    FileIntegrity(String),
    Misc,
}
impl From<std::io::Error> for HtError {
    fn from(e: Error) -> Self {
        HtError::Io(e)
    }
}