use std::fmt;
#[derive(Debug)]
pub enum MdError {
    IoError(std::io::Error),
    ParsingError(String),
    Msg(String),
    // more needed
}

impl fmt::Display for MdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MdError::IoError(err) => write!(f, "I/O error: {err}"),
            MdError::ParsingError(msg) => write!(f, "parsing error: {msg}"),
            MdError::Msg(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for MdError {}

impl From<std::io::Error> for MdError {
    fn from(err: std::io::Error) -> Self {
        MdError::IoError(err)
    }
}
