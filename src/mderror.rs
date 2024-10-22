use std::fmt;
#[derive(Debug)]
pub enum MdError {
    IoError(std::io::Error),
    NotInMetadata(String),
    ParsingError(String),
    Msg(String),
    // more needed
}

impl fmt::Display for MdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MdError::*;

        match self {
            IoError(err) => write!(f, "I/O error: {err}"),
            NotInMetadata(msg) => write!(f, "metadata error: {msg}"),
            ParsingError(msg) => write!(f, "parsing error: {msg}"),
            Msg(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for MdError {}

impl From<std::io::Error> for MdError {
    fn from(err: std::io::Error) -> Self {
        MdError::IoError(err)
    }
}
