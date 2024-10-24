use std::fmt;

#[derive(Debug)]
pub enum MdError {
    IoError(std::io::Error),
    MetadataError(String),
    InvalidSQLSyntax(String),
    // There was an error while parsing the input JSON
    ParsingError(String),
    DuckDBError(duckdb::Error),
    Msg(String),
    // more needed
}

impl fmt::Display for MdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use MdError::*;

        match self {
            IoError(err) => write!(f, "I/O error: {err}"),
            MetadataError(msg) => write!(f, "metadata error: {msg}"),
            InvalidSQLSyntax(msg) => write!(f, "SQL syntax error: {msg}"),
            ParsingError(msg) => write!(f, "parsing error: {msg}"),
            DuckDBError(err) => write!(f, "DuckDB error: {err}"),
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

impl From<duckdb::Error> for MdError {
    fn from(err: duckdb::Error) -> Self {
        MdError::DuckDBError(err)
    }
}

/// A small convenience macro, based on the format! macro in the standard library.
///
/// Instead of directly constructing an `MdError::ParsingError` on a formatted
/// string, you can use `parse_error!` to get the same result with a little less
/// typing. The arguments are those you would pass to the format! macro.
///
/// `let err = parsing_error!("something wrong with variable {}", variable);`
macro_rules! parsing_error {
    ($($arg:tt)*) => {
        $crate::mderror::MdError::ParsingError(format!($($arg)*))
    }
}
pub(crate) use parsing_error;

macro_rules! metadata_error {
    ($($arg:tt)*) => {
        $crate::mderror::MdError::MetadataError(format!($($arg)*))
    };
}
pub(crate) use metadata_error;

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_error_macro() {
        let variable = "AGE";
        let dataset = "us2015a";
        let err = parsing_error!(
            "something wrong with variable {} in dataset {dataset}",
            variable
        );

        assert_eq!(
            err.to_string(),
            "parsing error: something wrong with variable AGE in dataset us2015a"
        );
    }

    #[test]
    fn test_metadata_error_macro() {
        let variable = "AGE";
        let gen_width = 4;
        let detailed_width = 3;

        let err = metadata_error!(
            "invalid widths for variable {}: general width is {} but detailed width is {}",
            variable,
            gen_width,
            detailed_width,
        );

        assert_eq!(err.to_string(), "metadata error: invalid widths for variable AGE: general width is 4 but detailed width is 3");
    }
}
