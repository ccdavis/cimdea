//! The cimdea error type.

use std::fmt;

/// The cimdea error type.
///
/// As a user, the most common thing to do with these errors is to convert them to strings and
/// print or log them. You could also have logic to match on each variant and handle them
/// separately, but the variants in this struct are not yet stable. Several more variants may be
/// added in the future, and the existing variants may change or be consolidated.
///
/// ```
/// use cimdea::mderror::MdError;
///
/// let err = MdError::MetadataError("missing metadata for variable AGE".to_string());
/// assert_eq!(err.to_string(), "metadata error: missing metadata for variable AGE");
/// ```
#[derive(Debug)]
pub enum MdError {
    IoError(std::io::Error),
    /// An error in the metadata. This could be caused by missing metadata for a requested
    /// variable. Or it could be that metadata does not make sense for one reason or another.
    MetadataError(String),
    /// Invalid SQL syntax passed to the SQL engine. This likely indicates a bug in cimdea.
    InvalidSQLSyntax(String),
    /// An error while parsing input JSON.
    ParsingError(String),
    /// An error from the DuckDB data platform. This likely indicates a bug in cimdea.
    DuckDBError(duckdb::Error),
    /// A generic cimdea error.
    Msg(String),
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
