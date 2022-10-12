#[derive(Debug)]
pub enum Error {
    EofWhileParsing,
    ParsingFailed,
    ConnectionClosed,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}
