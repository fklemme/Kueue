#[derive(Debug)]
pub enum ParseError {
    EofWhileParsing,
    ParsingFailed,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug)]
pub enum MessageError {
    SendFailed,
    ReceiveFailed,
    ConnectionClosed,
}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for MessageError {}
