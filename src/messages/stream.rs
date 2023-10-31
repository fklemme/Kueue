//! Read and write messages from and to the network.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// MessageStream builds a high-level abstraction of sending messages over the
/// network on top of a TcpStream. It takes ownership of a given TcpStream and
/// instantiates buffers to account for caching (yet) incomplete messages.
pub struct MessageStream {
    /// The underlying TcpStream.
    stream: TcpStream,
    /// Buffers bytes read from the TcpStream. The buffer starts with a
    /// fixed size of INIT_READ_BUFFER_LEN and doubles in size whenever
    /// its capacitance is reached while receiving data from the network.
    read_buffer: Vec<u8>,
    /// Holds received message chunks from previous read operations.
    /// The buffer grows dynamically until it fits a complete message.
    msg_buffer: Vec<u8>,
}

/// Initial size of the read buffer. Whenever its size was insufficient to read
/// all data available on the network, its capacity is doubled. This avoids too
/// many parsing attempts on (yet) incomplete messages at the cost of higher
/// memory consumption.
const INIT_READ_BUFFER_LEN: usize = 32 * 1024;

impl MessageStream {
    /// Create a high-level message stream abstraction on top of a TcpStream.
    pub fn new(stream: TcpStream) -> Self {
        MessageStream {
            stream,
            read_buffer: vec![0; INIT_READ_BUFFER_LEN],
            msg_buffer: Vec::new(),
        }
    }

    /// Send a message over the network.
    pub async fn send<T: Serialize + Debug>(&mut self, message: &T) -> Result<(), MessageError> {
        log::trace!("Sending message: {:?}", message);
        let buffer = serde_json::to_vec(message).unwrap();

        match self.stream.write_all(&buffer).await {
            Ok(()) => Ok(()),
            Err(e) => {
                log::error!("Write error: {}", e);
                Err(MessageError::SendFailed)
            }
        }
    }

    /// Receive a message from the network.
    pub async fn receive<T: for<'a> Deserialize<'a> + Debug>(&mut self) -> Result<T, MessageError> {
        loop {
            // Parse message from message buffer
            match self.parse_message::<T>() {
                Ok(message) => {
                    log::trace!("Received message: {:?}", message);
                    return Ok(message);
                }
                Err(ParseError::EofWhileParsing) => {} // no return -> continue reading from stream
                Err(_) => return Err(MessageError::ReceiveFailed), // give up and propagate error
            }

            // Read more data from stream
            match self.stream.read(&mut self.read_buffer).await {
                Ok(0) => return Err(MessageError::ConnectionClosed),
                Ok(bytes_read) => {
                    // Move read bytes into message buffer and continue loop
                    self.msg_buffer.extend(&self.read_buffer[..bytes_read]);

                    if bytes_read == self.read_buffer.len() {
                        // The entire read buffer was occupied to fetch bytes from the network.
                        // Enlarge the size of the read buffer to reduce unnecessary parsing attempts.
                        log::debug!(
                            "Enlarging read buffer to {} KB.",
                            self.read_buffer.len() / 1024 * 2
                        );
                        self.read_buffer.resize(self.read_buffer.len() * 2, 0);
                    }
                }
                Err(e) => {
                    log::error!("Read error: {}", e);
                    return Err(MessageError::ReceiveFailed);
                }
            }
        }
    }

    /// Deserialize the next message.
    fn parse_message<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, ParseError> {
        // Try to parse T from msg_buffer
        let de = serde_json::Deserializer::from_slice(&self.msg_buffer);
        let mut message_iterator = de.into_iter::<T>();
        match message_iterator.next() {
            Some(result) => match result {
                Ok(message) => {
                    // Successfully read message. Remove consumed bytes from buffer.
                    let bytes_consumed = message_iterator.byte_offset();
                    self.msg_buffer.drain(..bytes_consumed);
                    Ok(message)
                }
                Err(e) if e.is_eof() => {
                    // Incomplete message. We need to read more data from the stream.
                    Err(ParseError::EofWhileParsing)
                }
                Err(e) => {
                    // Bad things happened! We need to give up.
                    log::error!("Parse error: {}", e);
                    Err(ParseError::ParsingFailed)
                }
            },
            None => {
                // Happens when buffer is empty. We need to read data from stream.
                debug_assert!(self.msg_buffer.is_empty());
                Err(ParseError::EofWhileParsing)
            }
        }
    }
}

/// Errors related to the MessageStream.
#[derive(Debug)]
pub enum MessageError {
    /// Failed to write the message to the network.
    SendFailed,
    /// Failed to receive a message from the network.
    ReceiveFailed,
    /// Network connection has been closed.
    ConnectionClosed,
}

impl std::error::Error for MessageError {}

impl std::fmt::Display for MessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// ParseError is used internally to distinguish between
/// incomplete and (syntactically) failed parsing attempts.
#[derive(Debug)]
enum ParseError {
    /// The end of input was reached before parsing could be completed.
    EofWhileParsing,
    /// Parsing of the message failed.
    ParsingFailed,
}

impl std::error::Error for ParseError {}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
