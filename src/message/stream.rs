use crate::message::error::Error;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub const STREAM_BUFFER_LEN: usize = 1024;

pub struct MessageStream {
    stream: TcpStream,
    stream_buffer: Vec<u8>,
    msg_buffer: Vec<u8>,
}

impl MessageStream {
    pub fn new(stream: TcpStream) -> Self {
        MessageStream {
            stream,
            stream_buffer: vec![0; STREAM_BUFFER_LEN],
            msg_buffer: Vec::new(),
        }
    }

    fn parse_message<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, Error> {
        // Try to parse T from msg_buffer
        let de = serde_json::Deserializer::from_slice(&self.msg_buffer);
        let mut message_iterator = de.into_iter::<T>();
        match message_iterator.next() {
            Some(result) => match result {
                Ok(message) => {
                    // Successfully read message. Remove read bytes from buffer.
                    let bytes_consumed = message_iterator.byte_offset();
                    self.msg_buffer.drain(..bytes_consumed);
                    return Ok(message);
                }
                Err(e) if e.is_eof() => {
                    // Incomplete message. We need to write more data from the stream.
                    Err(Error::EofWhileParsing)
                }
                Err(e) => {
                    // Bad things happend! We need to give up.
                    eprintln!("Parse error: {}", e); // for debugging
                    Err(Error::ParsingFailed)
                }
            },
            None => {
                // Happens when buffer is empty. We need to read data from stream.
                assert!(self.msg_buffer.is_empty());
                Err(Error::EofWhileParsing)
            }
        }
    }

    pub async fn read<T: for<'a> Deserialize<'a>>(&mut self) -> Result<T, Error> {
        loop {
            // Parse message from message buffer
            match self.parse_message::<T>() {
                Ok(message) => return Ok(message),
                Err(Error::EofWhileParsing) => {} // continue, reading from stream
                Err(e) => return Err(e),          // give up and propagate error
            }

            // Read more data from stream
            match self.stream.read(&mut self.stream_buffer).await {
                Ok(bytes_read) if bytes_read == 0 => return Err(Error::ConnectionClosed),
                Ok(bytes_read) => {
                    // Move read bytes into message buffer and continue loop
                    self.msg_buffer.extend(&self.stream_buffer[..bytes_read]);
                }
                Err(e) => {
                    eprintln!("Read error: {}", e); // for debugging
                    return Err(Error::ConnectionClosed);
                }
            }
        }
    }

    pub async fn write<T: Serialize>(&mut self, message: &T) -> Result<(), Error> {
        let buffer = serde_json::to_vec(message).unwrap();

        match self.stream.write_all(&buffer).await {
            Ok(()) => Ok(()),
            Err(e) => {
                eprintln!("Write error: {}", e); // for debugging
                Err(Error::ConnectionClosed)
            }
        }
    }
}
