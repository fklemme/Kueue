use kueue::message::{stream::MessageStream, ClientMessage, error::MessageError};

pub struct Client {
    stream: MessageStream,
}

impl Client {
    pub fn new(stream: MessageStream) -> Self {
        Client { stream }
    }

    pub async fn run(&mut self) {
        // for testing: read ClientMessages and print out!
        loop {
            match self.stream.receive::<ClientMessage>().await {
                Ok(message) => println!("Received message: {:?}", message),
                Err(MessageError::ConnectionClosed) => {
                    println!("Connection closed!");
                    return;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return;
                }
            }
        }
    }
}
