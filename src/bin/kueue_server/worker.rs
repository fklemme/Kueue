use kueue::message::{stream::MessageStream, WorkerMessage, error::MessageError};

pub struct Worker {
    name: String,
    stream: MessageStream,
}

impl Worker {
    pub fn new(name: String, stream: MessageStream) -> Self {
        Worker { name, stream }
    }

    pub async fn run(&mut self) {
        // for testing: read WorkerMessages and print out!
        loop {
            match self.stream.receive::<WorkerMessage>().await {
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
