use crate::{
    config::Config,
    server::{handle_connection, shared_state::Manager},
};
use anyhow::{bail, Result};
use std::sync::{Arc, RwLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::{channel, Receiver, Sender},
};
use tokio_util::sync::CancellationToken;

pub struct TestServer {
    config: Arc<RwLock<Config>>,
    shared: Arc<RwLock<Manager>>,
    cancel_token: CancellationToken,
    keep_alive: Option<Sender<()>>,
    shutdown: Receiver<()>,
}

impl TestServer {
    pub fn new(config: Config) -> Self {
        let cancel_token = CancellationToken::new();
        let (keep_alive, shutdown) = channel::<()>(1);

        Self {
            config: Arc::new(RwLock::new(config.clone())),
            shared: Arc::new(RwLock::new(Manager::new(config))),
            cancel_token,
            keep_alive: Some(keep_alive),
            shutdown,
        }
    }

    pub fn connect<Stream: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static>(
        &self,
        stream: Stream,
    ) -> Result<()> {
        match &self.keep_alive {
            Some(keep_alive) => {
                tokio::spawn(handle_connection(
                    stream,
                    self.config.clone(),
                    self.shared.clone(),
                    self.cancel_token.clone(),
                    keep_alive.clone(),
                ));
                Ok(())
            }
            None => {
                bail!("Server has already been shut down!")
            }
        }
    }

    pub async fn stop(&mut self) {
        if let Some(keep_alive) = self.keep_alive.take() {
            // Cancel all spawned tasks.
            self.cancel_token.cancel();
            drop(keep_alive);
            // Wait for all senders in the spawned tasks to be dropped.
            self.shutdown.recv().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::Config,
        messages::{
            stream::{MessageError, MessageStream},
            ClientToServerMessage, HelloMessage, ServerToClientMessage,
        },
        server::test::TestServer,
    };
    use simple_logger::SimpleLogger;
    use tokio::io::duplex;

    #[tokio::test]
    async fn server_recv_hello_and_bye() {
        // Setup test config and test stream.
        let config = Config::new(None).unwrap();
        let (server_stream, client_stream) = duplex(1024);

        SimpleLogger::new()
            .with_level(config.get_log_level().unwrap().to_level_filter())
            .init()
            .unwrap();

        // Setup server.
        let mut server = TestServer::new(config);
        assert!(server.connect(server_stream).is_ok());

        // Simulate simple client.
        let mut stream = MessageStream::new(client_stream);

        // Send hello, receive welcome.
        assert!(stream.send(&HelloMessage::HelloFromClient).await.is_ok());
        assert_eq!(
            stream.receive::<ServerToClientMessage>().await,
            Ok(ServerToClientMessage::WelcomeClient)
        );

        // Send bye, connection closed by server.
        assert!(stream.send(&ClientToServerMessage::Bye).await.is_ok());
        assert_eq!(
            stream.receive::<ServerToClientMessage>().await,
            Err(MessageError::StreamClosed)
        );

        // Shutdown server.
        server.stop().await;
    }
}
