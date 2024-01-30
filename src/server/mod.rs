mod client_connection;
mod shared_state;
mod tcp;
mod test;
mod worker_connection;

pub use tcp::TcpServer;
pub use test::TestServer;

use crate::{
    config::Config,
    messages::stream::MessageStream,
    messages::{HelloMessage, ServerToClientMessage, ServerToWorkerMessage},
    server::{
        client_connection::ClientConnection, shared_state::Manager,
        worker_connection::WorkerConnection,
    },
};
use std::sync::{Arc, RwLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc::Sender,
};
use tokio_util::sync::CancellationToken;

/// Initiate welcome handshake with new connection
/// to distinguish between client and worker.
pub async fn handle_connection<Stream: AsyncReadExt + AsyncWriteExt + Unpin>(
    stream: Stream,
    config: Arc<RwLock<Config>>,
    shared: Arc<RwLock<Manager>>,
    cancel_token: CancellationToken,
    keep_alive: Sender<()>,
) {
    // Read hello message to distinguish between client and worker.
    let mut stream = MessageStream::new(stream);
    match stream.receive::<HelloMessage>().await {
        Ok(HelloMessage::HelloFromClient) => {
            // Handle client connection.
            match stream.send(&ServerToClientMessage::WelcomeClient).await {
                Ok(()) => {
                    log::trace!("Exchanged welcome handshake with client!");
                    let mut client = ClientConnection::new(stream, config, shared, cancel_token);
                    client.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeClient: {}", e),
            }
        }
        Ok(HelloMessage::HelloFromWorker { worker_name }) => {
            // Handle worker connection.
            match stream.send(&ServerToWorkerMessage::WelcomeWorker).await {
                Ok(()) => {
                    log::trace!("Exchanged welcome handshake with worker '{}'!", worker_name);
                    let mut worker = WorkerConnection::new(
                        worker_name.clone(),
                        stream,
                        config,
                        shared,
                        cancel_token,
                    );
                    worker.run().await;
                }
                Err(e) => log::error!("Failed to send WelcomeWorker: {}", e),
            }
            log::info!("Connection to worker {} closed!", worker_name);
        }
        // Connected client failed to identify correctly.
        Err(e) => log::error!("Failed to read HelloMessage: {}", e),
    }
    // Client or worker session ended.
    drop(keep_alive);
}
