use config::Config;

pub struct TestWorker {
    config: Config,
}

impl TestWorker {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::Config, server::TestServer};
    use tokio::io::duplex;

    #[tokio::test]
    async fn server_and_worker() {
        let config = Config::new(None).unwrap();
        let server = TestServer::new(config);
        // let worker = TestWorker::new(config);

        // let (mut server_stream, mut worker_stream) = duplex(64);
        // server.connect(server_stream);
        // worker.connect(worker_stream);

        // // TODO: Can we test if the worker is connected?
        // // TODO: Should we do it even here?
        // sleep(Duration::from_millis(100)).await;

        // worker.stop();
        // server.stop();
    }
}