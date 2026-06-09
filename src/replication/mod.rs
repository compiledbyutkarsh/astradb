use tokio::sync::broadcast;

#[derive(
    Clone,
    Debug,
)]
pub struct ReplicationEvent {
    pub operation: String,

    pub key: String,

    pub value: Option<String>,
}

#[derive(Clone)]
pub struct ReplicationBus {
    sender:
        broadcast::Sender<
            ReplicationEvent,
        >,
}

impl ReplicationBus {
    pub fn new() -> Self {
        let (
            sender,
            _,
        ) = broadcast::channel(1024);

        Self { sender }
    }

    pub fn publish(
        &self,
        event: ReplicationEvent,
    ) {
        let _ =
            self.sender.send(event);
    }

    pub fn subscribe(
        &self,
    ) -> broadcast::Receiver<
        ReplicationEvent,
    > {
        self.sender.subscribe()
    }
}