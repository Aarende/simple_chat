use tokio::sync::broadcast;

// Комната, внутри которой рассылаются сообщения
pub struct Room {
    pub bc_sender: broadcast::Sender<String>
}

impl Room {
    pub fn new() -> Self {
        Room {
            bc_sender: broadcast::channel::<String>(100).0
        }
    }
}