use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use futures_util::StreamExt;
use super::{WebSocketReceiver, WebSocketSender, ReadResult};

// Структура для получения сообщений от пользователя
pub struct UserReceiver {
    pub nickname: String,
    pub ws_receiver: WebSocketReceiver,
    pub bc_sender: broadcast::Sender<String>
}

impl UserReceiver {
    pub fn new(nickname: String, ws_receiver: WebSocketReceiver, bc_sender: broadcast::Sender<String>) -> Self {
        UserReceiver {
            nickname,
            ws_receiver,
            bc_sender
        }
    }
    pub async fn read_message(&mut self) -> ReadResult {
        // Обрабатываем сообщение от пользователя
        match self.ws_receiver.next().await {
            Some(Ok(Message::Text(msg))) => {
                let msg = msg.trim().to_string();
                if msg == "/quit" {
                    ReadResult::Quit
                } else {
                    ReadResult::Message(msg)
                }
            }
            Some(Ok(Message::Close(_))) | None => ReadResult::Disconnected,
            Some(Err(e)) => {
                eprintln!("Ошибка чтения от {}: {}", self.nickname, e);
                ReadResult::Disconnected
            }
            _ => ReadResult::Message(String::new()), // Ping/Pong
        }
    }
}


// Структура для отправки сообщений пользователю
pub struct UserSender {
    pub ws_sender: WebSocketSender,
    pub bc_receiver: broadcast::Receiver<String>
}

impl UserSender {
    pub fn new(ws_sender: WebSocketSender, bc_receiver: broadcast::Receiver<String>) -> Self {
        UserSender {
            ws_sender,
            bc_receiver
        }
    }
}