use std::net::SocketAddr;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{Mutex, broadcast};
use tokio::net::TcpStream;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use chrono::Local;
use crate::chat_utils::{ReadResult, {user::{UserReceiver, UserSender}, room::Room}};

// Основная структура - чат, хранящий множество комнат
pub struct Chat {
    rooms: Arc<Mutex<HashMap<u32, Room>>>,
}

impl Default for Chat {
    fn default() -> Self {
        Chat { rooms: Arc::new(Mutex::new(HashMap::new())) }
    }
}

impl Chat {
    pub fn new() -> Self {
        Chat::default()
    }
    pub async fn get_room_sender(&self, room_id: u32) -> broadcast::Sender<String> {
        let mut rooms = self.rooms.lock().await;
        
        let room = rooms.entry(room_id).or_insert_with(|| {
            println!("Создана комната {}", room_id);
            Room::new()
        });
        
        room.bc_sender.clone()
    }
    pub async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) {

        // Handshak'им соединение
        let ws_stream = match accept_async(stream).await {
            Ok(ws) => ws,
            Err(_) => {
                eprintln!("Ошибка handshake для {}", addr);
                return;
            }
        };
        println!("WebSocket handshake выполнен для {}", addr);

        let (mut ws_sender, mut ws_receiver) = ws_stream.split();
        
        // Получаем никнейм
        let nickname = loop {
            match ws_receiver.next().await {
                Some(Ok(Message::Text(nickname))) => {
                    let nickname = nickname.trim().to_string();
                    if !nickname.is_empty() {
                        let _ = ws_sender.send(Message::Text("Success".into())).await;
                        break nickname;
                    } else if let Err(e) = ws_sender.send(Message::Text("❌ Ошибка: никнейм не должен быть пустым, попробуйте ещё раз:".into())).await {
                        eprintln!("Клиент {} столкнулся с ошибкой при вводе никнейма: {}", addr, e);
                        return;    
                    }
                }
                Some(Ok(Message::Close(_))) => {
                    eprintln!("Клиент {} отключился при вводе никнейма", addr);
                    return;
                }
                Some(Err(e)) => {
                    eprintln!("Клиент {} столкнулся с ошибкой при вводе никнейма: {}", addr, e);
                    return;
                }
                _ => {}
            }
        };

        // Получаем id комнаты, в которую пользователь хочет попасть
        let room_id = loop {
            match ws_receiver.next().await {
                Some(Ok(Message::Text(room_id))) => {
                    let room_id = room_id.trim().to_string();
                    if let Ok(room_id) = room_id.parse::<u32>() {
                        let _ = ws_sender.send(Message::Text("Success".into())).await;
                        break room_id;
                    } else if let Err(e) = ws_sender.send(Message::Text("❌ Ошибка: id должен быть неотрицательным числом, попробуйте ещё раз:".into())).await {
                        eprintln!("Клиент {} столкнулся с ошибкой при вводе id комнаты: {}", addr, e);
                        return;
                    }
                }
                Some(Ok(Message::Close(_))) => {
                    eprintln!("Клиент {} отключился при вводе id комнаты", addr);
                    return;
                }
                Some(Err(e)) => {
                    eprintln!("Клиент {} столкнулся с ошибкой при вводе id комнаты: {}", addr, e);
                    return;
                }
                _ => {}
            }
        };

        let _ = ws_sender.send(Message::Text("📝 Подключение успешно, добро пожаловать в чат! Введите /quit для выхода: \n".into())).await;

        // Получаем копию канала
        let bc_sender = self.get_room_sender(room_id).await;

        // Отправляем уведомление всем членам комнаты о подключении нового пользователя
        let _ = bc_sender.send(format!("[{}] {}: {} подключился к чату!",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            "Server",
            nickname
        ));

        // Создаём пользователей для чтения и отправки сообщений
        let mut new_user_sender = UserSender::new(ws_sender, bc_sender.subscribe());
        let mut new_user_receiver = UserReceiver::new(nickname.clone(), ws_receiver, bc_sender);

        // Задача для отправки сообщений клиенту
        let sending_task = tokio::spawn(async move {
            loop {
                match new_user_sender.bc_receiver.recv().await {
                    Ok(msg) => {
                        if new_user_sender.ws_sender.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("{} пропустил {} сообщений", nickname, n);
                        continue;
                    }
                }
            }
        });

        // Задача для чтения сообщений от клиента
        let reading_task = tokio::spawn(async move {
            loop {
                match new_user_receiver.read_message().await {
                    ReadResult::Quit => {
                        println!("{} вышел из чата", new_user_receiver.nickname);
                        let _ = new_user_receiver.bc_sender.send(format!("[{}] {}: {} отключился от чата!",
                            Local::now().format("%Y-%m-%d %H:%M:%S"),
                            "Server",
                            new_user_receiver.nickname
                        ));
                        break;
                    }
                    ReadResult::Message(msg) if !msg.is_empty() => {
                        let formatted = format!("[{}] {}: {}", 
                            Local::now().format("%Y-%m-%d %H:%M:%S"),
                            new_user_receiver.nickname, 
                            msg
                        );
                        if new_user_receiver.bc_sender.send(formatted).is_err() {
                            break;
                        }
                    }
                    ReadResult::Disconnected => {
                        println!("{} отключился", new_user_receiver.nickname);
                        let _ = new_user_receiver.bc_sender.send(format!("[{}] {}: {} отключился от чата!",
                            Local::now().format("%Y-%m-%d %H:%M:%S"),
                            "Server",
                            new_user_receiver.nickname
                        ));
                        break;
                    }
                    _ => {}
                }
            }
        });

        // Подключаем задачи и считаем, что после этого подключение обработано
        let _ = tokio::join!(sending_task, reading_task);

    }
}