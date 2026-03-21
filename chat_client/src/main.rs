use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{self, AsyncBufReadExt, BufReader};
use dotenv::dotenv;
use std::{env, io::Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Подгружаем переменные окружения из .env файла
    dotenv().ok();

    let server_url = env::var("CHAT_SERVER_URL")?;
    
    println!("🔗 Подключение к {}", server_url);
    
    // Пытаемся подключиться к серверу
    match connect_async(server_url).await {
        Ok((ws_stream, _)) => {
            println!("✅ Подключено к серверу!\n");
            
            let (mut write, mut read) = ws_stream.split();
            
            // Получаем никнейм
            print!("👤 Ваш никнейм: ");
            std::io::stdout().flush()?;
            let mut nickname = String::new();
            loop {
                std::io::stdin().read_line(&mut nickname)?;
                nickname = nickname.trim().to_string();
            
                // Отправляем никнейм
                write.send(Message::Text(nickname.clone().into())).await?;

                // Получаем ответ от сервера
                if let Some(Ok(Message::Text(msg))) = read.next().await {
                    let msg = msg.to_string();
                    if &msg == "Success" {
                        break;
                    }
                    println!("{}", msg);
                }

                // Очищаем строку после неудачного ввода
                std::io::stdout().flush()?;
                nickname = String::new();
            }
            
            // Получаем ID комнаты
            print!("🏠 ID комнаты: ");
            std::io::stdout().flush()?;
            let mut room_id = String::new();
            loop {
                std::io::stdin().read_line(&mut room_id)?;
                room_id = room_id.trim().to_string();
            
                // Отправляем ID
                write.send(Message::Text(room_id.clone().into())).await?;

                // Получаем ответ от сервера
                if let Some(Ok(Message::Text(msg))) = read.next().await {
                    let msg = msg.to_string();
                    if &msg == "Success" {
                        break;
                    }
                    println!("{}", msg);
                }

                // Очищаем строку после неудачного ввода
                std::io::stdout().flush()?;
                room_id = String::new();
            }
            
            // Ждём приветственное сообщение от сервера
            if let Some(Ok(Message::Text(msg))) = read.next().await {
                println!("{}", msg);
            }
            
            // Задача для чтения сообщений от сервера
            let read_task = tokio::spawn(async move {
                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            println!("{}", text);
                        }
                        Ok(Message::Close(_)) => {
                            println!("\n🔌 Сервер закрыл соединение");
                            break;
                        }
                        Err(e) => {
                            eprintln!("\n❌ Ошибка: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            });
            
            // Задача для отправки сообщений на сервер
            let write_task = tokio::spawn(async move {
                let stdin = io::stdin();
                let mut reader = BufReader::new(stdin).lines();
                
                while let Ok(Some(line)) = reader.next_line().await {
                    if line.trim() == "/quit" {
                        println!("👋 Выход из чата...");
                        let _ = write.send(Message::Close(None)).await;
                        break;
                    }
                    
                    if !line.is_empty() && let Err(e) = write.send(Message::Text(line.into())).await {
                        eprintln!("❌ Ошибка отправки: {}", e);
                        break;
                    }
                }
            });
            
            let _ = tokio::join!(read_task, write_task);
            
        }
        Err(e) => {
            eprintln!("❌ Не удалось подключиться к серверу!");
            eprintln!("Ошибка: {}", e);
        }
    }
    
    println!("\n👋 Чат завершён");
    Ok(())
    
}
