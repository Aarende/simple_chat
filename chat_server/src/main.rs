use std::{env, sync::Arc};
use tokio::net::TcpListener;
use chat_server::Chat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Создаём чат
    let chat = Chat::new();

    // Открываем подключение
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;

    let chat = Arc::new(chat);
    
    // Обрабатываем подключения
    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Новое подключение: {}", addr);
        
        let chat_clone = Arc::clone(&chat); 

        tokio::spawn(async move {
            chat_clone.handle_connection(stream, addr).await;
        });
    }
}
