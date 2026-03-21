use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
use futures_util::stream::{SplitSink, SplitStream};

pub mod user;
pub mod room;
pub mod chat;

pub type WebSocketReceiver = SplitStream<WebSocketStream<TcpStream>>;
pub type WebSocketSender = SplitSink<WebSocketStream<TcpStream>, Message>;

pub enum ReadResult {
    Message(String),
    Quit,
    Disconnected,
}