use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub mod types;
use types::RESPType;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async {
                    handle_connection(stream)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    // let mut buf = BytesMut::with_capacity(1024);
    let mut buf = [0; 1024];

    loop {
        let len = stream.read(&mut buf).await?;
        if len == 0 {
            continue;
        }
        dbg!(len);
        let mut buf = Bytes::copy_from_slice(&buf);
        let cmd = RESPType::parse(&mut buf)?;
        dbg!(cmd);
        stream.write_all(b"+PONG\r\n").await?;
    }
}
