use anyhow::Result;
use std::{
    error::Error,
    io::{Read, Write},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    match listener.accept().await {
        Ok((stream, _addr)) => {
            _ = handle_connection(stream)
                .await
                .map_err(|e| println!("error: {}", e))
        }
        Err(e) => println!("error: {}", e),
    }

    Ok(())
}

async fn handle_connection(mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buf = [0; 1024];
    loop {
        let _len = stream.read(&mut buf);
        stream.write("+PONG\r\n".as_bytes()).await?;
    }

    Ok(())
}
