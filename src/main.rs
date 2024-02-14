use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub mod types;
use types::{RESPCmd, RESPType};

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
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes

    loop {
        let len = stream.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf);
        let cmd = RESPType::parse(&mut buf)?;

        let resp = handle_command(cmd)?;

        stream.write_all(&resp.to_bytes()).await?;
    }
}

fn handle_command(cmd: RESPType) -> Result<RESPType> {
    let cmd = RESPCmd::parse(cmd)?;

    Ok(match cmd {
        RESPCmd::Echo(echo) => RESPType::Bulk(Some(echo)),
        RESPCmd::Ping => RESPType::String(String::from("PONG")),
    })
}
