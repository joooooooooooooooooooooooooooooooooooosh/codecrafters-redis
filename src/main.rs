use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

pub mod types;
pub mod work;
use types::{parse_args, Args, Bulk, Db, Entry, RESPType};

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;
    let db = Arc::new(Mutex::new(HashMap::<Bulk, Entry>::new()));

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let db = db.clone();
                let args = args.clone();
                tokio::spawn(async move {
                    handle_connection(stream, db, args)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}

async fn handle_connection(mut stream: TcpStream, db: Db, args: Args) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes

    loop {
        let len = stream.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf);
        let cmd = RESPType::parse(&mut buf)?;

        let resp = work::handle_command(cmd, db.clone(), args.clone()).await?;
        stream.write_all(&resp.to_bytes()).await?;
    }
}
