use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

pub mod respcmd;
pub mod resptype;
pub mod types;
pub mod work;

use crate::respcmd::{Conf, RESPCmd};
use crate::resptype::RESPType;
use types::{parse_args, Args, Bulk, Db, Entry};

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await?;
    let db = Arc::new(Mutex::new(HashMap::<Bulk, Entry>::new()));

    if let Some((ref host, ref port)) = args.replica_of {
        let _ = handshake(host, port, &args).await;
    }

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
        stream.write_all(&dbg!(resp.as_bytes())).await?;
    }
}

async fn handshake(host: &String, port: &String, args: &Args) -> Result<()> {
    let mut buf = [0; 512];
    let mut conn = TcpStream::connect(format!("{host}:{port}")).await?;

    conn.write_all(&RESPCmd::Ping.as_bytes()).await?;

    conn.read(&mut buf).await?; // TODO: check PONG

    conn.write_all(&RESPCmd::ReplConf((Conf::ListeningPort, Bulk::from(&args.port))).as_bytes())
        .await?;

    conn.read(&mut buf).await?; // TODO: check OK

    conn.write_all(&RESPCmd::ReplConf((Conf::Capa, Bulk::from("psync2"))).as_bytes())
        .await?;

    conn.read(&mut buf).await?; // TODO: check OK

    conn.write_all(&RESPCmd::Psync((Bulk::from("?"), Bulk::from("-1"))).as_bytes())
        .await?;

    conn.read(&mut buf).await?; // TODO: parse fullresync (simple string)
    conn.read(&mut buf).await?; // TODO: parse empty RDB file

    Ok(())
}
