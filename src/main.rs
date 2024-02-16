use anyhow::Result;
use bytes::Bytes;
use std::{collections::HashMap, io::Write, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

pub mod respcmd;
pub mod resptype;
pub mod types;
pub mod work;

use crate::{
    respcmd::{Conf, RESPCmd},
    resptype::RESPType,
    types::{parse_args, Bulk, Config, Db, Entry},
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.read().await.port)).await?;
    let db = Arc::new(Mutex::new(HashMap::<Bulk, Entry>::new()));

    if let Some((ref host, ref port)) = config.read().await.replica_of {
        let _ = handshake(host, port, &config).await;
    };

    let is_master = config.read().await.is_master();

    if is_master {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let db = db.clone();
                    let config = config.clone();
                    tokio::spawn(async move {
                        master_handle_connection(stream, db, config, peer_addr)
                            .await
                            .map_err(|e| println!("error: {}", e))
                    });
                }
                Err(e) => println!("error: {}", e),
            }
        }
    }

    // rust_analyzer didn't like `else`
    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let db = db.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    slave_handle_connection(stream, db, config, peer_addr)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}

async fn master_handle_connection(
    mut stream: TcpStream,
    db: Db,
    args: Config,
    peer_addr: SocketAddr,
) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes

    loop {
        let len = stream.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf[..len]);
        let cmd = RESPCmd::parse(RESPType::parse(&mut buf)?)?;
        match cmd {
            RESPCmd::Set(_) => {
                // TODO: can we do this concurrently?
                for replica in args.write().await.replicas.iter_mut() {
                    dbg!(&replica);
                    replica.write_all(&buf)?;
                }
            }
            _ => {}
        }

        let resp = work::handle_command(cmd, db.clone(), args.clone(), peer_addr).await?;
        stream.write_all(&resp.as_bytes()).await?;
    }
}

async fn slave_handle_connection(
    mut stream: TcpStream,
    db: Db,
    args: Config,
    peer_addr: SocketAddr,
) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes

    loop {
        let len = stream.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf);
        let cmd = RESPCmd::parse(RESPType::parse(&mut buf)?)?;

        let resp = work::handle_command(cmd, db.clone(), args.clone(), peer_addr).await?;
        stream.write_all(&resp.as_bytes()).await?;
    }
}

async fn handshake(host: &String, port: &String, args: &Config) -> Result<()> {
    let mut buf = [0; 512];
    let mut conn = TcpStream::connect(format!("{host}:{port}")).await?;

    conn.write_all(&RESPCmd::Ping.as_bytes()).await?;

    conn.read(&mut buf).await?; // TODO: check PONG

    conn.write_all(
        &RESPCmd::ReplConf((Conf::ListeningPort, Bulk::from(&args.read().await.port))).as_bytes(),
    )
    .await?;

    conn.read(&mut buf).await?; // TODO: check OK

    conn.write_all(&RESPCmd::ReplConf((Conf::Capa, Bulk::from("psync2"))).as_bytes())
        .await?;

    conn.read(&mut buf).await?; // TODO: check OK

    conn.write_all(&RESPCmd::Psync((Bulk::from("?"), Bulk::from("-1"))).as_bytes())
        .await?;

    conn.read(&mut buf).await?; // TODO: parse fullresync (simple string)
                                // TODO: parse empty RDB file

    Ok(())
}
