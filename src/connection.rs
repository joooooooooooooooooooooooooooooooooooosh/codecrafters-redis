use anyhow::Result;
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpListener, TcpStream},
    sync::mpsc::{self, UnboundedSender},
};

use crate::{
    respcmd::RESPCmd,
    resptype::RESPType,
    types::{Config, Db},
    work,
};

pub async fn master_listen(listener: TcpListener, db: Db, config: Config) -> ! {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
                let (reader, mut sender) = stream.into_split();
                tokio::spawn(async move {
                    while let Some(i) = rx.recv().await {
                        sender.write_all(&i).await.expect("problem sending")
                    }
                });

                let db = db.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    master_handle_connection(tx, reader, db, config)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}

pub async fn replica_listen(listener: TcpListener, db: Db, config: Config) -> ! {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let db = db.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    replica_handle_connection(stream, db, config)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}

pub async fn master_handle_connection(
    tx: UnboundedSender<Bytes>,
    mut reader: OwnedReadHalf,
    db: Db,
    config: Config,
) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes

    loop {
        let len = reader.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf[..len]);
        dbg!(&buf);
        let cmd = RESPCmd::parse(RESPType::parse(&mut buf)?)?;
        dbg!(&cmd);
        match cmd {
            RESPCmd::Set(_) => {
                for replica in config.write().await.replicas.iter_mut() {
                    replica.send(dbg!(cmd.clone().as_bytes().freeze()))?;
                    dbg!("alsdkfj");
                }
            }
            _ => {}
        }

        dbg!("alsdkfj");
        let resp = work::handle_command_master(cmd, db.clone(), config.clone(), &tx).await?;
        dbg!("alsdkfj");
        tx.send(resp.as_bytes().freeze())?;
        dbg!("alsdkfj");
    }
}

pub async fn replica_handle_connection<'a>(
    mut stream: TcpStream,
    db: Db,
    config: Config,
) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes

    loop {
        let len = stream.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf);
        let cmd = RESPCmd::parse(RESPType::parse(&mut buf)?)?;

        let resp = work::handle_command(cmd, db.clone(), config.clone()).await?;
        stream.write_all(&resp.as_bytes()).await?;
    }
}
