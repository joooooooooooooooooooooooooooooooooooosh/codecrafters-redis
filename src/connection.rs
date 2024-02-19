use core::slice::SlicePattern;

use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpListener, TcpStream},
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{self, UnboundedSender},
    },
};

use crate::{
    respcmd::{Conf, RESPCmd},
    resptype::RESPType,
    types::{Bulk, Config, Db},
    work::{self, handle_command},
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
    let (bx, mut rx) = broadcast::channel(16);
    let bx2 = bx.clone();

    tokio::spawn(async move {
        let mut buf = [0; 1024];
        loop {
            let len = reader.read(&mut buf).await.expect("problem reading");
            if len == 0 {
                continue;
            }

            let buf = Bytes::copy_from_slice(&buf[..len]);
            bx.send(buf).expect("problem sending");
        }
    });

    loop {
        let mut buf = match rx.recv().await {
            Ok(b) => b,
            Err(RecvError::Lagged(n)) => bail!("missed {n} commands"),
            e @ Err(_) => e?,
        };

        while let Ok((cmd, len)) = RESPType::parse(&mut buf) {
            let cmd = RESPCmd::parse(cmd)?;
            match cmd {
                RESPCmd::Set(_) => {
                    for (replica, _, _) in config.write().await.replicas.iter_mut() {
                        replica.send(cmd.clone().as_bytes())?;
                    }
                }
                _ => {}
            }

            if let Some(resp) =
                work::handle_command_master(cmd.clone(), db.clone(), config.clone(), &tx, &bx2)
                    .await?
            {
                tx.send(resp.as_bytes().freeze())?;
            };

            match cmd {
                RESPCmd::Set(_) => {
                    config.write().await.offset += len;
                }
                _ => {}
            }
        }
    }
}

pub async fn replica_handle_connection(
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

        let mut buf = Bytes::copy_from_slice(&buf[..len]);
        while let Ok((cmd, len)) = RESPType::parse(&mut buf) {
            let cmd = RESPCmd::parse(cmd)?;
            if let Some(resp) = work::handle_command(cmd, db.clone(), config.clone()).await? {
                stream.write_all(&resp.as_bytes()).await?;
            }

            config.write().await.offset += len;
        }
    }
}

pub async fn connect_to_master(host: String, port: String, config: Config, db: Db) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes
    let mut conn = TcpStream::connect(format!("{host}:{port}")).await?;

    conn.write_all(&RESPCmd::Ping.as_bytes()).await?;

    while let 0 = conn.read(&mut buf).await? {} // TODO: check OK

    conn.write_all(
        &RESPCmd::ReplConf((Conf::ListeningPort, Bulk::from(&config.read().await.port))).as_bytes(),
    )
    .await?;

    while let 0 = conn.read(&mut buf).await? {} // TODO: check OK

    conn.write_all(&RESPCmd::ReplConf((Conf::Capa, Bulk::from("psync2"))).as_bytes())
        .await?;

    while let 0 = conn.read(&mut buf).await? {} // TODO: check OK

    conn.write_all(&RESPCmd::Psync((Bulk::from("?"), Bulk::from("-1"))).as_bytes())
        .await?;

    while let 0 = conn.read(&mut buf).await? {}

    let mut buff = Bytes::copy_from_slice(&buf);
    let _full_resync = RESPType::parse(&mut buff)?;

    // Recieve RDBFile
    let (RESPType::RDBFile(file), _len) = (match RESPType::parse(&mut buff) {
        Ok(f) => f,
        Err(_) => {
            while let 0 = conn.read(&mut buf).await? {}
            let mut buff = Bytes::copy_from_slice(&buf);
            RESPType::parse(&mut buff)?
        }
    }) else {
        bail!("Did not receive RDB file");
    };

    dbg!(Bytes::copy_from_slice(file.as_slice()));

    // process_rdb_file(file)?;

    // TODO: verify full resync and rdb file

    // TODO: reduce hacky duplication
    while let Ok((cmd, len)) = RESPType::parse(&mut buff) {
        let cmd = RESPCmd::parse(cmd)?;
        if let Some(resp) = handle_command(cmd.clone(), db.clone(), config.clone()).await? {
            match cmd {
                RESPCmd::ReplConf((Conf::GetAck, _)) => conn.write_all(&resp.as_bytes()).await?,
                _ => {}
            }
        }

        config.write().await.offset += len;
    }

    loop {
        let len = conn.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf[..len]);
        while let Ok((cmd, len)) = RESPType::parse(&mut buf) {
            let cmd = RESPCmd::parse(cmd)?;
            if let Some(resp) = handle_command(cmd.clone(), db.clone(), config.clone()).await? {
                match cmd {
                    RESPCmd::ReplConf((Conf::GetAck, _)) => {
                        conn.write_all(&resp.as_bytes()).await?
                    }
                    _ => {}
                }
            }

            config.write().await.offset += len;
        }
    }
}

#[repr(u8)]
enum RDBOpcode {
    EOF = 0xFF,
    SelectDB = 0xFE,
    ExpireTime = 0xFD,
    ExpireTimeMs = 0xFC,
    ResizeDB = 0xFB,
    Auxiliary = 0xFA,
}

impl RDBOpcode {
    const fn from(val: u8) -> Option<Self> {
        Some(match val {
            0xFF => Self::EOF,
            0xFE => Self::SelectDB,
            0xFD => Self::ExpireTime,
            0xFC => Self::ExpireTimeMs,
            0xFB => Self::ResizeDB,
            0xFA => Self::Auxiliary,
            _ => return None,
        })
    }
}

fn process_rdb_file(file: Vec<u8>) -> Result<()> {
    let mut file = Bytes::from(file);
    let b"REDIS" = file.split_to(5).as_ref() else {
        bail!("REDIS fingerprint missing");
    };

    let _version = file.split_to(4);

    while !file.is_empty() {
        match RDBOpcode::from(file[0]) {
            Some(RDBOpcode::EOF) => break, // TODO: checksum
            Some(RDBOpcode::SelectDB) => {}
            Some(RDBOpcode::ExpireTime) => {}
            Some(RDBOpcode::ExpireTimeMs) => {}
            Some(RDBOpcode::ResizeDB) => {}
            Some(RDBOpcode::Auxiliary) => {}
            None => {}
        }
    }

    Ok(())
}
