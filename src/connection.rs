use std::{
    ops::Add,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
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
    types::{Bulk, Config, Db, Entry},
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

    process_rdb_file(file, db.clone()).await?;

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
    Auxiliary = 0xFA,
    ResizeDB = 0xFB,
    ExpireTimeMs = 0xFC,
    ExpireTime = 0xFD,
    SelectDB = 0xFE,
    EOF = 0xFF,
}

impl RDBOpcode {
    const fn from(val: u8) -> Option<Self> {
        Some(match val {
            0xFA => Self::Auxiliary,
            0xFB => Self::ResizeDB,
            0xFC => Self::ExpireTimeMs,
            0xFD => Self::ExpireTime,
            0xFE => Self::SelectDB,
            0xFF => Self::EOF,
            _ => return None,
        })
    }
}

pub async fn process_rdb_file(file: Vec<u8>, db: Db) -> Result<()> {
    let mut file = Bytes::from(file);
    let b"REDIS" = file.split_to(5).as_ref() else {
        bail!("REDIS fingerprint missing");
    };

    let _version = file.split_to(4);

    while !file.is_empty() {
        let opcode = file.get_u8();
        match RDBOpcode::from(opcode) {
            Some(RDBOpcode::Auxiliary) => {
                let _key = parse_string(&mut file)?;
                let _val = parse_string(&mut file)?;
            }
            Some(RDBOpcode::ResizeDB) => {
                let _ht_len = parse_int(&mut file)?;
                let _ht_expiry_len = parse_int(&mut file)?;
            }
            Some(RDBOpcode::ExpireTimeMs) => {
                let ex = file.get_u64_le() as u128;
                let val_type = file.get_u8();
                let (key, val) = parse_key_val(val_type, &mut file)?;

                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                let ex = if ex < now {
                    continue;
                } else {
                    SystemTime::now().add(Duration::from_millis((ex - now) as u64))
                };

                let mut db = db.lock().await;
                db.insert(key.into(), (val, Some(ex)).into());
            }
            Some(RDBOpcode::ExpireTime) => {
                let ex = file.get_u32_le() as u64;
                let val_type = file.get_u8();
                let (key, val) = parse_key_val(val_type, &mut file)?;

                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                let ex = if ex < now {
                    continue;
                } else {
                    SystemTime::now().add(Duration::from_secs(ex - now))
                };

                let mut db = db.lock().await;
                db.insert(key.into(), (val, Some(ex)).into());
            }
            Some(RDBOpcode::SelectDB) => {
                let _db_num = file.get_u8();
            }
            Some(RDBOpcode::EOF) => break, // TODO: checksum
            None => {
                let val_type = opcode;
                let (key, val) = parse_key_val(val_type, &mut file)?;
                let mut db = db.lock().await;
                db.insert(key.into(), (val, None).into());
            }
        }
    }

    enum Len {
        One(u8),
        Two(u8),
        Four,
        Special(u8),
    }

    enum StringType {
        String(Bytes),
        Int(u32),
    }

    impl Into<Bulk> for StringType {
        fn into(self) -> Bulk {
            match self {
                StringType::String(s) => Bulk::from(String::from_utf8_lossy(s.as_ref()).as_ref()),
                StringType::Int(i) => Bulk::from(i.to_string().as_ref()),
            }
        }
    }

    impl Into<Entry> for (StringType, Option<SystemTime>) {
        fn into(self) -> Entry {
            Entry {
                val: self.0.into(),
                timeout: self.1,
            }
        }
    }

    fn parse_len(file: &mut Bytes) -> Len {
        const MASK: u8 = 0b1100_0000;
        let len = file.get_u8();
        let typ = (len & MASK) >> 6;
        let len = len & !MASK;

        match typ {
            0b00 => Len::One(len),
            0b01 => Len::Two(len),
            0b10 => Len::Four,
            0b11 => Len::Special(len),
            _ => unreachable!(),
        }
    }

    fn parse_int(file: &mut Bytes) -> Result<StringType> {
        let typ = parse_len(file);
        Ok(StringType::Int(match typ {
            Len::One(len) => len as u32,
            Len::Two(len) => {
                let second = file.get_u8() as u16;
                let mut len = (len as u16) << 8;
                len |= second;
                len as u32
            }
            Len::Four => file.get_u32(),
            Len::Special(fmt) => match fmt {
                0 => file.get_u8() as u32,
                1 => file.get_u16() as u32,
                2 => file.get_u32(),
                s => bail!("Invalid len {s}"),
            },
        }))
    }

    fn parse_string(file: &mut Bytes) -> Result<StringType> {
        let typ = parse_len(file);
        Ok(match typ {
            Len::One(len) => StringType::String(file.split_to(len as usize)),
            Len::Two(len) => {
                let second = file.get_u8() as u16;
                let mut len = (len as u16) << 8;
                len |= second;
                StringType::String(file.split_to(len as usize))
            }
            Len::Four => {
                let len = file.get_u32();
                StringType::String(file.split_to(len as usize))
            }
            Len::Special(fmt) => StringType::Int(match fmt {
                0 => file.get_u8() as u32,
                1 => file.get_u16() as u32,
                2 => file.get_u32(),
                s => bail!("Invalid len {s}"),
            }),
        })
    }

    fn parse_key_val(val_type: u8, file: &mut Bytes) -> Result<(StringType, StringType)> {
        let key = parse_string(file)?;
        let val = match val_type {
            0 => parse_string(file)?,
            _ => todo!(),
        };

        Ok((key, val))
    }

    Ok(())
}
