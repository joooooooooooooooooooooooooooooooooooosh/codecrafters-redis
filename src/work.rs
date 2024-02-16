use anyhow::Result;
use bytes::Bytes;
use std::time::SystemTime;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc::UnboundedSender,
};

use crate::{
    bulk,
    respcmd::{Conf, RESPCmd},
    resptype::RESPType,
    types::{Bulk, Config, Db, Entry},
};

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

pub async fn handle_command_master(
    cmd: RESPCmd,
    db: Db,
    config: Config,
    tx: &UnboundedSender<Bytes>,
) -> Result<RESPType> {
    Ok(match cmd {
        RESPCmd::ReplConf((conf, arg)) => handle_replconf(conf, arg, config, tx).await?,
        RESPCmd::Psync((id, offset)) => handle_psync(id, offset).await?,
        _ => handle_command(cmd, db, config).await?,
    })
}

pub async fn handle_command(cmd: RESPCmd, db: Db, config: Config) -> Result<RESPType> {
    Ok(match cmd {
        RESPCmd::Echo(echo) => RESPType::Bulk(Some(echo)),
        RESPCmd::Ping => RESPType::String(String::from("PONG")),
        RESPCmd::Set((key, val, timeout)) => handle_set(key, val, timeout, db).await,
        RESPCmd::Get(key) => handle_get(key, db).await,
        RESPCmd::Info(topic) => handle_info(topic, config).await,
        RESPCmd::ReplConf((c @ Conf::GetAck, arg)) => handle_replconf_replica(c, arg)?,
        RESPCmd::FullResync(_) => todo!(),
        _ => unimplemented!(), // shouldn't be needed on a replica
    })
}

async fn handle_replconf(
    conf: Conf,
    arg: Bulk,
    config: Config,
    tx: &UnboundedSender<Bytes>,
) -> Result<RESPType> {
    match conf {
        Conf::ListeningPort => {
            config.write().await.replicas.push(tx.clone());
            Ok(RESPType::String(String::from("OK")))
        }
        other => handle_replconf_replica(other, arg),
    }
}

fn handle_replconf_replica(conf: Conf, _arg: Bulk) -> Result<RESPType> {
    match conf {
        Conf::Capa => Ok(RESPType::String(String::from("OK"))),
        Conf::GetAck => Ok(RESPCmd::ReplConf((Conf::Ack, bulk!("0"))).to_command()),
        Conf::Ack => todo!(),
        Conf::ListeningPort => unreachable!(),
    }
}

async fn handle_set(key: Bulk, val: Bulk, timeout: Option<SystemTime>, db: Db) -> RESPType {
    let mut db = db.lock().await;
    db.insert(key, Entry { val, timeout });

    RESPType::String(String::from("OK"))
}

async fn handle_get(key: Bulk, db: Db) -> RESPType {
    let db = db.lock().await;

    let val = db.get(&key).and_then(|e| {
        if e.timeout.is_some_and(|timeout| timeout < SystemTime::now()) {
            None
        } else {
            Some(e.val.clone())
        }
    });

    RESPType::Bulk(val)
}

async fn handle_info(topic: Option<Bulk>, args: Config) -> RESPType {
    // TODO: handle INFO with no topic (all sections)
    let Some(topic) = topic else { unimplemented!() };

    RESPType::Bulk(Some(match topic.as_bytes() {
        b"replication" => info_replication(args).await,
        _ => unimplemented!(),
    }))
}

async fn info_replication(args: Config) -> Bulk {
    let role = match args.read().await.is_master() {
        true => "master",
        false => "slave",
    };
    Bulk::from(
        format!(
            "\
role:{role}
master_replid:{REPLICATION_ID}
master_repl_offset:0
"
        )
        .as_str(),
    )
}

async fn handle_psync(_id: Bulk, _offset: Bulk) -> Result<RESPType> {
    let mut rdb = Vec::new();
    File::open("./redis.rdb")
        .await?
        .read_to_end(&mut rdb)
        .await?;

    Ok(RESPType::Multi(vec![
        RESPCmd::FullResync((bulk!(REPLICATION_ID), bulk!("0"))).to_command(),
        RESPType::RDBFile(rdb),
    ]))
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
    match RESPType::parse(&mut buff) {
        Ok(_) => {}
        Err(_) => {
            while let 0 = conn.read(&mut buf).await? {}
            let mut buff = Bytes::copy_from_slice(&buf);
            RESPType::parse(&mut buff)?;
        }
    }

    // TODO: verify full resync and rdb file

    // TODO: reduce hacky duplication
    while let Ok(cmd) = RESPType::parse(&mut buff) {
        let cmd = RESPCmd::parse(cmd)?;
        let resp = handle_command(cmd.clone(), db.clone(), config.clone()).await?;
        match cmd {
            RESPCmd::ReplConf((Conf::GetAck, _)) => conn.write_all(&resp.as_bytes()).await?,
            _ => {}
        }
    }

    loop {
        let len = conn.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf[..len]);
        while let Ok(cmd) = RESPType::parse(&mut buf) {
            let cmd = RESPCmd::parse(cmd)?;
            let resp = handle_command(cmd.clone(), db.clone(), config.clone()).await?;
            match cmd {
                RESPCmd::ReplConf((Conf::GetAck, _)) => conn.write_all(&resp.as_bytes()).await?,
                _ => {}
            }
        }
    }
}
