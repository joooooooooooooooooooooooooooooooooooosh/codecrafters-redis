use anyhow::Result;
use std::{
    net::{SocketAddr, TcpStream},
    time::SystemTime,
};
use tokio::{fs::File, io::AsyncReadExt};

use crate::{
    bulk,
    respcmd::{Conf, RESPCmd},
    resptype::RESPType,
    types::{Bulk, Config, Db, Entry},
};

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

pub async fn handle_command(
    cmd: RESPCmd,
    db: Db,
    config: Config,
    peer_addr: SocketAddr,
) -> Result<RESPType> {
    Ok(match cmd {
        RESPCmd::Echo(echo) => RESPType::Bulk(Some(echo)),
        RESPCmd::Ping => RESPType::String(String::from("PONG")),
        RESPCmd::Set((key, val, timeout)) => handle_set(key, val, timeout, db).await,
        RESPCmd::Get(key) => handle_get(key, db).await,
        RESPCmd::Info(topic) => handle_info(topic, config).await,
        RESPCmd::ReplConf((conf, arg)) => handle_replconf(conf, arg, config, peer_addr).await?,
        RESPCmd::Psync((id, offset)) => handle_psync(id, offset).await?,
        RESPCmd::FullResync(_) => todo!(),
    })
}

async fn handle_replconf(
    conf: Conf,
    arg: Bulk,
    config: Config,
    mut peer_addr: SocketAddr,
) -> Result<RESPType> {
    match conf {
        Conf::ListeningPort => {
            dbg!(peer_addr, &arg);
            peer_addr.set_port(dbg!(arg.to_string().parse()?));
            dbg!(peer_addr);
            let conn = TcpStream::connect(peer_addr)?;
            dbg!(&conn);
            config.write().await.replicas.push(conn);
        }
        Conf::Capa => {
            // TODO: handle properly
        }
    }

    Ok(RESPType::String(String::from("OK")))
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
    dbg!("here");
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
