use std::time::SystemTime;

use crate::types::{Args, Bulk, Db, Entry, RESPCmd, RESPType};
use anyhow::Result;

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

pub async fn handle_command(cmd: RESPType, db: Db, args: Args) -> Result<RESPType> {
    let cmd = RESPCmd::parse(cmd)?;

    Ok(match cmd {
        RESPCmd::Echo(echo) => RESPType::Bulk(Some(echo)),
        RESPCmd::Ping => RESPType::String(String::from("PONG")),
        RESPCmd::Set((key, val, timeout)) => handle_set(key, val, timeout, db).await,
        RESPCmd::Get(key) => handle_get(key, db).await,
        RESPCmd::Info(topic) => handle_info(topic, args),
    })
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

fn handle_info(topic: Bulk, args: Args) -> RESPType {
    RESPType::Bulk(Some(match topic.as_bytes() {
        b"replication" => info_replication(args),
        _ => unimplemented!(),
    }))
}

fn info_replication(args: Args) -> Bulk {
    let role = match args.replica_of {
        Some(_) => "slave",
        None => "master",
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
