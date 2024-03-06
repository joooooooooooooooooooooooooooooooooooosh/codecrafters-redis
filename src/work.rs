use anyhow::{bail, Result};
use bytes::Bytes;
use std::{
    cmp::Ordering,
    collections::HashMap,
    ops::AddAssign,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::{broadcast::Sender, mpsc::UnboundedSender, RwLock},
    task::JoinSet,
    time,
};

use crate::{
    bulk,
    respcmd::{Conf, ConfGet, RESPCmd},
    resptype::RESPType,
    types::{Bulk, Config, Db, Entry, StreamEntry, StringEntry},
};

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

pub async fn handle_command_master(
    cmd: RESPCmd,
    db: Db,
    config: Config,
    tx: &UnboundedSender<Bytes>,
    bx: &Sender<Bytes>,
) -> Result<Option<RESPType>> {
    Ok(Some(match cmd {
        RESPCmd::ReplConf((conf, arg)) => handle_replconf(conf, arg, config, tx, bx).await?,
        RESPCmd::Psync((id, offset)) => handle_psync(id, offset, config).await?.into(),
        RESPCmd::Wait((num_replicas, timeout)) => {
            handle_wait(num_replicas, timeout, config).await.into()
        }
        _ => handle_command(cmd, db, config).await?.into(),
    })
    .flatten())
}

pub async fn handle_command(cmd: RESPCmd, db: Db, config: Config) -> Result<Option<RESPType>> {
    Ok(Some(match cmd {
        RESPCmd::Echo(echo) => RESPType::Bulk(Some(echo)).into(),
        RESPCmd::Ping => RESPType::String(String::from("PONG")).into(),
        RESPCmd::Set((key, val, timeout)) => handle_set(key, val, timeout, db).await.into(),
        RESPCmd::Get(key) => handle_get(key, db).await.into(),
        RESPCmd::Info(topic) => handle_info(topic, config).await.into(),
        RESPCmd::ReplConf((c @ Conf::GetAck, arg)) => {
            handle_replconf_replica(c, arg, config).await?
        }
        RESPCmd::FullResync(_) => todo!(),
        RESPCmd::Config((arg, confget)) => handle_config(arg, confget, config).await.into(),
        RESPCmd::Keys(arg) => handle_keys(arg, db).await?.into(),
        RESPCmd::Type(field) => handle_type(field, db).await.into(),
        RESPCmd::Xadd((field, id, map)) => handle_xadd(field, id, map, db).await?.into(),
        _ => unimplemented!(), // shouldn't be needed on a replica
    })
    .flatten())
}

async fn handle_xadd(field: Bulk, id: Bulk, map: HashMap<Bulk, Bulk>, db: Db) -> Result<RESPType> {
    fn err() -> Result<RESPType> {
        Ok(RESPType::Error(String::from(
            "ERR The ID specified in XADD is equal or smaller than the target stream top item",
        )))
    }

    // TODO: put stuff in stream
    // TODO: update rather than replace existing streams
    let id = id.as_string();

    let mut db = db.lock().await;
    let Entry::Stream(stream) = db.entry(field).or_insert(Entry::Stream(Default::default())) else {
        bail!("Called XADD on a non-stream");
    };

    let (id_ms_time, id_sq_num) = id.split_once('-').unwrap();
    let id_ms_time: usize = id_ms_time.parse()?;
    let id_sq_num: usize = if let Ok(num) = id_sq_num.parse() {
        num
    } else {
        if id_sq_num == "*" {
            let mut len = stream
                .iter()
                .filter(|e| e.id.0 == id_ms_time)
                .collect::<Vec<_>>()
                .len();
            if len == 0 && id_ms_time == 0 {
                len = 1;
            }
            len
        } else {
            return err();
        }
    };

    if id_ms_time == 0 && id_sq_num == 0 {
        return Ok(RESPType::Error(String::from(
            "ERR The ID specified in XADD must be greater than 0-0",
        )));
    }

    if let Some(last) = stream.last() {
        let (ms_time, sq_num) = last.id;

        match ms_time.cmp(&id_ms_time) {
            Ordering::Greater => return err(),
            Ordering::Equal if sq_num >= id_sq_num => return err(),
            _ => {}
        };
    }

    let event = StreamEntry {
        id: (id_ms_time, id_sq_num),
        vals: map,
    };
    stream.push(event);

    Ok(RESPType::String(format!("{id_ms_time}-{id_sq_num}")))
}

async fn handle_type(field: Bulk, db: Db) -> RESPType {
    RESPType::Bulk(Some(bulk!(match db.lock().await.get(&field) {
        Some(Entry::String(_)) => "string",
        Some(Entry::Stream(_)) => "stream",
        None => "none",
    })))
}

async fn handle_keys(_arg: Bulk, db: Db) -> Result<RESPType> {
    Ok(RESPType::Array(
        db.lock()
            .await
            .keys()
            .map(|k| RESPType::Bulk(Some(k.to_owned())))
            .collect(),
    ))
}

async fn handle_config(_arg: Bulk, confget: ConfGet, config: Config) -> RESPType {
    RESPType::Array(match confget {
        ConfGet::Dir => vec![
            RESPType::Bulk(Some(bulk!("dir"))),
            RESPType::Bulk(config.read().await.dir.as_ref().map(|val| bulk!(val))),
        ],
        ConfGet::DbFilename => vec![
            RESPType::Bulk(Some(bulk!("dbfilename"))),
            RESPType::Bulk(
                config
                    .read()
                    .await
                    .dbfilename
                    .as_ref()
                    .map(|val| bulk!(val)),
            ),
        ],
    })
}

async fn handle_wait(min_replicas: usize, timeout: usize, config: Config) -> RESPType {
    async fn ping_replicas(config: Config, num_responses: Arc<RwLock<usize>>, min_replicas: usize) {
        let master_offset = config.read().await.offset;

        let already_acked = config
            .read()
            .await
            .replicas
            .iter()
            .filter(|(_, _, offset)| offset >= &master_offset)
            .count();
        num_responses.write().await.add_assign(already_acked);

        if already_acked >= min_replicas {
            return;
        }

        let mut set = JoinSet::new();
        for (replica, receiver, offset) in config.read().await.replicas.iter() {
            if offset >= &master_offset {
                continue;
            }

            let num_responses = num_responses.clone();
            let replica = replica.clone();
            let mut receiver = receiver.resubscribe();

            set.spawn(async move {
                _ = replica.send(RESPCmd::ReplConf((Conf::GetAck, bulk!("*"))).as_bytes());
                loop {
                    match receiver.recv().await {
                        Ok(mut resp) => match RESPType::parse(&mut resp) {
                            Ok((cmd, _)) => match RESPCmd::parse(cmd) {
                                Ok(RESPCmd::ReplConf((Conf::Ack, _))) => {
                                    num_responses.write().await.add_assign(1);
                                    break;
                                }
                                _ => continue,
                            },
                            Err(_) => continue,
                        },
                        Err(_) => continue,
                    }
                }
            });
        }

        loop {
            set.join_next().await;
            if num_responses.read().await.clone() >= min_replicas {
                set.abort_all()
            }
        }
    }

    let num_responses = Arc::new(RwLock::const_new(0));
    if timeout == 0 {
        ping_replicas(config, num_responses.clone(), min_replicas).await;
    } else {
        let sleep = time::sleep(Duration::from_millis(timeout as u64));

        tokio::select! {
            _ = ping_replicas(config, num_responses.clone(), min_replicas) => {}
            _ = sleep => {}
        };
    }

    let resp = num_responses.read().await.clone();
    RESPType::Integer(resp as isize)
}

async fn handle_replconf(
    conf: Conf,
    arg: Bulk,
    config: Config,
    tx: &UnboundedSender<Bytes>,
    bx: &Sender<Bytes>,
) -> Result<Option<RESPType>> {
    match conf {
        Conf::ListeningPort => {
            config
                .write()
                .await
                .replicas
                .push((tx.clone(), bx.subscribe(), 0));
            Ok(RESPType::String(String::from("OK")).into())
        }
        other => handle_replconf_replica(other, arg, config).await,
    }
}

async fn handle_replconf_replica(
    conf: Conf,
    _arg: Bulk,
    config: Config,
) -> Result<Option<RESPType>> {
    Ok(match conf {
        Conf::Capa => RESPType::String(String::from("OK")).into(),
        Conf::GetAck => RESPCmd::ReplConf((
            Conf::Ack,
            bulk!(config.read().await.offset.to_string().as_str()),
        ))
        .to_command()
        .into(),
        Conf::Ack => None, // TODO: update offset
        Conf::ListeningPort => unreachable!(),
    })
}

async fn handle_set(key: Bulk, val: Bulk, timeout: Option<SystemTime>, db: Db) -> RESPType {
    let mut db = db.lock().await;
    db.insert(key, Entry::String(StringEntry { val, timeout }));

    RESPType::String(String::from("OK"))
}

async fn handle_get(key: Bulk, db: Db) -> RESPType {
    let db = db.lock().await;

    let val = db.get(&key).and_then(|e| match e {
        Entry::String(e) => {
            if e.timeout.is_some_and(|timeout| timeout < SystemTime::now()) {
                None
            } else {
                Some(e.val.clone())
            }
        }
        Entry::Stream(_) => todo!(),
    });

    RESPType::Bulk(val)
}

async fn handle_info(topic: Option<Bulk>, config: Config) -> RESPType {
    // TODO: handle INFO with no topic (all sections)
    let Some(topic) = topic else { unimplemented!() };

    RESPType::Bulk(Some(match topic.as_bytes() {
        b"replication" => info_replication(config).await,
        _ => unimplemented!(),
    }))
}

async fn info_replication(config: Config) -> Bulk {
    let role = match config.read().await.is_master() {
        true => "master",
        false => "slave",
    };

    // TODO: fetch master offset if this is a replica
    Bulk::from(
        format!(
            "\
role:{role}
master_replid:{REPLICATION_ID}
master_repl_offset:{}
",
            0 // TODO: should this be the current offset?
        )
        .as_str(),
    )
}

async fn handle_psync(_id: Bulk, _offset: Bulk, config: Config) -> Result<RESPType> {
    let mut rdb = Vec::new();
    File::open("./redis.rdb")
        .await?
        .read_to_end(&mut rdb)
        .await?;

    config.write().await.offset = 0;

    Ok(RESPType::Multi(vec![
        RESPCmd::FullResync((bulk!(REPLICATION_ID), bulk!("0"))).to_command(),
        RESPType::RDBFile(rdb),
    ]))
}
