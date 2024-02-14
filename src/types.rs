use std::{
    char,
    collections::HashMap,
    env::args,
    ops::Add,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::Mutex;

pub type Db = Arc<Mutex<HashMap<Bulk, Entry>>>;

static CRLF: &[u8; 2] = b"\r\n";
static NIL_BULK: &[u8; 4] = b"-1\r\n";

const INTEGER: u8 = b':';
const STRING: u8 = b'+';
const BULK: u8 = b'$';
const ARRAY: u8 = b'*';
const ERROR: u8 = b'-';

pub struct _Args {
    pub port: String,
    pub replica_of: Option<(String, String)>,
}
pub type Args = Arc<_Args>;

pub fn parse_args() -> Args {
    let port = args()
        .skip_while(|arg| arg != "--port")
        .skip(1)
        .next()
        .unwrap_or(String::from("6379"));

    let mut replica_iter = args().skip_while(|arg| arg != "--replicaof").skip(1);
    let replica_of = if let Some(master_host) = replica_iter.next() {
        let master_port = replica_iter.next().expect("host must have port");
        Some((master_host, master_port))
    } else {
        None
    };

    Arc::new(_Args { port, replica_of })
}

#[derive(Debug)]
pub enum RESPType {
    Integer(isize),
    String(String),
    Bulk(Option<Bulk>),
    Array(Vec<RESPType>),
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Bulk {
    len: usize,
    data: Bytes,
}

impl Bulk {
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }

    pub fn from(from: &str) -> Self {
        Self {
            len: from.len(),
            data: Bytes::copy_from_slice(from.as_bytes()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub val: Bulk,
    pub timeout: Option<SystemTime>,
}

impl RESPType {
    pub fn to_bytes(self) -> BytesMut {
        match self {
            RESPType::Integer(_) => todo!(),
            RESPType::String(s) => Self::ser_string(s),
            RESPType::Bulk(b) => Self::ser_bulk(b),
            RESPType::Array(a) => Self::ser_array(a),
            RESPType::Error(_) => todo!(),
        }
    }

    fn ser_string(s: String) -> BytesMut {
        let mut resp = BytesMut::with_capacity(s.len());

        resp.put_u8(STRING);
        resp.put_slice(&s.into_bytes());
        resp.put_slice(CRLF);

        resp
    }

    fn ser_bulk(b: Option<Bulk>) -> BytesMut {
        let mut resp = BytesMut::with_capacity(b.as_ref().map_or(5, |b| b.len));

        resp.put_u8(BULK);
        if let Some(b) = b {
            resp.put_slice(&b.len.to_string().into_bytes());
            resp.put_slice(CRLF);
            resp.extend_from_slice(&b.data);
        } else {
            resp.put_slice(b"-1");
        }
        resp.put_slice(CRLF);

        resp
    }

    fn ser_array(a: Vec<Self>) -> BytesMut {
        let mut resp = BytesMut::new();

        resp.put_u8(ARRAY);
        resp.put_slice(&a.len().to_string().into_bytes());
        resp.put_slice(CRLF);
        a.into_iter()
            .for_each(|val| resp.put_slice(&val.to_bytes()));

        resp
    }
}

impl RESPType {
    pub fn parse(buf: &mut Bytes) -> Result<Self> {
        let typ = buf.get_u8();
        Ok(match typ {
            INTEGER => Self::Integer(Self::parse_integer(buf)?),
            STRING => Self::String(Self::parse_string(buf)?),
            BULK => Self::Bulk(Self::parse_bulk(buf)?),
            ARRAY => Self::Array(Self::parse_array(buf)?),
            ERROR => Self::Error(Self::parse_error(buf)?),
            _ => bail!("Invalid type marker"),
        })
    }

    fn parse_integer(_buf: &mut Bytes) -> Result<isize> {
        todo!()
    }

    fn parse_uinteger(buf: &mut Bytes) -> Result<usize> {
        let mut total: usize = 0;

        while !buf.is_empty() && buf[0].is_ascii_digit() {
            total *= 10;
            total += buf.get_u8() as usize - 48
        }

        Ok(total)
    }

    fn parse_array(buf: &mut Bytes) -> Result<Vec<Self>> {
        let len = Self::parse_uinteger(buf)?;
        Self::parse_crlf(buf)?;

        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(Self::parse(buf)?);
        }

        Ok(vec)
    }

    fn parse_string(buf: &mut Bytes) -> Result<String> {
        let mut s = String::new();

        while buf[0] != b'\r' {
            s.push(buf.get_u8() as char)
        }
        Self::parse_crlf(buf)?;

        Ok(s)
    }

    fn parse_bulk(buf: &mut Bytes) -> Result<Option<Bulk>> {
        if &buf[..4] == NIL_BULK {
            return Ok(None);
        }

        let len = Self::parse_uinteger(buf)?;
        Self::parse_crlf(buf)?;

        let data = buf.split_to(len);
        Self::parse_crlf(buf)?;

        Ok(Some(Bulk { len, data }))
    }

    fn parse_error(_buf: &mut Bytes) -> Result<String> {
        todo!()
    }

    fn parse_crlf(buf: &mut Bytes) -> Result<()> {
        let crlf = buf.get_u16();
        if crlf != u16::from_be_bytes(*b"\r\n") {
            bail!("Missing CRLF")
        }

        Ok(())
    }
}

pub enum Conf {
    ListeningPort,
    Capa,
}

pub enum RESPCmd {
    Echo(Bulk),
    Ping,
    Get(Bulk),
    Set((Bulk, Bulk, Option<SystemTime>)),
    Info(Option<Bulk>),
    ReplConf((Conf, Bulk)),
    Psync((Bulk, Bulk)),
}

impl RESPCmd {
    pub fn to_command(self) -> RESPType {
        match self {
            RESPCmd::Echo(_) => todo!(),
            RESPCmd::Ping => RESPType::Array(vec![RESPType::Bulk(Some(Bulk::from("PING")))]),
            RESPCmd::Get(_) => todo!(),
            RESPCmd::Set(_) => todo!(),
            RESPCmd::Info(_) => todo!(),
            RESPCmd::ReplConf((conf, bulk)) => Self::handle_replconf(conf, bulk),
            RESPCmd::Psync((id, offset)) => Self::handle_psync(id, offset),
        }
    }

    fn handle_replconf(conf: Conf, bulk: Bulk) -> RESPType {
        RESPType::Array(vec![
            RESPType::Bulk(Some(Bulk::from("REPLCONF"))),
            RESPType::Bulk(Some(Bulk::from(match conf {
                Conf::ListeningPort => "listening-port",
                Conf::Capa => "capa",
            }))),
            RESPType::Bulk(Some(bulk)),
        ])
    }

    fn handle_psync(id: Bulk, offset: Bulk) -> RESPType {
        RESPType::Array(vec![
            RESPType::Bulk(Some(Bulk::from("PSYNC"))),
            RESPType::Bulk(Some(id)),
            RESPType::Bulk(Some(offset)),
        ])
    }
}

impl RESPCmd {
    pub fn parse(cmd: RESPType) -> Result<Self> {
        let RESPType::Array(cmd) = cmd else {
            bail!("Top level command must be array");
        };

        let mut parts = cmd.into_iter();
        let Some(RESPType::Bulk(Some(cmd))) = parts.next() else {
            bail!("Command must be non-null bulk string");
        };

        Ok(match cmd.data.to_ascii_uppercase().as_slice() {
            b"PING" => Self::Ping,
            b"ECHO" => Self::Echo(Self::parse_echo(parts)?),
            b"SET" => Self::Set(Self::parse_set(parts)?),
            b"GET" => Self::Get(Self::parse_get(parts)?),
            b"INFO" => Self::Info(Self::parse_info(parts)?),
            b"REPLCONF" => Self::ReplConf(Self::parse_replconf(parts)?),
            _ => Self::Ping, // try not to crash
        })
    }

    fn parse_echo(mut parts: impl Iterator<Item = RESPType>) -> Result<Bulk> {
        let Some(RESPType::Bulk(Some(echo))) = parts.next() else {
            bail!("Echo must take a non-null bulk string");
        };

        Ok(echo)
    }

    fn parse_set(
        mut parts: impl Iterator<Item = RESPType>,
    ) -> Result<(Bulk, Bulk, Option<SystemTime>)> {
        let Some(RESPType::Bulk(Some(key))) = parts.next() else {
            bail!("Set requires a key");
        };

        let Some(RESPType::Bulk(Some(val))) = parts.next() else {
            bail!("Set requires a value");
        };

        let mut timeout = None;
        if let Some(RESPType::Bulk(Some(bulk))) = parts.next() {
            if bulk.data.as_ref().to_ascii_uppercase() == b"PX" {
                let Some(RESPType::Bulk(Some(mut i))) = parts.next() else {
                    bail!("PX requires an expiry");
                };

                let i = RESPType::parse_uinteger(&mut i.data)?;
                timeout = Some(SystemTime::now().add(Duration::from_millis(i as u64)))
            }
        }

        Ok((key, val, timeout))
    }

    fn parse_get(mut parts: impl Iterator<Item = RESPType>) -> Result<Bulk> {
        let Some(RESPType::Bulk(Some(key))) = parts.next() else {
            bail!("Set requires a key");
        };

        Ok(key)
    }

    fn parse_info(mut parts: impl Iterator<Item = RESPType>) -> Result<Option<Bulk>> {
        Ok(if let Some(RESPType::Bulk(Some(topic))) = parts.next() {
            Some(topic)
        } else {
            None
        })
    }

    fn parse_replconf(mut parts: impl Iterator<Item = RESPType>) -> Result<(Conf, Bulk)> {
        let Some(RESPType::Bulk(Some(conf))) = parts.next() else {
            bail!("Missing replconf arguments");
        };

        let Some(RESPType::Bulk(Some(state))) = parts.next() else {
            bail!("Missing replconf arguments");
        };

        let conf = match conf.data.to_ascii_lowercase().as_slice() {
            b"listening-port" => Conf::ListeningPort,
            b"capa" => Conf::Capa,
            _ => bail!("Invalid replconf argument"),
        };

        Ok((conf, state))
    }
}
