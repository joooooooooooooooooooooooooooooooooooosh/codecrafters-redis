use std::{collections::HashMap, env::args, fmt::Display, io::Read, sync::Arc, time::SystemTime};

use bytes::Bytes;
use tokio::sync::Mutex;

pub type Db = Arc<Mutex<HashMap<Bulk, Entry>>>;

pub static CRLF: &[u8; 2] = b"\r\n";
pub static NIL_BULK: &[u8; 4] = b"-1\r\n";

pub const INTEGER: u8 = b':';
pub const STRING: u8 = b'+';
pub const BULK: u8 = b'$';
pub const ARRAY: u8 = b'*';
pub const ERROR: u8 = b'-';

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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Bulk {
    pub len: usize,
    pub data: Bytes,
}

#[macro_export]
macro_rules! bulk {
    ($thing:expr) => {
        Bulk::from($thing)
    };
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

impl Display for Bulk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::with_capacity(self.len);
        let _ = self.data.as_ref().read_to_string(&mut s);
        f.write_str(s.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub val: Bulk,
    pub timeout: Option<SystemTime>,
}
