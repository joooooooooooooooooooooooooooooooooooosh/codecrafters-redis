use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::types::{Bulk, ARRAY, BULK, CRLF, ERROR, INTEGER, NIL_BULK, STRING};

#[derive(Debug)]
pub enum RESPType {
    Integer(isize),
    String(String),
    Bulk(Option<Bulk>),
    Array(Vec<Self>),
    Error(String),
    RDBFile(Vec<u8>),
    Multi(Vec<Self>),
}

impl RESPType {
    pub fn as_bytes(self) -> BytesMut {
        match self {
            RESPType::Integer(_) => todo!(),
            RESPType::String(s) => Self::ser_string(s),
            RESPType::Bulk(b) => Self::ser_bulk(b),
            RESPType::Array(a) => Self::ser_array(a),
            RESPType::Error(_) => todo!(),
            RESPType::RDBFile(f) => Self::ser_rdb_file(f),
            RESPType::Multi(m) => Self::ser_multi(m),
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
            .for_each(|val| resp.put_slice(&val.as_bytes()));

        resp
    }

    fn ser_rdb_file(f: Vec<u8>) -> BytesMut {
        let mut resp = BytesMut::with_capacity(f.len() + 10);

        resp.put_u8(BULK);
        resp.put_slice(&f.len().to_string().into_bytes());
        resp.put_slice(CRLF);
        resp.put_slice(f.as_slice());

        dbg!(resp)
    }

    fn ser_multi(m: Vec<Self>) -> BytesMut {
        let mut resp = BytesMut::new();

        m.into_iter()
            .for_each(|val| resp.put_slice(&val.as_bytes()));

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

    pub fn parse_uinteger(buf: &mut Bytes) -> Result<usize> {
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
        // TODO: allow parsing RDB file (no ending CRLF)
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
