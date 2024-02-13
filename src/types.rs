use anyhow::{bail, Result};
use bytes::{Buf, BytesMut};

#[derive(Debug)]
pub enum RESPType {
    Integer(isize),
    String(String),
    Bulk(Option<Bulk>),
    Array(Vec<RESPType>),
    Error(String),
}

#[derive(Debug)]
pub struct Bulk {
    _len: usize,
    _data: String,
}

impl RESPType {
    pub fn parse(buf: &mut BytesMut) -> Result<Self> {
        let typ = buf.get_u8();
        Ok(match typ {
            b':' => Self::Integer(Self::parse_integer(buf)?),
            b'+' => Self::String(Self::parse_string(buf)?),
            b'$' => Self::Bulk(Self::parse_bulk(buf)?),
            b'*' => Self::Array(Self::parse_array(buf)?),
            b'-' => Self::Error(Self::parse_error(buf)?),
            _ => bail!("Invalid type marker"),
        })
    }

    fn parse_integer(_buf: &mut BytesMut) -> Result<isize> {
        todo!()
    }

    fn parse_uinteger(buf: &mut BytesMut) -> Result<usize> {
        Ok(buf.get_u8().to_string().parse()?)
    }

    fn parse_array(buf: &mut BytesMut) -> Result<Vec<Self>> {
        let len = Self::parse_uinteger(buf)?;
        Self::parse_crlf(buf)?;

        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(Self::parse(buf)?);
        }

        Ok(vec)
    }

    fn parse_string(buf: &mut BytesMut) -> Result<String> {
        let mut s = String::new();

        while buf[0] != b'\r' {
            s.push(buf.get_u8() as char)
        }
        Self::parse_crlf(buf)?;

        Ok(s)
    }

    fn parse_bulk(buf: &mut BytesMut) -> Result<Option<Bulk>> {
        todo!()
    }

    fn parse_error(buf: &mut BytesMut) -> Result<String> {
        todo!()
    }

    fn parse_crlf(buf: &mut BytesMut) -> Result<()> {
        let crlf = buf.get_u16();
        if crlf != u16::from_be_bytes(*b"\r\n") {
            bail!("Missing CRLF")
        }

        Ok(())
    }
}

pub enum RESPCmd {
    Echo,
    Ping,
}
