use crate::error::RatsioError;
use crate::protocol::parser::operation;
use crate::ops::*;

use tokio::codec::{Decoder, Encoder};

use bytes::{BytesMut, BufMut};
use nom::{Err as NomErr};

#[derive(Debug, Default, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct OpCodec {
}

impl Decoder for OpCodec {
    type Item = Op;
    type Error = RatsioError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let len = buf.len();
        if len == 0{
            return Ok(None);
        }
        match operation(&buf[..]) {
            Err(NomErr::Incomplete(_)) => Ok(None),
            Ok((remaining, item)) => {
                debug!(target: "ratsio", " Op::Item => {:?}", item);
                buf.split_to(len - remaining.len());
                Ok(Some(item))
            },
            Err(NomErr::Error(err)) => {
                //scan for \r\n and recover there.
                let txt = String::from(&(*String::from_utf8_lossy(&buf[..])));
                error!(target: "ratsio", " Error parsing => {:?}\n{}", err, txt);
                if let Some(offset) = buf[..].windows(2).position(|w| w == b"\r\n") {
                    buf.split_to(offset);
                    self.decode(buf)
                }else{
                    buf.split_to(len);
                    Ok(None)
                }
            },
            Err(NomErr::Failure(err)) => {
                //scan for \r\n and recover there.
                let txt = String::from(&(*String::from_utf8_lossy(&buf[..])));
                error!(target: "ratsio", " Failure parsing => {:?}\n{}", err, txt);
                if let Some(offset) = buf[..].windows(2).position(|w| w == b"\r\n") {
                    buf.split_to(offset);
                    self.decode(buf)
                }else{
                    buf.split_to(len);
                    Ok(None)
                }
            },
        }
    }
}

impl Encoder for OpCodec {
    type Item = Op;
    type Error = RatsioError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let buf = item.into_bytes()?;
        let buf_len = buf.len();
        let remaining_bytes = dst.remaining_mut();
        if remaining_bytes < buf_len {
            dst.reserve(buf_len);
        }
        debug!(" Sending --->\n{}",  String::from(&(*String::from_utf8_lossy(&buf[..]))));
        dst.put(&buf[..]);
        Ok(())
    }
}
