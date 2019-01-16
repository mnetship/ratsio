#![allow(clippy::redundant_closure)]

use nom::{AsChar, ErrorKind, InputTakeAtPosition, IResult,
          FindToken, recognize_float};
use std::collections::HashMap;
use std::convert::From;

use crate::ops::*;

#[allow(dead_code, clippy::needless_pass_by_value)]
fn whitespace<'a, T>(input: T) -> IResult<T, T>
where
  T: InputTakeAtPosition,
  <T as InputTakeAtPosition>::Item: AsChar + Clone,
  &'a str: FindToken<<T as InputTakeAtPosition>::Item>,
{
  input.split_at_position(|item| {
    let c = item.clone().as_char();
    !(c == ' ' || c == '\t' || c == '\r' || c == '\n')
  })
  //this could be written as followed, but not using FindToken is faster
  //eat_separator!(input, " \t\r\n")
}

macro_rules! leading_ws (
  ($i:expr, $($args:tt)*) => (
    {
      match sep!($i, whitespace, $($args)*) {
        Err(e) => Err(e),
        Ok((i1,o))    => Ok((i1, o))
      }
    }
  )
);

#[allow(dead_code, clippy::needless_pass_by_value)]
fn space<'a, T>(input: T) -> IResult<T, T>
where
  T: InputTakeAtPosition,
  <T as InputTakeAtPosition>::Item: AsChar + Clone,
  &'a str: FindToken<<T as InputTakeAtPosition>::Item>,
{
  input.split_at_position(|item| {
    let c = item.clone().as_char();
    !(c == ' ' || c == '\t')
  })
  //this could be written as followed, but not using FindToken is faster
  //eat_separator!(input, " \t")
}

macro_rules! leading_space (
  ($i:expr, $($args:tt)*) => (
    {
      match sep!($i, space, $($args)*) {
        Err(e) => Err(e),
        Ok((i1,o))    => Ok((i1, o))
      }
    }
  )
);

named!(float<f32>, leading_ws!(flat_map!(recognize_float, parse_to!(f32))));

named!(boolean<bool>, leading_ws!(alt!(
      tag!("true")  => {|_| true}
    | tag!("false") => {|_| false }
  ))
);

#[allow(dead_code, clippy::needless_pass_by_value)]
fn to_str(i: Vec<u8>) -> String {
    String::from_utf8_lossy(&i).into_owned()
}

#[allow(dead_code, clippy::needless_pass_by_value)]
fn allowed_char_in_key<T>(input: T) -> IResult<T, T, u32>
    where
        T: InputTakeAtPosition,
        <T as InputTakeAtPosition>::Item: AsChar,
{
    input.split_at_position1(|item| {
        let c = item.as_char();
        c == '\\' || c == '\"'
    }, ErrorKind::TakeTill1)
}

named!(
  string<String>,
  delimited!(leading_ws!(tag!("\"")),
    map!(
      escaped_transform!(call!(allowed_char_in_key), '\\',
        alt!(
           tag!("\\")       => { |_| &b"\\"[..] }
         | tag!("\"")       => { |_| &b"\""[..] }
         | tag!("b")        => { |_| &[0x08][..]}
         | tag!("f")        => { |_| &[0x0C][..]}
         | tag!("t")        => { |_| &b"\t"[..] }
         | tag!("r")        => { |_| &b"\r"[..] }
         | tag!("n")        => { |_| &b"\n"[..] }
       )
      ),
    to_str),
    tag!("\"")
  )
);

named!(
  array<Vec<JsonValue>>,
  delimited!(
    leading_ws!(tag!("[")),
    separated_list!(leading_ws!(tag!(",")), value),
    leading_ws!(tag!("]"))
  )
);

named!(
  key_value<(String, JsonValue)>,
  separated_pair!(leading_ws!(string), leading_ws!(tag!(":")), leading_ws!(value))
);

named!(
  object<HashMap<String, JsonValue>>,
  map!(
    delimited!(leading_ws!(tag!("{")), separated_list!(leading_ws!(tag!(",")), key_value), leading_ws!(tag!("}"))),
    |tuple_vec| {
      let mut h: HashMap<String, JsonValue> = HashMap::new();
      for (k, v) in tuple_vec {
        h.insert(k, v);
      }
      h
    }
  )
);

named!(
  pub value<JsonValue>,
  leading_ws!(alt!(
        object  => { |h|   JsonValue::Object(h)   }
      | array   => { |v|   JsonValue::Array(v)    }
      | string  => { |s|   JsonValue::String(s)   }
      | boolean => { |b|   JsonValue::Boolean(b)  }
      | float   => { |num| JsonValue::Number(num) }
    ))
);


#[allow(dead_code)]
fn is_space(ch: u8) -> bool {
    match ch {
        b'\t' | b' ' => true,
        _ => false,
    }
}

#[allow(dead_code)]
fn is_newline(ch: u8) -> bool {
    match ch {
        | b'\r' | b'\n' => true,
        _ => false,
    }
}

#[allow(dead_code, clippy::needless_pass_by_value)]
fn allowed_char_in_err_msg<T>(input: T) -> IResult<T, T, u32>
    where
        T: InputTakeAtPosition,
        <T as InputTakeAtPosition>::Item: AsChar,
{
    input.split_at_position1(|item| {
        let c = item.as_char();
        c == '\\' || c == '\''
    }, ErrorKind::TakeTill1)
}

named!(
  error_msg<String>,
  leading_ws!(delimited!(tag!("'"),
    map!(
      escaped_transform!(call!(allowed_char_in_err_msg), '\\',
        alt!(
           tag!("\\")       => { |_| &b"\\"[..] }
         | tag!("\"")       => { |_| &b"\""[..] }
         | tag!("b")        => { |_| &[0x08][..]}
         | tag!("f")        => { |_| &[0x0C][..]}
         | tag!("t")        => { |_| &b"\t"[..] }
         | tag!("r")        => { |_| &b"\r"[..] }
         | tag!("n")        => { |_| &b"\n"[..] }
       )
      ),
      to_str),
    tag!("'")))
);


named!(text_token<String>, map!(take_till!(|x| {is_space(x) || is_newline(x)}),
    |s| String::from_utf8(Vec::from(s)).unwrap().to_owned()));


//MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n
named!(message<Message>, do_parse!(
    item: map!(delimited!(leading_ws!(take_while!(is_space)), separated_list!(take_while!(is_space), text_token),
        leading_space!(tag!("\r\n"))), |tokens| {
            let token_len = tokens.len();
            let size: usize =  tokens[token_len-1].parse().unwrap();
            let subject = if token_len > 1 { tokens[0].to_owned() } else { String::from("") };
            let sid =  if token_len > 2 { tokens[1].to_owned() } else {String::from("") };
            let reply_to =  if token_len > 3 { Some(tokens[2].to_owned()) } else { None };
            (size, Message{
                subject, sid, reply_to, payload: Vec::new(),
            })
      })                  >>
    payload: take!(item.0) >>
    tag!("\r\n")          >>
    ( Message{
       subject: item.1.subject,
       sid: item.1.sid,
       reply_to: item.1.reply_to,
       payload: Vec::from(payload),
    })
));


//PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n
named!(publish<Publish>, do_parse!(
    item: map!(delimited!(leading_ws!(take_while!(is_space)), separated_list!(take_while!(is_space), text_token),
      leading_space!(tag!("\r\n"))), |tokens| {
        let token_len = tokens.len();
        let size: usize =  tokens[token_len-1].parse().unwrap();
        let subject = if token_len > 1 { tokens[0].to_owned() } else { String::from("") };
        let reply_to =  if token_len > 2 { Some(tokens[1].to_owned()) } else { None };
       (size, Publish{
            subject, reply_to, payload: Vec::new(),
        })
      })                      >>
    payload: take!(item.0) >>
    tag!("\r\n")              >>
    (Publish{
        subject: item.1.subject,
        reply_to: item.1.reply_to,
        payload: Vec::from(payload),
    })
));


//SUB <subject> [queue group] <sid>\r\n
named!(subscribe<Subscribe>,
   map!(delimited!(take_while!(is_space), separated_list!(take_while!(is_space), text_token),
      leading_space!(tag!("\r\n"))), |tokens| {
        let token_len = tokens.len();
        let sid = tokens[token_len-1].to_owned();
        let subject = if token_len > 1 { tokens[0].to_owned() } else { String::from("") };
        let queue_group =  if token_len > 2 { Some(tokens[1].to_owned()) } else { None };
        Subscribe{
            subject, sid, queue_group,
        }
   })
);


//UNSUB <sid> [max_msgs]
named!(un_subscribe<UnSubscribe>,
   map!(delimited!(take_while!(is_space), separated_list!(take_while!(is_space), text_token),
      leading_space!(tag!("\r\n"))), |tokens| {
        let token_len = tokens.len();
        let sid = tokens[0].to_owned();
        let max_msgs: Option<u32> = if token_len > 1 {
          match tokens[1].parse() {
            Ok(size) => Some(size),
            _ => None,
          }
        }else{
            None
        };

        UnSubscribe{
            sid, max_msgs,
        }
   })
);


named!(
    pub operation<Op>,
    alt!(
          pair!(leading_ws!(tag_no_case!("MSG")), message) => { |(_, msg)| Op::MSG(msg) }
        | tuple!(leading_ws!(tag_no_case!("INFO")), object, leading_space!(tag!("\r\n"))) => { |(_, json_obj, _)|
            Op::INFO(ServerInfo::from(JsonValue::Object(json_obj)))}
        | tuple!(leading_ws!(tag_no_case!("CONNECT")), object, leading_space!(tag!("\r\n"))) => { |(_, json_obj, _)|
            Op::CONNECT(Connect::from(JsonValue::Object(json_obj)))}
        | pair!(leading_ws!(tag_no_case!("+OK")), leading_space!(tag!("\r\n"))) => { |(_,_)| Op::OK }
        | pair!(leading_ws!(tag_no_case!("PING")), leading_space!(tag!("\r\n"))) => { |(_,_)| Op::PING }
        | pair!(leading_ws!(tag_no_case!("PONG")), leading_space!(tag!("\r\n"))) => { |(_,_)| Op::PONG }
        | tuple!(leading_ws!(tag_no_case!("-ERR")), error_msg, leading_space!(tag!("\r\n"))) => { |(_, msg, _): (_, String, _)|
           Op::ERR(msg) }
        | pair!(leading_ws!(tag_no_case!("PUB")), publish) => { |(_, publish)| Op::PUB(publish) }
        | pair!(leading_ws!(tag_no_case!("SUB")), subscribe) => { |(_, sub)| Op::SUB(sub) }
        | pair!(leading_ws!(tag_no_case!("UNSUB")), un_subscribe) => { |(_, un_sub)| Op::UNSUB(un_sub) }
        //Un parsed data.
    )
);