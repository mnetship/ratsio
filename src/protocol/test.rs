use crate::ops::*;
use std::collections::HashMap;
use super::parser::*;

////////////////////////// TESTS ////////////////////////////

#[test]
fn parse_pub() {
    let input: &[u8] = b"PUB FRONT.DOOR INBOX.22 11\r\nKnock Knock\r\n";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::PUB(Publish {
                subject: String::from("FRONT.DOOR"),
                reply_to: Some(String::from("INBOX.22")),
                payload: Vec::from(b"Knock Knock" as &[u8]),
            }))),
        Err(err) => {
            println!(" parse_pub ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}


#[test]
fn parse_pub_no_reply() {
    let input: &[u8] = b"PUB FRONT.DOOR 11\r\nKnock Knock\r\n";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::PUB(Publish {
                subject: String::from("FRONT.DOOR"),
                reply_to: None,
                payload: Vec::from(b"Knock Knock" as &[u8]),
            }))),
        Err(err) => {
            println!(" parse_pub_no_reply ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}


#[test]
fn parse_sub() {
    let input: &[u8] = b"SUB BAR G1 44\r\n";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::SUB(Subscribe {
                subject: String::from("BAR"),
                sid: String::from("44"),
                queue_group: Some(String::from("G1")),
            }))),
        Err(err) => {
            println!(" parse_sub ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}


#[test]
fn parse_sub_no_group() {
    let input: &[u8] = b"SUB FOO 1\r\n";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::SUB(Subscribe {
                subject: String::from("FOO"),
                sid: String::from("1"),
                queue_group: None,
            }))),
        Err(err) => {
            println!(" parse_sub_no_group ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}


#[test]
fn parse_un_sub() {
    let input: &[u8] = b"UNSUB 4\r\n";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::UNSUB(UnSubscribe {
                sid: String::from("4"),
                max_msgs: None,
            }))),
        Err(err) => {
            println!(" parse_sub ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}


#[test]
fn parse_un_sub_no_max() {
    let input: &[u8] = b"UNSUB 1 5\r\n";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::UNSUB(UnSubscribe {
                sid: String::from("1"),
                max_msgs: Some(5),
            }))),
        Err(err) => {
            println!(" parse_sub_no_group ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}

#[test]
fn parse_message() {
    let input: &[u8] = b"MSG FOO.BAR 9 INBOX.34 11\r\nHello World\r\n\0";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"\0" as &[u8], Op::MSG(Message {
                subject: String::from("FOO.BAR"),
                sid: String::from("9"),
                reply_to: Some(String::from("INBOX.34")),
                payload: Vec::from(b"Hello World" as &[u8]),
            }))),
        Err(err) => {
            println!(" parse_message ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}

#[test]
fn parse_message_no_reply() {
    let input: &[u8] = b"MSG FOO.BAR 9  20\r\nHello World No Reply\r\n\0";
    match operation(input) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"\0" as &[u8], Op::MSG(Message {
                subject: String::from("FOO.BAR"),
                sid: String::from("9"),
                reply_to: None,
                payload: Vec::from(b"Hello World No Reply" as &[u8]),
            }))),
        Err(err) => {
            println!(" parse_message ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}

#[test]
fn parse_obj1() {
    let test = b"  { \"v\": 42,
  \"b\": \"x\"
  }\0";
    let x: HashMap<_, _> = vec![(String::from("b"), JsonValue::String(String::from("x"))),
                                (String::from("v"), JsonValue::Number(42.0))].into_iter().collect();
    assert_eq!(value(&test[..]).unwrap(), (b"\0" as &[u8], JsonValue::Object(x)));
}


#[test]
fn parse_info_operation() {
    let input = format!("{}\r\n", r#"INFO {
        "server_id":"ad29ea9cbb16f2865c177bbd4db446ca",
        "version":"0.6.8","go":"go1.5.1",
        "host":"0.0.0.0","port":4222,
        "auth_required":false,
        "ssl_required":false,
        "max_payload":1048576}"#);
    match operation(&input.as_bytes()) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::INFO(ServerInfo {
                server_id: String::from("ad29ea9cbb16f2865c177bbd4db446ca"),
                version: String::from("0.6.8"),
                go: String::from("go1.5.1"),
                host: String::from("0.0.0.0"),
                port: 4222,
                max_payload: 1048576,
                proto: 0,
                client_id: 0,
                auth_required: false,
                tls_required: false,
                tls_verify: false,
                connect_urls: Vec::new(),
            }))),
        Err(err) => {
            println!(" parse_info_operation ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}


#[test]
fn parse_connect_operation() {
    let input = format!("{}\r\n", r#"CONNECT {
        "verbose":false,"pedantic":false,
        "tls_required":false,"name":"",
        "lang":"go","version"   :"1.2.2"   ,    "protocol":   1}     "#);
    match operation(&input.as_bytes()) {
        Ok(obj) =>
        //println!(" parse_info_operation >>>>>>>>>>>>>> {:?}", obj),
            assert_eq!(obj, (b"" as &[u8], Op::CONNECT(Connect {
                verbose: false,
                pedantic: false,
                version: String::from("1.2.2"),
                protocol: 1,
                lang: String::from("go"),
                name: Some(String::from("")),
                tls_required: false,
                user: None,
                pass: None,
                auth_token: None,
                echo: true,
            }))),
        Err(err) => {
            println!(" parse_info_operation ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
}


#[test]
fn parse_err_operation() {
    let input = format!("{}\r\n", r#"-ERR 'Unknown Protocol Operation'       "#);
    match operation(&input.as_bytes()) {
        Ok(obj) => assert_eq!(obj, (b"" as &[u8], Op::ERR(String::from("Unknown Protocol Operation")))),
        Err(err) => {
            println!(" parse_err_operation ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
    //assert!(false);
}

#[test]
fn parse_ok_operation() {
    let input = format!("{}\r\n", r#"+OK    "#);

    match operation(&input.as_bytes()) {
        Ok(obj) => assert_eq!(obj, (b"" as &[u8], Op::OK)),
        Err(err) => {
            println!(" parse_ok_operation ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
    //assert!(false);
}

#[test]
fn parse_ping_operation() {
    let input = format!("{}\r\n", r#"  PING  "#);

    match operation(&input.as_bytes()) {
        Ok(obj) => assert_eq!(obj, (b"" as &[u8], Op::PING)),
        Err(err) => {
            println!(" parse_ping_operation ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
    //assert!(false);
}

#[test]
fn parse_pong_operation() {
    let input = format!("{}\r\n", r#"PONG"#);

    match operation(&input.as_bytes()) {
        Ok(obj) => assert_eq!(obj, (b"" as &[u8], Op::PONG)),
        Err(err) => {
            println!(" parse_pong_operation ~~~~~~~~~ ERROR {:?}", err);
            assert!(false)
        }
    }
    //assert!(false);
}
