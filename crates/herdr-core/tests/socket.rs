#![cfg(unix)]

mod common;

use common::framed::{Framed, Viewer};
use common::*;
use herdr_core::wire;
use serde_json::json;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(2);

/// Opens a socket on a fresh connection; returns the connection and the channel.
fn open(s: &Session, address: &str) -> (Framed, u32) {
    let mut conn = Framed::open(s);
    let reply = conn.call("socket.open", json!({"address": address}));
    let channel = reply["result"]["channel"].as_u64().unwrap_or_else(|| panic!("{reply}")) as u32;
    (conn, channel)
}

/// The next DATA frame on `channel` within `WAIT`.
fn data(conn: &mut Framed, channel: u32) -> Vec<u8> {
    loop {
        let (ch, kind, payload) = conn.read(WAIT).expect("a frame");
        if ch == channel && kind == wire::DATA {
            return payload;
        }
    }
}

fn echo<S: Read + Write + Send + 'static>(mut stream: S) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n @ 1..) = stream.read(&mut buf) {
            if stream.write_all(&buf[..n]).is_err() {
                break;
            }
        }
    });
}

#[test]
fn echoes_through_a_local_socket() {
    let s = Session::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || echo(listener.accept().unwrap().0));
    let (mut conn, channel) = open(&s, &format!("127.0.0.1:{port}"));
    conn.send(channel, wire::DATA, b"ping");
    assert_eq!(data(&mut conn, channel), b"ping");
}

#[test]
fn refuses_non_loopback() {
    let s = Session::new();
    let mut conn = Framed::open(&s);
    for address in ["10.0.0.1:80", "example.com:80"] {
        let reply = conn.call("socket.open", json!({"address": address}));
        assert_eq!(reply["error"]["data"]["kind"], "not_allowed", "{reply}");
    }
}

#[test]
fn refused_when_nothing_listens() {
    let s = Session::new();
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut conn = Framed::open(&s);
    let reply = conn.call("socket.open", json!({"address": format!("127.0.0.1:{port}")}));
    assert_eq!(reply["error"]["data"]["kind"], "refused", "{reply}");
}

#[test]
fn needs_framed_without_hello() {
    let s = Session::new();
    s.json(&["server", "start"]);
    let reply = s.connect().call(1, "socket.open", json!({"address": "127.0.0.1:1"}));
    assert_eq!(reply["error"]["data"]["kind"], "needs_framed", "{reply}");
}

#[test]
fn closing_channel_closes_socket() {
    let s = Session::new();
    let t = run(&s, &["sh", "-c", "while read l; do echo got-$l; done"]);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (done, ended) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut peer = listener.accept().unwrap().0;
        let mut buf = [0u8; 16];
        while peer.read(&mut buf).unwrap_or(0) > 0 {}
        done.send(()).unwrap();
    });
    let mut viewer = Viewer::attach(&s, &t, 80, 24, json!({}));
    let reply = viewer.conn.call("socket.open", json!({"address": format!("127.0.0.1:{port}")}));
    let channel = reply["result"]["channel"].as_u64().unwrap() as u32;
    viewer.conn.send(channel, wire::CLOSE, b"");
    ended.recv_timeout(WAIT).expect("the socket closed");
    viewer.typed(b"hello\n");
    viewer.shows("got-hello");
}

#[test]
fn unix_socket_path_works() {
    let s = Session::new();
    let path = s.root.join("echo.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    std::thread::spawn(move || echo(listener.accept().unwrap().0));
    let (mut conn, channel) = open(&s, path.to_str().unwrap());
    conn.send(channel, wire::DATA, b"ping");
    assert_eq!(data(&mut conn, channel), b"ping");
}

#[test]
fn a_closing_peer_ends_the_channel() {
    let s = Session::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || drop(listener.accept().unwrap().0));
    let (mut conn, channel) = open(&s, &format!("127.0.0.1:{port}"));
    let (ch, kind, payload) = conn.read(WAIT).expect("a frame");
    assert_eq!((ch, kind, payload.as_slice()), (channel, wire::CLOSE, &b""[..]));
}
