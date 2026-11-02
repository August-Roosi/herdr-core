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

#[test]
fn bidirectional_bulk_does_not_hang() {
    const TOTAL: usize = 8 << 20;
    let s = Session::new();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    // Echoes while reading: the peer blocks writing if we stop reading.
    std::thread::spawn(move || {
        let mut read = listener.accept().unwrap().0;
        let mut write = read.try_clone().unwrap();
        echo_split(&mut read, &mut write);
    });
    let (mut conn, channel) = open(&s, &format!("127.0.0.1:{port}"));
    // Under the core's queue bound (1024 frames): 512 frames of 16 KiB.
    for _ in 0..TOTAL / (16 << 10) {
        conn.send(channel, wire::DATA, &[7u8; 16 << 10]);
    }
    let mut got = 0;
    let start = std::time::Instant::now();
    while got < TOTAL {
        assert!(start.elapsed() < Duration::from_secs(30), "stuck after {got} bytes");
        if let Some((ch, wire::DATA, payload)) = conn.read(Duration::from_millis(200))
            && ch == channel
        {
            got += payload.len();
            conn.send(channel, wire::CREDIT, &(payload.len() as u32).to_le_bytes());
        }
    }
    assert_eq!(got, TOTAL);
}

fn echo_split(read: &mut std::net::TcpStream, write: &mut std::net::TcpStream) {
    let mut buf = [0u8; 16 << 10];
    while let Ok(n @ 1..) = read.read(&mut buf) {
        if write.write_all(&buf[..n]).is_err() {
            break;
        }
    }
}

/// A session with the machine `rohan` added (a second core on this host).
fn with_rohan() -> (Session, Session) {
    let rohan = Session::new();
    let mut s = Session::new();
    s.env.push(fake_ssh(&s, &[("rohan", &rohan)]));
    s.env.push(("HC_RECONNECT_MS".into(), "200".into()));
    s.json(&["machine", "add", "rohan"]);
    (rohan, s)
}

fn echo_listener() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || echo(listener.accept().unwrap().0));
    port
}

fn open_on(s: &Session, on: &str, address: &str) -> (Framed, serde_json::Value) {
    let mut conn = Framed::open(s);
    let reply = conn.call("socket.open", json!({"on": on, "address": address}));
    (conn, reply)
}

#[test]
fn echoes_through_a_machine() {
    let (_rohan, s) = with_rohan();
    let port = echo_listener();
    let (mut conn, reply) = open_on(&s, "rohan", &format!("127.0.0.1:{port}"));
    let channel = reply["result"]["channel"].as_u64().unwrap_or_else(|| panic!("{reply}")) as u32;
    conn.send(channel, wire::DATA, b"ping");
    assert_eq!(data(&mut conn, channel), b"ping");
}

#[test]
fn remote_refusal_comes_back() {
    let (_rohan, s) = with_rohan();
    let (_conn, reply) = open_on(&s, "rohan", "10.0.0.1:80");
    assert_eq!(reply["error"]["data"]["kind"], "not_allowed", "{reply}");
}

#[test]
fn socket_channel_ends_when_line_drops() {
    let (rohan, s) = with_rohan();
    let port = echo_listener();
    let (mut conn, reply) = open_on(&s, "rohan", &format!("127.0.0.1:{port}"));
    let channel = reply["result"]["channel"].as_u64().unwrap_or_else(|| panic!("{reply}")) as u32;
    conn.send(channel, wire::DATA, b"ping");
    assert_eq!(data(&mut conn, channel), b"ping");

    rohan.hc(&["server", "stop"]);
    // The next frame on the channel is its CLOSE: no "reconnecting" notice, no bytes.
    let start = std::time::Instant::now();
    loop {
        assert!(start.elapsed() < Duration::from_secs(5), "no CLOSE within 5 s");
        let Some((ch, kind, payload)) = conn.read(Duration::from_millis(200)) else { continue };
        if ch == channel {
            assert_eq!(kind, wire::CLOSE, "{:?}", String::from_utf8_lossy(&payload));
            break;
        }
    }
    while let Some((ch, _, _)) = conn.read(Duration::from_millis(500)) {
        assert_ne!(ch, channel, "a frame after CLOSE");
    }
}

#[test]
fn unknown_machine() {
    let s = Session::new();
    let (_conn, reply) = open_on(&s, "nowhere", "127.0.0.1:1");
    assert_eq!(reply["error"]["data"]["kind"], "machine_unknown", "{reply}");
}

#[test]
fn viewer_close_ends_remote_socket() {
    let (_rohan, s) = with_rohan();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (done, ended) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut peer = listener.accept().unwrap().0;
        let mut buf = [0u8; 16];
        while peer.read(&mut buf).unwrap_or(0) > 0 {}
        done.send(()).unwrap();
    });
    let (mut conn, reply) = open_on(&s, "rohan", &format!("127.0.0.1:{port}"));
    let channel = reply["result"]["channel"].as_u64().unwrap_or_else(|| panic!("{reply}")) as u32;
    conn.send(channel, wire::CLOSE, b"");
    ended.recv_timeout(WAIT).expect("the socket there closed");
}

#[test]
fn bulk_through_relay() {
    const TOTAL: usize = 4 << 20;
    let (_rohan, s) = with_rohan();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let mut read = listener.accept().unwrap().0;
        let mut write = read.try_clone().unwrap();
        echo_split(&mut read, &mut write);
    });
    let (mut conn, reply) = open_on(&s, "rohan", &format!("127.0.0.1:{port}"));
    bulk(&mut conn, &reply, TOTAL);
}

/// Sends `total` bytes on the reply's channel and checks they echo back unchanged, crediting as they come.
fn bulk(conn: &mut Framed, reply: &serde_json::Value, total: usize) {
    let channel = reply["result"]["channel"].as_u64().unwrap_or_else(|| panic!("{reply}")) as u32;
    let sent: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    for chunk in sent.chunks(16 << 10) {
        conn.send(channel, wire::DATA, chunk);
    }
    let mut got = Vec::with_capacity(total);
    let start = std::time::Instant::now();
    while got.len() < total {
        assert!(start.elapsed() < Duration::from_secs(60), "stuck after {} bytes", got.len());
        if let Some((ch, wire::DATA, payload)) = conn.read(Duration::from_millis(200))
            && ch == channel
        {
            got.extend_from_slice(&payload);
            conn.send(channel, wire::CREDIT, &(payload.len() as u32).to_le_bytes());
        }
    }
    assert!(got == sent, "the bytes came back changed");
}

/// A live terminal here sharing `127.0.0.1:<port>` as `share.<name>`; returns its address.
fn share(s: &Session, name: &str, port: u16) -> String {
    run(s, &["--label", &format!("share.{name}=127.0.0.1:{port}"), "sleep", "100"])
}

fn id_of(s: &Session) -> String {
    s.json(&["server", "status"])["core"]["id"].as_str().unwrap().to_string()
}

fn pings(conn: &mut Framed, reply: &serde_json::Value) {
    let channel = reply["result"]["channel"].as_u64().unwrap_or_else(|| panic!("{reply}")) as u32;
    conn.send(channel, wire::DATA, b"ping");
    assert_eq!(data(conn, channel), b"ping");
}

#[test]
fn share_resolves_locally() {
    let s = Session::new();
    share(&s, "echo", echo_listener());
    let mut conn = Framed::open(&s);
    let reply = conn.call("socket.open", json!({"address": "share:echo"}));
    pings(&mut conn, &reply);
}

#[test]
fn unknown_share_not_shared() {
    let s = Session::new();
    let mut conn = Framed::open(&s);
    let reply = conn.call("socket.open", json!({"address": "share:nothing"}));
    assert_eq!(reply["error"]["data"]["kind"], "not_shared", "{reply}");
}

#[test]
fn share_gone_when_terminal_ends() {
    let s = Session::new();
    let t = share(&s, "echo", echo_listener());
    s.json(&["kill", &t]);
    let mut conn = Framed::open(&s);
    let reply = conn.call("socket.open", json!({"address": "share:echo"}));
    assert_eq!(reply["error"]["data"]["kind"], "not_shared", "{reply}");
}

#[test]
fn peer_reaches_shared_socket() {
    let (rohan, s) = with_rohan();
    share(&s, "echo", echo_listener());
    let (mut conn, reply) = open_on(&rohan, &id_of(&s), "share:echo");
    pings(&mut conn, &reply);
}

#[test]
fn unix_share_not_allowed() {
    let s = Session::new();
    run(&s, &["--label", "share.u=/tmp/x.sock", "sleep", "100"]);
    let mut conn = Framed::open(&s);
    let reply = conn.call("socket.open", json!({"address": "share:u"}));
    assert_eq!(reply["error"]["data"]["kind"], "not_allowed", "{reply}");
}

#[test]
fn peer_unix_share_not_allowed() {
    let (rohan, s) = with_rohan();
    run(&s, &["--label", &format!("share.u={}", s.socket().display()), "sleep", "100"]);
    let (_conn, reply) = open_on(&rohan, &id_of(&s), "share:u");
    assert_eq!(reply["error"]["data"]["kind"], "not_allowed", "{reply}");
}

#[test]
fn bulk_from_a_peer() {
    let (rohan, s) = with_rohan();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        let mut read = listener.accept().unwrap().0;
        let mut write = read.try_clone().unwrap();
        echo_split(&mut read, &mut write);
    });
    share(&s, "bulk", port);
    let (mut conn, reply) = open_on(&rohan, &id_of(&s), "share:bulk");
    bulk(&mut conn, &reply, 4 << 20);
}

#[test]
fn peer_raw_address_not_allowed() {
    let (rohan, s) = with_rohan();
    let port = echo_listener();
    let (_conn, reply) = open_on(&rohan, &id_of(&s), &format!("127.0.0.1:{port}"));
    assert_eq!(reply["error"]["data"]["kind"], "not_allowed", "{reply}");
}

#[test]
fn machine_list_includes_peers() {
    let (rohan, s) = with_rohan();
    // The line comes up after `machine add` returns.
    wait_until("rohan sees its peer", || rohan.json(&["machine", "ls"])["peers"][0]["state"] == "connected");
    let list = rohan.json(&["machine", "ls"]);
    assert_eq!(list["machines"], json!([]), "{list}");
    let peer = &list["peers"][0];
    assert_eq!((&peer["id"], &peer["state"]), (&json!(id_of(&s)), &json!("connected")), "{list}");
    assert!(peer["name"].is_string(), "{list}");
}

#[test]
fn peer_socket_closes_when_line_drops() {
    let (rohan, s) = with_rohan();
    share(&s, "echo", echo_listener());
    let (mut conn, reply) = open_on(&rohan, &id_of(&s), "share:echo");
    pings(&mut conn, &reply);
    let channel = reply["result"]["channel"].as_u64().unwrap() as u32;
    s.hc(&["server", "stop"]);
    let start = std::time::Instant::now();
    loop {
        assert!(start.elapsed() < Duration::from_secs(5), "no CLOSE within 5 s");
        let Some((ch, kind, _)) = conn.read(Duration::from_millis(200)) else { continue };
        if ch == channel {
            assert_eq!(kind, wire::CLOSE);
            break;
        }
    }
}
