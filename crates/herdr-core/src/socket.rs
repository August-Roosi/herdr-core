//! `socket.open`: a loopback TCP port or unix socket, streamed over a framed connection.

use crate::remote::conn_key;
use crate::rpc::RpcError;
use crate::runtime::{Core, State};
use crate::wire::{self, Conn};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Where a socket may point. Decided without IO.
#[derive(Debug, PartialEq)]
pub enum Target {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

/// Client frames waiting for a socket that is slow to take them. Past this the
/// channel is closed, so a stuck peer can't grow the core's memory.
const MAX_QUEUED: usize = 1024;

/// An open socket: bytes for it, and the two tasks that pump it.
pub struct Open {
    input: tokio::sync::mpsc::Sender<Vec<u8>>,
    tasks: [tokio::task::AbortHandle; 2],
}

/// Only loopback addresses and unix socket paths. Names are never resolved,
/// so a DNS answer can't redirect the connection.
pub fn allowed(address: &str) -> Result<Target, RpcError> {
    if address.starts_with('/') {
        return Ok(Target::Unix(address.into()));
    }
    match address.parse::<SocketAddr>() {
        Ok(addr) if addr.ip().is_loopback() => Ok(Target::Tcp(addr)),
        _ => Err(RpcError::new(
            "not_allowed",
            format!("{address} is not a loopback address"),
            "only 127.0.0.0/8, ::1 or a unix socket path",
        )),
    }
}

type Halves = (Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>);

/// What a live terminal here shares as `share.<name>` (the newest, if several do).
pub fn resolve_share(st: &State, name: &str) -> Option<String> {
    let label = format!("share.{name}");
    st.reg.terminals.values().rev().filter(|t| t.is_live()).find_map(|t| t.labels.get(&label).cloned())
}

async fn connect(target: Target) -> std::io::Result<Halves> {
    Ok(match target {
        Target::Tcp(addr) => {
            let (r, w) = tokio::net::TcpStream::connect(addr).await?.into_split();
            (Box::new(r), Box::new(w))
        }
        Target::Unix(path) => {
            let (r, w) = crate::platform::connect_unix(&path).await?;
            (Box::new(r), Box::new(w))
        }
    })
}

/// Connects to `address`, or to what a terminal here shares as `share:<name>`.
async fn connect_to(core: &Core, address: &str) -> Result<Halves, RpcError> {
    let address = match address.strip_prefix("share:") {
        Some(name) => resolve_share(&core.lock(), name).ok_or_else(|| {
            RpcError::new("not_shared", format!("no live terminal here shares {name:?}"), "is it running?")
        })?,
        None => address.to_string(),
    };
    let target = allowed(&address)?;
    let refused = |why: String| RpcError::new("refused", format!("{address}: {why}"), "is something listening there?");
    match tokio::time::timeout(Duration::from_secs(5), connect(target)).await {
        Ok(Ok(halves)) => Ok(halves),
        Ok(Err(e)) => Err(refused(e.to_string())),
        Err(_) => Err(refused("timed out".into())),
    }
}

pub async fn open(core: &Arc<Core>, conn: &Arc<Conn>, address: &str) -> Result<u32, RpcError> {
    let halves = connect_to(core, address).await?;
    let channel = conn.new_channel();
    conn.with(|out| out.open(channel));
    start(core, conn, channel, halves);
    Ok(channel)
}

/// `socket.open` from a machine this core dialed: it reaches only what a
/// terminal here shares. The reply goes on `back` ahead of the socket's bytes.
pub async fn serve(core: &Arc<Core>, back: &Arc<Conn>, id: Value, address: &str) {
    let opened = match address.starts_with("share:") {
        true => connect_to(core, address).await,
        false => Err(RpcError::new(
            "not_allowed",
            format!("{address} is not shared here"),
            "a machine this core dialed opens only share:<name>",
        )),
    };
    let reply = |result| wire::encode(0, wire::DATA, crate::rpc::response(id, result).to_string().as_bytes());
    match opened {
        Ok(halves) => {
            let channel = back.new_channel();
            back.with(|out| {
                out.control(reply(Ok(json!({"channel": channel}))));
                out.open(channel);
            });
            start(core, back, channel, halves);
        }
        Err(e) => back.with(|out| out.control(reply(Err(e)))),
    }
}

/// Pumps an open socket on `channel`, already open on `conn`.
fn start(core: &Arc<Core>, conn: &Arc<Conn>, channel: u32, (read, write): Halves) {
    let key = (conn_key(conn), channel);
    let (input, rx) = tokio::sync::mpsc::channel(MAX_QUEUED);
    // The pumps take the state lock to leave it, so they can't end before we are in.
    let mut st = core.lock();
    let tasks = [
        tokio::spawn(read_pump(core.clone(), conn.clone(), channel, read)).abort_handle(),
        tokio::spawn(write_pump(core.clone(), conn.clone(), channel, write, rx)).abort_handle(),
    ];
    st.sockets.insert(key, Open { input, tasks });
}

/// Socket bytes to the channel, paused while a window's worth waits for credit.
/// It never waits on the write side, so a peer that writes while it reads can't deadlock us.
async fn read_pump(core: Arc<Core>, conn: Arc<Conn>, channel: u32, mut read: Box<dyn AsyncRead + Send + Unpin>) {
    let mut buf = vec![0u8; 64 << 10];
    loop {
        // The client's credit has no wake-up of its own: while paused, look again shortly.
        if conn.outbox().queued(channel) >= wire::WINDOW {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        match read.read(&mut buf).await {
            Ok(n @ 1..) => conn.with(|out| {
                out.data(channel, &buf[..n]);
            }),
            _ => break,
        }
    }
    end(&core, &conn, channel);
}

/// Channel bytes to the socket.
async fn write_pump(
    core: Arc<Core>,
    conn: Arc<Conn>,
    channel: u32,
    mut write: Box<dyn AsyncWrite + Send + Unpin>,
    mut input: tokio::sync::mpsc::Receiver<Vec<u8>>,
) {
    while let Some(bytes) = input.recv().await {
        if write.write_all(&bytes).await.is_err() {
            break;
        }
    }
    end(&core, &conn, channel);
}

/// The socket ended on its own side: both pumps stop and the channel ends after what it already sent.
fn end(core: &Core, conn: &Arc<Conn>, channel: u32) {
    if let Some(s) = core.lock().sockets.remove(&(conn_key(conn), channel)) {
        s.tasks.iter().for_each(|t| t.abort());
        conn.with(|out| out.finish(channel, b""));
    }
}

/// The client sent bytes on `channel`: for the socket, if it is one.
pub fn input(core: &Core, conn: &Arc<Conn>, channel: u32, bytes: Vec<u8>) -> bool {
    let mut st = core.lock();
    let key = (conn_key(conn), channel);
    let Some(socket) = st.sockets.get(&key) else { return false };
    if socket.input.try_send(bytes).is_err() {
        // Too far behind (or already ending): the channel ends now.
        if let Some(s) = st.sockets.remove(&key) {
            s.tasks.iter().for_each(|t| t.abort());
        }
        conn.with(|out| out.close(channel, b""));
    }
    true
}

/// The client closed `channel`: a socket there ends. False: not a socket.
pub fn close(core: &Core, conn: &Arc<Conn>, channel: u32) -> bool {
    let gone = core.lock().sockets.remove(&(conn_key(conn), channel));
    gone.map(|s| s.tasks.iter().for_each(|t| t.abort())).is_some()
}

/// A connection ended: its sockets end.
pub fn conn_gone(core: &Core, conn: &Arc<Conn>) {
    let key = conn_key(conn);
    core.lock().sockets.retain(|(k, _), s| {
        let ours = *k == key;
        if ours {
            s.tasks.iter().for_each(|t| t.abort());
        }
        !ours
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_loopback_and_unix_paths_are_allowed() {
        for ok in ["127.0.0.1:1", "127.5.5.5:1", "[::1]:1"] {
            assert!(matches!(allowed(ok), Ok(Target::Tcp(_))), "{ok}");
        }
        assert_eq!(allowed("/tmp/x.sock").unwrap(), Target::Unix("/tmp/x.sock".into()));
        for no in ["0.0.0.0:1", "[::]:1", "192.168.1.2:1", "localhost:1"] {
            assert_eq!(allowed(no).unwrap_err().kind, "not_allowed", "{no}");
        }
    }
}
