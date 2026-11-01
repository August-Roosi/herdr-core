//! `socket.open`: a loopback TCP port or unix socket, streamed over a framed connection.

use crate::remote::conn_key;
use crate::rpc::RpcError;
use crate::runtime::Core;
use crate::wire::{self, Conn};
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

/// An open socket: bytes for it, and the task that pumps it.
pub struct Open {
    input: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    task: tokio::task::AbortHandle,
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

async fn connect(
    target: Target,
) -> std::io::Result<(Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>)> {
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

pub async fn open(core: &Arc<Core>, conn: &Arc<Conn>, address: &str) -> Result<u32, RpcError> {
    let target = allowed(address)?;
    let refused = |why: String| RpcError::new("refused", format!("{address}: {why}"), "is something listening there?");
    let (read, write) = match tokio::time::timeout(Duration::from_secs(5), connect(target)).await {
        Ok(Ok(halves)) => halves,
        Ok(Err(e)) => return Err(refused(e.to_string())),
        Err(_) => return Err(refused("timed out".into())),
    };
    let channel = conn.new_channel();
    let key = (conn_key(conn), channel);
    let (input, rx) = tokio::sync::mpsc::unbounded_channel();
    conn.with(|out| out.open(channel));
    // The pump takes the state lock to leave it, so it can't run before we are in.
    let mut st = core.lock();
    let task = tokio::spawn(pump(core.clone(), conn.clone(), channel, read, write, rx)).abort_handle();
    st.sockets.insert(key, Open { input, task });
    Ok(channel)
}

/// Socket bytes to the channel (paused while a window's worth waits for credit),
/// channel bytes to the socket. Ends on EOF, an error, or when aborted.
async fn pump(
    core: Arc<Core>,
    conn: Arc<Conn>,
    channel: u32,
    mut read: Box<dyn AsyncRead + Send + Unpin>,
    mut write: Box<dyn AsyncWrite + Send + Unpin>,
    mut input: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let mut buf = vec![0u8; 64 << 10];
    loop {
        // The client's credit has no wake-up of its own: while paused, look again shortly.
        let paused = conn.outbox().queued(channel) >= wire::WINDOW;
        tokio::select! {
            bytes = input.recv() => match bytes {
                Some(bytes) if write.write_all(&bytes).await.is_ok() => {}
                _ => break,
            },
            n = read.read(&mut buf), if !paused => match n {
                Ok(n @ 1..) => conn.with(|out| {
                    out.data(channel, &buf[..n]);
                }),
                _ => break,
            },
            () = tokio::time::sleep(Duration::from_millis(5)), if paused => {}
        }
    }
    // Its end follows what it already sent.
    if core.lock().sockets.remove(&(conn_key(&conn), channel)).is_some() {
        conn.with(|out| out.finish(channel, b""));
    }
}

/// The client sent bytes on `channel`: for the socket, if it is one.
pub fn input(core: &Core, conn: &Arc<Conn>, channel: u32, bytes: Vec<u8>) -> bool {
    let st = core.lock();
    st.sockets.get(&(conn_key(conn), channel)).is_some_and(|s| {
        let _ = s.input.send(bytes);
        true
    })
}

/// The client closed `channel`: a socket there ends.
pub fn close(core: &Core, conn: &Arc<Conn>, channel: u32) {
    if let Some(s) = core.lock().sockets.remove(&(conn_key(conn), channel)) {
        s.task.abort();
    }
}

/// A connection ended: its sockets end.
pub fn conn_gone(core: &Core, conn: &Arc<Conn>) {
    let key = conn_key(conn);
    core.lock().sockets.retain(|(k, _), s| {
        let ours = *k == key;
        if ours {
            s.task.abort();
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
