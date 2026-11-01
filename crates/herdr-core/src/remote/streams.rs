//! Streams there relayed to viewers here: terminal viewers and sockets.

use super::{Line, Relayed, conn_key, deadline_for, line, localize, lock};
use crate::machines::Machine;
use crate::rpc::RpcError;
use crate::runtime::Core;
use crate::wire::{self, Conn};
use serde_json::Value;
use std::sync::Arc;

/// The calls whose reply opens a stream there.
pub const STREAMS: [&str; 2] = ["terminal.attach", "socket.open"];

/// Opens a stream there (`terminal.attach` or `socket.open`): its channel there
/// is relayed to a new channel on this connection, both ways.
pub async fn open_stream(
    core: &Core,
    conn: &Arc<Conn>,
    m: &Machine,
    method: &str,
    params: Value,
) -> Result<Value, RpcError> {
    let method = STREAMS.into_iter().find(|s| *s == method).expect("a stream method");
    let line = line(core, m).await?;
    let here = conn.new_channel();
    conn.with(|out| out.open(here));
    let deadline = deadline_for(core, method, &params);
    let sink = (conn.clone(), here, method == "socket.open");
    match line.call_into(&m.name, method, params.clone(), Some(sink), deadline).await {
        Ok(mut r) => {
            let there = r["channel"].as_u64().unwrap_or_default() as u32;
            let mut st = core.lock();
            // It went live with the reply: a CLOSE there or a dropped line since
            // then found it not relayed yet and left its end to us.
            if lock(&line.streams).contains_key(&there) {
                let viewer = Relayed { line, there, conn: conn.clone(), machine: m.id.clone(), method, params };
                st.relayed.insert((conn_key(conn), here), viewer);
            } else {
                conn.with(|out| out.finish(here, b""));
            }
            drop(st);
            r["channel"] = here.into();
            Ok(localize(core, &m.name, &[], r))
        }
        Err(e) => {
            conn.with(|out| out.close(here, b""));
            Err(e)
        }
    }
}

/// A line dropped. Terminal viewers hear so and stay open: the programs keep
/// running there, and the viewers get them back with the line. A socket
/// there is gone with it, so its channel here ends.
pub(super) fn line_dropped(core: &Core, m: &Machine, line: &Line) {
    let notice = format!("\r\n[line to {} lost; reconnecting...]\r\n", m.name);
    let streams: Vec<_> = lock(&line.streams).drain().map(|(_, sink)| sink).collect();
    for (conn, here, socket) in streams {
        if socket {
            // Not relayed yet: `open_stream` ends it once it is.
            if core.lock().relayed.remove(&(conn_key(&conn), here)).is_some() {
                conn.with(|out| out.finish(here, b""));
            }
        } else {
            conn.with(|out| {
                out.data(here, notice.as_bytes());
            });
        }
    }
}

/// A viewer's frame for a relayed stream goes there. False: not relayed.
pub fn relay_in(core: &Core, conn: &Arc<Conn>, here: u32, kind: u8, payload: &[u8]) -> bool {
    let r = {
        let mut st = core.lock();
        let Some(r) = st.relayed.get_mut(&(conn_key(conn), here)) else { return false };
        // What a reattach after a dropped line brings back.
        if kind == wire::RESIZE && payload.len() == 4 {
            r.params["cols"] = u16::from_le_bytes([payload[0], payload[1]]).into();
            r.params["rows"] = u16::from_le_bytes([payload[2], payload[3]]).into();
        } else if kind == wire::FACTS
            && let Ok(display) = serde_json::from_slice::<Value>(payload)
        {
            r.params["display"] = display;
        }
        r.clone()
    };
    // Read here: our stream to the viewer may send more, and so may the one there to us.
    if kind == wire::CREDIT && payload.len() == 4 {
        conn.with(|out| out.credit(here, u32::from_le_bytes(payload.try_into().unwrap()) as usize));
    }
    // While the line is down this goes nowhere: typing into a gap is lost, not queued.
    let _ = r.line.frames.send(wire::encode(r.there, kind, payload));
    true
}

/// A viewer's connection closed: its relayed streams there end.
pub fn conn_gone(core: &Core, conn: &Arc<Conn>) {
    let key = conn_key(conn);
    let mut st = core.lock();
    let gone: Vec<(u32, Arc<Line>, u32)> = st
        .relayed
        .iter()
        .filter(|((k, _), _)| *k == key)
        .map(|((_, here), r)| (*here, r.line.clone(), r.there))
        .collect();
    for (here, line, there) in gone {
        st.relayed.remove(&(key, here));
        lock(&line.streams).remove(&there);
        let _ = line.frames.send(wire::encode(there, wire::CLOSE, b""));
    }
}
