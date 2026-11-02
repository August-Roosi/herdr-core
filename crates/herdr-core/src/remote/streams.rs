//! Streams there relayed to viewers here: terminal viewers and sockets.

use super::{Line, Relayed, To, conn_key, deadline_for, line, localize, lock, negotiate_resize};
use crate::rpc::RpcError;
use crate::runtime::Core;
use crate::wire::{self, Conn};
use serde_json::Value;
use std::sync::Arc;

/// The calls whose reply opens a stream there.
pub const STREAMS: [&str; 2] = ["terminal.attach", "socket.open"];

/// Opens a stream there (`terminal.attach` or `socket.open`; a peer only
/// serves the latter): its channel there is relayed to a new channel on this
/// connection, both ways.
pub async fn open_stream(
    core: &Core,
    conn: &Arc<Conn>,
    to: &To,
    method: &str,
    mut params: Value,
) -> Result<Value, RpcError> {
    let method = STREAMS.into_iter().find(|s| *s == method).expect("a stream method");
    let (line, name, id) = match to {
        To::Machine(m) => (line(core, m).await?, &m.name, &m.id),
        To::Peer { id, name } => (super::peers::line(core, id, name)?, name, id),
    };
    negotiate_resize(&line, &mut params);
    let here = conn.new_channel();
    conn.with(|out| out.open_paused(here));
    let deadline = deadline_for(core, method, &params);
    let sink = ((conn.clone(), here, method == "socket.open"), false);
    match line.call_into(name, method, params.clone(), Some(sink), deadline).await {
        Ok(mut r) => {
            let there = r["channel"].as_u64().unwrap_or_default() as u32;
            if method == "terminal.attach" {
                let enabled = params["observe"] == true
                    && params["resize_notifications"] == true
                    && r["resize_notifications"] == true;
                r["resize_notifications"] = enabled.into();
                params["resize_notifications"] = enabled.into();
            }
            let mut st = core.lock();
            // It went live with the reply: a CLOSE there or a dropped line since
            // then found it not relayed yet and left its end to us.
            if lock(&line.streams).contains_key(&there) {
                let viewer = Relayed { line, there, conn: conn.clone(), machine: id.clone(), method, params };
                st.relayed.insert((conn_key(conn), here), viewer);
            } else {
                conn.with(|out| out.finish(here, b""));
            }
            drop(st);
            r["channel"] = here.into();
            Ok(localize(core, name, &[], r))
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
pub(super) fn line_dropped(core: &Core, name: &str, line: &Line) {
    let notice = format!("\r\n[line to {name} lost; reconnecting...]\r\n");
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

/// A frame from there on its stream `there`, for the viewer here it goes to.
/// False: no such stream. The streams guard is let go before anything else is
/// locked: `conn_gone` takes the state lock, then this one.
pub(super) fn stream_out(core: &Core, line: &Line, there: u32, kind: u8, payload: &[u8]) -> bool {
    let sink = match kind {
        wire::DATA => lock(&line.streams).get(&there).cloned(),
        wire::RESIZE if payload.len() == 4 => lock(&line.streams).get(&there).cloned(),
        wire::CLOSE => lock(&line.streams).remove(&there),
        _ => None,
    };
    let Some((conn, here, _)) = sink else { return false };
    if kind == wire::RESIZE {
        conn.with(|out| out.ordered_resize(here, payload));
    } else if kind == wire::DATA {
        conn.with(|out| {
            out.data(here, payload);
        });
    // Not relayed yet: `open_stream` ends it once it is.
    } else if core.lock().relayed.remove(&(conn_key(&conn), here)).is_some() {
        conn.with(|out| out.finish(here, payload));
    }
    true
}

/// A frame from there for a socket served here (`socket::serve`), as a
/// framed connection's own client frames are taken.
pub(super) fn served_in(core: &Core, line: &Line, there: u32, kind: u8, payload: &[u8]) {
    let back = &line.back;
    match kind {
        wire::DATA => {
            crate::socket::input(core, back, there, payload.to_vec());
        }
        wire::CREDIT if payload.len() == 4 => {
            back.with(|out| out.credit(there, u32::from_le_bytes(payload.try_into().unwrap()) as usize))
        }
        wire::CLOSE if crate::socket::close(core, back, there) => back.with(|out| out.close(there, b"")),
        _ => {}
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
