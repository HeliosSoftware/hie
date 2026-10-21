//! HL7 v2: messages as records, and an MLLP listener that spools each message
//! to disk before it acknowledges it.
//!
//! A message becomes a JSON object the transform can read without parsing pipes:
//! its type, control id, sender, and every segment as a list of fields (`PID-5`
//! is `fields[4]` of the `PID` segment; `MSH` counts the separator as its first
//! field, as the standard does). Components and repetitions stay as text with
//! their separators; the Python helpers in `hie_runtime/hie.py` split them.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// MLLP frame: `<VT>` message `<FS><CR>`.
pub const START: u8 = 0x0B;
pub const END: [u8; 2] = [0x1C, 0x0D];

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub message_type: String,
    pub control_id: String,
    pub sending_application: String,
    pub sending_facility: String,
    pub receiving_application: String,
    pub receiving_facility: String,
    pub version: String,
    pub segments: Vec<Segment>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    pub name: String,
    pub fields: Vec<String>,
}

/// Segment separators as they arrive: `\r` by the standard, `\n` or `\r\n` from
/// files edited by hand.
fn normalize(text: &str) -> String {
    text.replace("\r\n", "\r").replace('\n', "\r")
}

/// Parses one message. The first segment must be `MSH` with at least the
/// separator fields; a message is not refused for anything after that.
pub fn parse(text: &str) -> Result<Message> {
    let normalized = normalize(text);
    let lines: Vec<&str> = normalized
        .split('\r')
        .map(|l| l.trim_end_matches('\n'))
        .filter(|l| !l.trim().is_empty())
        .collect();
    let Some(first) = lines.first() else {
        bail!("empty message");
    };
    if !first.starts_with("MSH") || first.len() < 8 {
        bail!(
            "a message starts with an MSH segment; this one starts with {:?}",
            first.chars().take(20).collect::<String>()
        );
    }
    let field_sep = first[3..4].to_string();
    let mut segments = Vec::new();
    for line in &lines {
        let mut parts: Vec<String> = line.split(field_sep.as_str()).map(str::to_string).collect();
        let name = parts.remove(0);
        if name.len() != 3 {
            bail!("segment name {name:?} is not three characters");
        }
        let fields = if name == "MSH" {
            let mut f = vec![field_sep.clone()];
            f.extend(parts);
            f
        } else {
            parts
        };
        segments.push(Segment { name, fields });
    }
    let msh = |n: usize| segments[0].fields.get(n - 1).cloned().unwrap_or_default();
    Ok(Message {
        message_type: msh(9),
        control_id: msh(10),
        sending_application: msh(3),
        sending_facility: msh(4),
        receiving_application: msh(5),
        receiving_facility: msh(6),
        version: msh(12),
        segments,
        text: normalized,
    })
}

/// Splits a file holding several messages at every `MSH` segment. Returns each
/// message's text with the line it starts on.
pub fn split_messages(text: &str) -> Vec<(i64, String)> {
    let normalized = normalize(text);
    let mut out: Vec<(i64, String)> = Vec::new();
    for (i, line) in normalized.split('\r').enumerate() {
        if line.starts_with("MSH") {
            out.push((i as i64 + 1, String::new()));
        }
        if let Some((_, current)) = out.last_mut()
            && !line.trim().is_empty()
        {
            current.push_str(line);
            current.push('\r');
        }
    }
    out
}

impl Message {
    pub fn to_json(&self) -> Value {
        json!({
            "message_type": self.message_type,
            "control_id": self.control_id,
            "sending_application": self.sending_application,
            "sending_facility": self.sending_facility,
            "receiving_application": self.receiving_application,
            "receiving_facility": self.receiving_facility,
            "version": self.version,
            "segments": self.segments.iter().map(|s| json!({"name": s.name, "fields": s.fields})).collect::<Vec<_>>(),
            "text": self.text,
        })
    }

    /// The first segment of a name.
    pub fn segment(&self, name: &str) -> Option<&Segment> {
        self.segments.iter().find(|s| s.name == name)
    }
}

/// The application acknowledgement for a message: `AA` accepted, `AE` error,
/// `AR` rejected. The sender's names are swapped into the receiver's place.
pub fn ack(message: &Message, code: &str, text: &str, now: &str) -> String {
    let trigger = message
        .message_type
        .split('^')
        .nth(1)
        .unwrap_or("")
        .to_string();
    let version = if message.version.is_empty() {
        "2.5.1"
    } else {
        &message.version
    };
    let msa_text = text.replace(['|', '\r', '\n'], " ");
    format!(
        "MSH|^~\\&|HIE|HIE|{}|{}|{now}||ACK^{trigger}^ACK|{}|P|{version}\rMSA|{code}|{}|{msa_text}\r",
        message.sending_application,
        message.sending_facility,
        format_args!("HIE{now}"),
        message.control_id,
    )
}

/// An acknowledgement for bytes that are not a message at all.
fn reject(reason: &str, now: &str) -> String {
    format!(
        "MSH|^~\\&|HIE|HIE|||{now}||ACK|HIE{now}|P|2.5.1\rMSA|AR||{}\r",
        reason.replace(['|', '\r', '\n'], " ")
    )
}

pub fn frame(message: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 3);
    out.push(START);
    out.extend_from_slice(message.as_bytes());
    out.extend_from_slice(&END);
    out
}

/// Listens for MLLP connections and writes every message to `spool` as its own
/// file, acknowledging only once the file is on disk. A message that is not HL7
/// is answered `AR` and not kept.
pub async fn serve(feed_id: String, listener: TcpListener, spool: PathBuf) {
    let seq = std::sync::Arc::new(AtomicU64::new(0));
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(feed = %feed_id, "MLLP accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        tracing::info!(feed = %feed_id, %peer, "MLLP connection");
        let (feed_id, spool, seq) = (feed_id.clone(), spool.clone(), seq.clone());
        tokio::spawn(async move {
            if let Err(e) = connection(&feed_id, socket, &spool, &seq).await {
                tracing::warn!(feed = %feed_id, %peer, "MLLP connection ended: {e:#}");
            }
        });
    }
}

async fn connection(
    feed_id: &str,
    mut socket: tokio::net::TcpStream,
    spool: &Path,
    seq: &AtomicU64,
) -> Result<()> {
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = socket.read(&mut chunk).await.context("reading")?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);
        while let Some(start) = buffer.iter().position(|b| *b == START) {
            let Some(end) = buffer[start..]
                .windows(2)
                .position(|w| w == END)
                .map(|p| start + p)
            else {
                break;
            };
            let body = buffer[start + 1..end].to_vec();
            buffer.drain(..end + 2);
            let reply = receive(feed_id, &body, spool, seq)?;
            socket
                .write_all(&frame(&reply))
                .await
                .context("writing the acknowledgement")?;
        }
        if buffer.len() > 16 * 1024 * 1024 {
            bail!("16 MB without a frame end; closing");
        }
    }
}

/// Spools one message and builds its acknowledgement.
fn receive(feed_id: &str, body: &[u8], spool: &Path, seq: &AtomicU64) -> Result<String> {
    let now = chrono::Utc::now().format("%Y%m%d%H%M%S%.3f").to_string();
    let text = match std::str::from_utf8(body) {
        Ok(t) => t,
        Err(_) => {
            tracing::warn!(feed = feed_id, "MLLP message is not UTF-8; rejected");
            return Ok(reject("message is not UTF-8", &now));
        }
    };
    let message = match parse(text) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(feed = feed_id, "MLLP message rejected: {e:#}");
            return Ok(reject(&format!("{e:#}"), &now));
        }
    };
    let n = seq.fetch_add(1, Ordering::SeqCst);
    let safe_id: String = message
        .control_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(40)
        .collect();
    let name = format!("{now}-{n:04}-{safe_id}.hl7");
    std::fs::create_dir_all(spool).with_context(|| format!("creating {}", spool.display()))?;
    let path = spool.join(&name);
    let tmp = spool.join(format!(".{name}.part"));
    let store = || -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(message.text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &path)
    };
    match store() {
        Ok(()) => {
            tracing::info!(feed = feed_id, control_id = %message.control_id, kind = %message.message_type, file = %name, "message spooled");
            Ok(ack(&message, "AA", "", &now))
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            tracing::error!(feed = feed_id, "cannot spool a message: {e}");
            // Not stored, so not accepted: the sender keeps it and retries.
            Ok(ack(
                &message,
                "AE",
                &format!("HIE could not store the message: {e}"),
                &now,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADT: &str = "MSH|^~\\&|LAB|NORTH|HIE|HFS|20261021120000||ADT^A01|MSG0001|P|2.5.1\rEVN|A01|20261021120000\rPID|1||12345^^^NORTH^MR||Doe^Jane^M||19800101|F\r";

    #[test]
    fn a_message_parses_into_segments_and_fields() {
        let m = parse(ADT).unwrap();
        assert_eq!(m.message_type, "ADT^A01");
        assert_eq!(m.control_id, "MSG0001");
        assert_eq!(m.sending_application, "LAB");
        assert_eq!(m.version, "2.5.1");
        assert_eq!(m.segments.len(), 3);
        let msh = m.segment("MSH").unwrap();
        assert_eq!(msh.fields[0], "|");
        assert_eq!(msh.fields[1], "^~\\&");
        assert_eq!(msh.fields[8], "ADT^A01");
        let pid = m.segment("PID").unwrap();
        assert_eq!(pid.fields[2], "12345^^^NORTH^MR");
        assert_eq!(pid.fields[4], "Doe^Jane^M");
        assert_eq!(m.to_json()["segments"][2]["fields"][4], "Doe^Jane^M");
    }

    #[test]
    fn newlines_are_accepted_and_bad_messages_refused() {
        let m = parse(&ADT.replace('\r', "\n")).unwrap();
        assert_eq!(m.segments.len(), 3);
        assert!(parse("PID|1|x").is_err());
        assert!(parse("").is_err());
    }

    #[test]
    fn files_split_at_each_msh() {
        let two = format!("{ADT}\n{}", ADT.replace("MSG0001", "MSG0002"));
        let parts = split_messages(&two);
        assert_eq!(parts.len(), 2);
        assert_eq!((parts[0].0, parts[1].0), (1, 4));
        assert_eq!(parse(&parts[1].1).unwrap().control_id, "MSG0002");
    }

    #[test]
    fn the_ack_answers_the_sender() {
        let m = parse(ADT).unwrap();
        let a = ack(&m, "AA", "", "20261021120001");
        assert!(a.starts_with(
            "MSH|^~\\&|HIE|HIE|LAB|NORTH|20261021120001||ACK^A01^ACK|HIE20261021120001|P|2.5.1\r"
        ));
        assert!(a.ends_with("MSA|AA|MSG0001|\r"));
        let framed = frame(&a);
        assert_eq!(framed[0], START);
        assert_eq!(&framed[framed.len() - 2..], &END);
    }
}
