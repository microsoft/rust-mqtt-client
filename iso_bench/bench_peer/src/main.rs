// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Independent MQTT 5 peer that stands in for a broker so the client under test can be
//! benchmarked in isolation.
//!
//! It hand-rolls the minimal wire bytes it needs and does **not** depend on `ms-mqtt-client`, so
//! its behavior is invariant across client builds — that is what lets a receive/send measurement
//! be attributed to the *client* and not to broker behavior. The peer is deliberately trivial so
//! it is never the bottleneck: it pre-encodes frames and its ack path is a template splice.
//!
//! Roles (`ROLE` env var):
//! - `feed` — inbound test: after CONNACK, firehose PUBLISH frames at the client as fast as possible
//!   (or paced to `RATE`), draining anything the client sends. `QOS=0` (default) sends QoS 0; `QOS=1`
//!   sends QoS 1 with a cycling packet id (the client must PUBACK each; the drain loop consumes them).
//! - `sink` — outbound test: after CONNACK, drain the client's PUBLISHes; for QoS 1, reply with a
//!   PUBACK echoing the packet identifier.
//!
//! Transport: plaintext TCP by default, or TLS termination with `TLS=1` (needs `CERT_FILE`/
//! `KEY_FILE`; see `gen-test-certs.sh`).
//!
//! IMPORTANT TLS caveat: with plaintext the peer just writes pre-encoded bytes and is trivially
//! faster than the client, so throughput is client-bound. Under TLS the peer must **encrypt live**
//! (TLS records can't be pre-encoded or replayed), so it is no longer trivially faster —
//! single-session TLS throughput is bounded by `min(peer encrypt, client decrypt)`. For the
//! crypto-cost signal, prefer CLIENT CPU-per-message (`/usr/bin/time -v` on bench_client), which
//! stays un-confounded, and use `latency` mode for TLS round-trip.
//!
//! Env:
//!   ROLE          feed | sink                 (default: feed)
//!   BIND          listen address              (default: 127.0.0.1)
//!   PORT          listen port                 (default: 1883, use 8883 for TLS by convention)
//!   TLS           1 = terminate TLS           (default: 0)
//!   CERT_FILE     PEM server cert chain        (required if TLS=1)
//!   KEY_FILE      PEM server private key       (required if TLS=1)
//!   TOPIC         topic in pushed PUBLISHes   (feed; default: bench/inbound)
//!   PAYLOAD_BYTES pushed payload size         (feed; default: 64)
//!   QOS           0 | 1 for pushed PUBLISHes  (feed; default: 0)
//!   WINDOW        max in-flight QoS 1 publishes (feed; default: 1024)
//!   STAMP         embed 8-byte send time in payload (feed; default: 0, for recv-latency)
//!   BATCH         PUBLISH frames per write    (feed; default: 64)
//!   RATE          target msgs/sec, 0 = max    (feed; default: 0)

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, split};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time::{Duration, Instant, sleep_until};
use tokio_openssl::SslStream;

// Minimal canned MQTT 5 control packets.
// CONNACK: connect-ack flags 0x00 (no session present), reason 0x00 (success), property length 0.
const CONNACK: [u8; 5] = [0x20, 0x03, 0x00, 0x00, 0x00];
// PINGRESP.
const PINGRESP: [u8; 2] = [0xD0, 0x00];

#[derive(Clone, Copy)]
enum Role {
    Feed,
    Sink,
}

struct Config {
    role: Role,
    bind: String,
    port: u16,
    tls: bool,
    cert_file: Option<String>,
    key_file: Option<String>,
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    batch: usize,
    window: usize,
    stamp: bool,
    rate: u64,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let role = match env_str("ROLE", "feed").to_ascii_lowercase().as_str() {
            "feed" => Role::Feed,
            "sink" => Role::Sink,
            other => return Err(format!("unknown ROLE '{other}' (expected feed|sink)")),
        };
        let tls = matches!(env_str("TLS", "0").as_str(), "1" | "true");
        let cert_file = std::env::var("CERT_FILE").ok().filter(|s| !s.is_empty());
        let key_file = std::env::var("KEY_FILE").ok().filter(|s| !s.is_empty());
        if tls && (cert_file.is_none() || key_file.is_none()) {
            return Err("TLS=1 requires CERT_FILE and KEY_FILE".to_string());
        }
        let payload_bytes = env_usize("PAYLOAD_BYTES", 64);
        let qos = env_u64("QOS", 0) as u8;
        if qos > 1 {
            return Err("QOS must be 0 or 1 (feed does not implement QoS 2)".to_string());
        }
        let batch = env_usize("BATCH", 64).clamp(1, 65_534);
        let stamp = matches!(env_str("STAMP", "0").as_str(), "1" | "true");
        if stamp && payload_bytes < 8 {
            return Err(
                "STAMP=1 needs PAYLOAD_BYTES >= 8 (payload carries an 8-byte send stamp)"
                    .to_string(),
            );
        }
        Ok(Self {
            role,
            bind: env_str("BIND", "127.0.0.1"),
            port: env_u64("PORT", 1883) as u16,
            tls,
            cert_file,
            key_file,
            topic: env_str("TOPIC", "bench/inbound"),
            payload: vec![b'x'; payload_bytes],
            qos,
            batch,
            // QoS 1 max unacked, kept below the 65_535-id cycle (and >= batch) so a freshly assigned
            // packet id can never collide with one still in flight -- the client panics on a reused
            // in-flight id.
            window: env_usize("WINDOW", 1024).clamp(batch, 65_534),
            stamp,
            rate: env_u64("RATE", 0),
        })
    }
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|a| a == "--help" || a == "-h") || std::env::var("HELP").is_ok() {
        print_usage();
        return;
    }
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: {e}");
            std::process::exit(2);
        }
    };
    if let Err(e) = run(cfg).await {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    }
}

async fn run(cfg: Config) -> io::Result<()> {
    let listener = TcpListener::bind((cfg.bind.as_str(), cfg.port)).await?;

    let acceptor = if cfg.tls {
        let cert = cfg.cert_file.as_deref().unwrap();
        let key = cfg.key_file.as_deref().unwrap();
        Some(Arc::new(build_acceptor(cert, key).map_err(|e| {
            io::Error::other(format!("TLS setup failed: {e}"))
        })?))
    } else {
        None
    };

    let role = match cfg.role {
        Role::Feed => "feed",
        Role::Sink => "sink",
    };
    eprintln!(
        "bench_peer[{role}] listening on {}:{} ({})",
        cfg.bind,
        cfg.port,
        if cfg.tls { "tls" } else { "tcp" }
    );

    let cfg = Arc::new(cfg);
    loop {
        let (tcp, addr) = listener.accept().await?;
        tcp.set_nodelay(true)?;
        eprintln!("bench_peer: client connected from {addr}");
        let cfg = cfg.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let result = match acceptor {
                Some(acceptor) => match tls_accept(&acceptor, tcp).await {
                    Ok(stream) => serve(stream, &cfg).await,
                    Err(e) => Err(e),
                },
                None => serve(tcp, &cfg).await,
            };
            match result {
                Ok(()) => eprintln!("bench_peer: connection closed"),
                Err(e) => eprintln!("bench_peer: connection ended: {e}"),
            }
        });
    }
}

/// Builds a TLS server acceptor from a PEM cert chain and private key.
fn build_acceptor(
    cert_file: &str,
    key_file: &str,
) -> Result<SslAcceptor, openssl::error::ErrorStack> {
    let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
    builder.set_private_key_file(key_file, SslFiletype::PEM)?;
    builder.set_certificate_chain_file(cert_file)?;
    Ok(builder.build())
}

/// Completes the server-side TLS handshake over an accepted TCP connection.
async fn tls_accept(acceptor: &SslAcceptor, tcp: TcpStream) -> io::Result<SslStream<TcpStream>> {
    let ssl = openssl::ssl::Ssl::new(acceptor.context()).map_err(io::Error::other)?;
    let mut stream = SslStream::new(ssl, tcp).map_err(io::Error::other)?;
    Pin::new(&mut stream)
        .accept()
        .await
        .map_err(|e| io::Error::other(format!("TLS handshake failed: {e}")))?;
    Ok(stream)
}

/// Dispatches an established byte stream (TCP or TLS) to the configured role.
async fn serve<S>(stream: S, cfg: &Config) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match cfg.role {
        Role::Feed => handle_feed(stream, cfg).await,
        Role::Sink => handle_sink(stream).await,
    }
}

/// Inbound test: after the handshake, firehose pre-encoded PUBLISH frames at the client.
async fn handle_feed<S>(stream: S, cfg: &Config) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (rd, mut wr) = split(stream);
    let mut reader = BufReader::new(rd);
    let _connect = read_packet(&mut reader).await?; // consume CONNECT
    wr.write_all(&CONNACK).await?;

    // Reader task: drain what the client sends and stop on EOF/DISCONNECT so its send buffer never
    // backs up. For QoS 1 each PUBACK frees one window slot (see below).
    let window = Arc::new(Semaphore::new(if cfg.qos == 1 { cfg.window } else { 0 }));
    let window_reader = window.clone();
    let is_qos1 = cfg.qos == 1;
    let drain = tokio::spawn(async move {
        loop {
            match read_packet(&mut reader).await {
                Ok((b0, _)) if (b0 >> 4) == 14 => break, // DISCONNECT
                Ok((b0, _)) if is_qos1 && (b0 >> 4) == 4 => window_reader.add_permits(1), // PUBACK
                Ok(_) => {}
                Err(_) => break,
            }
        }
        // No more PUBACKs will arrive: close so a writer blocked on acquire_many wakes (Err) and exits.
        window_reader.close();
    });

    // Pre-encode a batch of PUBLISH frames so the hot loop is mostly `write` (plus TLS when enabled).
    // For QoS 1 we patch a fresh, cycling packet id into each frame and cap in-flight to `window`:
    // the client keeps every incoming id pending until it PUBACKs and panics if a still-pending id is
    // reused, so we act like a compliant server (never exceed the window, never reuse a live id). A
    // slot is freed per PUBACK the reader sees; with window << 65536 the cycling id never collides.
    let (frame, pkid_off) = build_publish(&cfg.topic, &cfg.payload, cfg.qos);
    let frame_len = frame.len();
    // Where the payload begins in each frame (it's the tail); STAMP overwrites its first 8 bytes.
    let stamp_off = cfg.stamp.then(|| frame_len - cfg.payload.len());
    let mut batch = Vec::with_capacity(frame_len * cfg.batch);
    for _ in 0..cfg.batch {
        batch.extend_from_slice(&frame);
    }

    let started = Instant::now();
    let mut sent: u64 = 0;
    let mut next_pkid: u16 = 1;
    let batch_msgs = cfg.batch as u64;
    loop {
        if let Some(off) = pkid_off {
            // Wait for `batch` free window slots, then stamp fresh cycling ids into the batch.
            let Ok(permit) = window.acquire_many(cfg.batch as u32).await else {
                break;
            };
            permit.forget(); // slots come back via the reader's add_permits on PUBACK, not on drop
            for i in 0..cfg.batch {
                let p = i * frame_len + off;
                let [hi, lo] = next_pkid.to_be_bytes();
                batch[p] = hi;
                batch[p + 1] = lo;
                next_pkid = if next_pkid == u16::MAX {
                    1
                } else {
                    next_pkid + 1
                };
            }
        }
        if let Some(off) = stamp_off {
            // Stamp the current send time (epoch nanos) into each frame's payload for recv-latency.
            let ts = epoch_nanos().to_le_bytes();
            for i in 0..cfg.batch {
                let p = i * frame_len + off;
                batch[p..p + 8].copy_from_slice(&ts);
            }
        }
        if wr.write_all(&batch).await.is_err() {
            break; // client disconnected
        }
        sent += batch_msgs;
        if cfg.rate > 0 {
            // Pace precisely: tokio's ~1ms timer would bunch sends into ms-scale bursts (which
            // inflates recv-latency via send-side queueing), so sleep the coarse part and busy-spin
            // the last <=2ms up to the target instant.
            let target = started + Duration::from_secs_f64(sent as f64 / cfg.rate as f64);
            let spin_margin = Duration::from_millis(2);
            while Instant::now() < target {
                let remaining = target - Instant::now();
                if remaining > spin_margin {
                    sleep_until(target - spin_margin).await;
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }

    drain.abort();
    Ok(())
}

/// Outbound test: after the handshake, drain the client's PUBLISHes and PUBACK QoS 1 messages.
async fn handle_sink<S>(stream: S) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (rd, mut wr) = split(stream);
    let mut reader = BufReader::new(rd);
    let _connect = read_packet(&mut reader).await?; // consume CONNECT
    wr.write_all(&CONNACK).await?;

    loop {
        let (b0, body) = match read_packet(&mut reader).await {
            Ok(packet) => packet,
            Err(_) => break, // EOF / broken connection
        };
        match b0 >> 4 {
            3 => {
                // PUBLISH: QoS is bits 2..1 of the first byte. QoS 1 requires a PUBACK echoing
                // the packet identifier; QoS 0 requires nothing (pure sink).
                let qos = (b0 >> 1) & 0x03;
                if qos > 0 {
                    if let Some(id) = extract_pkid(&body) {
                        wr.write_all(&build_puback(id)).await?;
                    }
                }
            }
            12 => wr.write_all(&PINGRESP).await?, // PINGREQ
            14 => break,                          // DISCONNECT
            _ => {}
        }
    }
    Ok(())
}

/// Reads one MQTT control packet: first byte + `remaining length` bytes of body.
async fn read_packet<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<(u8, Vec<u8>)> {
    let b0 = r.read_u8().await?;
    let len = read_remaining_length(r).await?;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).await?;
    Ok((b0, body))
}

/// Decodes an MQTT `remaining length` variable-byte integer (1..=4 bytes).
async fn read_remaining_length<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<usize> {
    let mut value = 0usize;
    let mut mult = 1usize;
    for _ in 0..4 {
        let byte = r.read_u8().await?;
        value += (byte & 0x7f) as usize * mult;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        mult *= 128;
    }
    Err(io::Error::other("malformed remaining length"))
}

/// Encodes an MQTT `remaining length` variable-byte integer onto `out`.
fn encode_remaining_length(mut len: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (len & 0x7f) as u8;
        len >>= 7;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

/// Builds a complete MQTT 5 PUBLISH frame. For QoS 1 the frame includes a placeholder packet id
/// (returned offset lets the caller patch it per send) and sets the QoS bit; QoS 0 has neither.
fn build_publish(topic: &str, payload: &[u8], qos: u8) -> (Vec<u8>, Option<usize>) {
    // Variable header: topic name (2-byte len + bytes), then (QoS 1 only) a 2-byte packet id, then
    // property length (0).
    let mut var = Vec::with_capacity(2 + topic.len() + 3);
    var.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    var.extend_from_slice(topic.as_bytes());
    let pkid_off_in_var = var.len();
    if qos == 1 {
        var.extend_from_slice(&[0x00, 0x00]); // packet id placeholder, patched per send
    }
    var.push(0x00); // property length = 0

    let remaining = var.len() + payload.len();
    let mut pkt = Vec::with_capacity(1 + 4 + remaining);
    pkt.push(if qos == 1 { 0x32 } else { 0x30 }); // PUBLISH, QoS bit set for 1, no DUP/RETAIN
    encode_remaining_length(remaining, &mut pkt);
    let pkid_off = (qos == 1).then_some(pkt.len() + pkid_off_in_var);
    pkt.extend_from_slice(&var);
    pkt.extend_from_slice(payload);
    (pkt, pkid_off)
}

/// Builds an MQTT 5 PUBACK for the given packet identifier (reason Success, no properties).
fn build_puback(id: u16) -> [u8; 6] {
    let [hi, lo] = id.to_be_bytes();
    // Remaining length 4 = packet id (2) + reason code (0x00) + property length (0x00).
    [0x40, 0x04, hi, lo, 0x00, 0x00]
}

/// Extracts the packet identifier from a QoS > 0 PUBLISH body (after the topic name).
fn extract_pkid(body: &[u8]) -> Option<u16> {
    if body.len() < 2 {
        return None;
    }
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let idx = 2 + topic_len;
    if body.len() < idx + 2 {
        return None;
    }
    Some(u16::from_be_bytes([body[idx], body[idx + 1]]))
}

/// Wall-clock nanoseconds since the Unix epoch -- comparable across processes on one host so the
/// client can difference it against its own receive time (recv-latency).
fn epoch_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn print_usage() {
    print!(
        "\
bench_peer — independent MQTT 5 peer for isolating the client under test (env-var driven)

Stands in for a broker so the client's transport can be measured without broker confounds. Start
this first, then point bench_client at it (HOST/PORT).

Roles:
  ROLE=feed   inbound test  — firehose PUBLISHes at the client (measure client receive throughput)
  ROLE=sink   outbound test — drain client PUBLISHes, PUBACK QoS 1 (measure client send path)

Transport: TCP by default; TLS=1 terminates TLS (needs CERT_FILE/KEY_FILE — see gen-test-certs.sh).
  NOTE: under TLS the peer must encrypt live, so single-session throughput is bounded by
  min(peer, client) crypto. Lean on bench_client CPU-per-msg (/usr/bin/time -v) for the TLS signal.

Env: ROLE(feed|sink), BIND, PORT, TLS(0|1), CERT_FILE, KEY_FILE,
     TOPIC, PAYLOAD_BYTES, QOS(0|1), WINDOW(feed QoS1 in-flight), STAMP(feed recv-latency), BATCH,
     RATE (feed; RATE=0 = max).

Examples:
  # Inbound over TCP: feed at max rate, 256-byte payloads
  ROLE=feed PORT=1883 PAYLOAD_BYTES=256 cargo run -p bench_peer --release

  # Inbound over TLS (generate certs first: ./gen-test-certs.sh)
  ROLE=feed TLS=1 PORT=8883 CERT_FILE=certs/server.crt KEY_FILE=certs/server.key \\
    PAYLOAD_BYTES=256 cargo run -p bench_peer --release

  # Outbound: sink + PUBACK for the client's publish workloads
  ROLE=sink PORT=1883 cargo run -p bench_peer --release
"
    );
}
