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
//! - `feed` — inbound test: after CONNACK, firehose pre-encoded QoS 0 PUBLISH frames at the client
//!   as fast as possible (or paced to `RATE`), draining anything the client sends.
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
//!   BATCH         PUBLISH frames per write    (feed; default: 64)
//!   RATE          target msgs/sec, 0 = max    (feed; default: 0)

use std::io;
use std::pin::Pin;
use std::sync::Arc;

use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, split};
use tokio::net::{TcpListener, TcpStream};
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
    batch: usize,
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
        Ok(Self {
            role,
            bind: env_str("BIND", "127.0.0.1"),
            port: env_u64("PORT", 1883) as u16,
            tls,
            cert_file,
            key_file,
            topic: env_str("TOPIC", "bench/inbound"),
            payload: vec![b'x'; payload_bytes],
            batch: env_usize("BATCH", 64).max(1),
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

    // Drain whatever the client sends (typically just a DISCONNECT); stop on EOF/DISCONNECT so the
    // client's send buffer never backs up.
    let drain = tokio::spawn(async move {
        loop {
            match read_packet(&mut reader).await {
                Ok((b0, _)) if (b0 >> 4) == 14 => break, // DISCONNECT
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    // Pre-encode one PUBLISH and a batch of them, so the hot loop is just `write` (plus TLS
    // encryption when enabled).
    let frame = build_publish(&cfg.topic, &cfg.payload);
    let mut batch = Vec::with_capacity(frame.len() * cfg.batch);
    for _ in 0..cfg.batch {
        batch.extend_from_slice(&frame);
    }

    let started = Instant::now();
    let mut sent: u64 = 0;
    let batch_msgs = cfg.batch as u64;
    loop {
        if wr.write_all(&batch).await.is_err() {
            break; // client disconnected
        }
        sent += batch_msgs;
        if cfg.rate > 0 {
            let target = started + Duration::from_secs_f64(sent as f64 / cfg.rate as f64);
            let now = Instant::now();
            if target > now {
                sleep_until(target).await;
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

/// Builds a complete MQTT 5 QoS 0 PUBLISH frame for the given topic and payload.
fn build_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
    // Variable header: topic name (2-byte len + bytes) + property length (0).
    let mut var = Vec::with_capacity(2 + topic.len() + 1);
    var.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    var.extend_from_slice(topic.as_bytes());
    var.push(0x00); // property length = 0

    let remaining = var.len() + payload.len();
    let mut pkt = Vec::with_capacity(1 + 4 + remaining);
    pkt.push(0x30); // PUBLISH, QoS 0, no DUP/RETAIN
    encode_remaining_length(remaining, &mut pkt);
    pkt.extend_from_slice(&var);
    pkt.extend_from_slice(payload);
    pkt
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
     TOPIC, PAYLOAD_BYTES, BATCH, RATE (feed; RATE=0 = max).

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
