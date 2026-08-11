// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Establishes and adapts the base byte stream to the target: direct TCP, HTTP `CONNECT` proxy
//! tunneling, and TLS. Everything above this layer consumes an `AsyncRead + AsyncWrite` and does
//! not care how the stream was obtained.
//!
//! The unit of this module is the [`TransportStream`] it produces; keep new stream-establishment
//! concerns here, but note that [`tls_handshake`] is the natural extraction point should the TLS
//! primitive ever need to be shared more widely.

use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::{
    io::{
        AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
        BufReader, ReadBuf,
    },
    net::TcpStream,
};
use tokio_openssl::SslStream;

use crate::transport::{Proxy, ProxyAuthorization, ProxyEndpoint, TlsConfig};

const MAX_PROXY_RESPONSE_HEADER_BYTES: usize = 64 * 1024;

/// An established base transport byte stream.
///
/// This is intentionally opaque to consumers: they only require `AsyncRead + AsyncWrite`.
/// Whether the underlying connection is plain TCP or a TLS connection to an HTTPS proxy is an
/// internal detail of [`connect`].
pub(crate) struct TransportStream(TransportStreamInner);

enum TransportStreamInner {
    /// A plain TCP connection (direct, or tunneled through an HTTP proxy).
    Plain(TcpStream),
    /// A TLS connection to an HTTPS proxy, carrying the `CONNECT` tunnel to the target.
    Tls(SslStream<TcpStream>),
}

/// Obtain a [`TransportStream`] connected to the given target, optionally through a proxy.
///
/// If `proxy` is `None`, this connects directly to the target.
/// If `proxy` is `Some`, an HTTP `CONNECT` tunnel is established through the proxy before
/// returning the stream. For an [`ProxyEndpoint::Https`] proxy, the connection to the proxy
/// itself is wrapped in TLS; the connection to the target is not (see [`connect_tls`]).
///
/// `tcp_nodelay` sets the `TCP_NODELAY` option (Nagle's algorithm) on the underlying TCP socket.
pub(crate) async fn connect(
    hostname: &str,
    port: u16,
    proxy: Option<Proxy>,
    tcp_nodelay: bool,
) -> io::Result<TransportStream> {
    match proxy {
        None => {
            let stream = tcp_connect(hostname, port, tcp_nodelay).await?;
            Ok(TransportStream(TransportStreamInner::Plain(stream)))
        }
        Some(proxy) => http_connect_tunnel(proxy, hostname, port, tcp_nodelay).await,
    }
}

/// Obtain a [`TransportStream`] connected to the given target and wrapped in a client-side TLS
/// session with the target, optionally through a proxy.
///
/// Equivalent to [`connect`] followed by [`tls_handshake`], so the same proxy behavior applies.
/// The TLS session established here is with the target. For an [`ProxyEndpoint::Https`] proxy, the
/// connection to the proxy itself is wrapped in a separate TLS session inside [`connect`].
///
/// `tcp_nodelay` sets the `TCP_NODELAY` option (Nagle's algorithm) on the underlying TCP socket.
pub(crate) async fn connect_tls(
    hostname: &str,
    port: u16,
    config: TlsConfig,
    proxy: Option<Proxy>,
    tcp_nodelay: bool,
) -> io::Result<SslStream<TransportStream>> {
    let stream = connect(hostname, port, proxy, tcp_nodelay).await?;
    tls_handshake(stream, config, hostname).await
}

/// Connect a [`TcpStream`] to the given host and port, applying the `TCP_NODELAY` option
/// (Nagle's algorithm) to the socket.
async fn tcp_connect(host: &str, port: u16, tcp_nodelay: bool) -> io::Result<TcpStream> {
    let stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(tcp_nodelay)?;
    Ok(stream)
}

/// Establish an HTTP CONNECT tunnel through the given proxy to the target host and port.
///
/// Connects to the proxy endpoint (wrapping the connection in TLS for an
/// [`ProxyEndpoint::Https`] proxy), performs the HTTP `CONNECT` exchange, and returns the
/// resulting transparent tunnel to the target.
async fn http_connect_tunnel(
    proxy: Proxy,
    target_host: &str,
    target_port: u16,
    tcp_nodelay: bool,
) -> io::Result<TransportStream> {
    let Proxy { endpoint, auth } = proxy;
    match endpoint {
        ProxyEndpoint::Http { hostname, port } => {
            let stream = tcp_connect(&hostname, port, tcp_nodelay).await?;
            let stream = http_connect_exchange(stream, target_host, target_port, &auth).await?;
            Ok(TransportStream(TransportStreamInner::Plain(stream)))
        }
        ProxyEndpoint::Https {
            hostname,
            port,
            tls_config,
        } => {
            let stream = tcp_connect(&hostname, port, tcp_nodelay).await?;
            // Wrap the connection to the proxy itself in TLS before tunneling.
            let stream = tls_handshake(stream, tls_config, &hostname).await?;
            let stream = http_connect_exchange(stream, target_host, target_port, &auth).await?;
            Ok(TransportStream(TransportStreamInner::Tls(stream)))
        }
    }
}

/// Perform the HTTP `CONNECT` request/response exchange over an established stream to a proxy,
/// returning the same stream — now a transparent tunnel to the target.
async fn http_connect_exchange<S>(
    mut stream: S,
    target_host: &str,
    target_port: u16,
    auth: &ProxyAuthorization,
) -> io::Result<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // `target_host` is embedded in the CONNECT request line and `Host` header. Control characters
    // or whitespace (notably CR/LF) would allow HTTP request splitting / header injection into the
    // proxy conversation. No valid hostname or IP literal contains such characters, so reject
    // rather than encode — the proxy expects a literal host in the authority-form request-target
    // and `Host` header, and percent-encoding would only produce an unresolvable host.
    if target_host.is_empty()
        || target_host
            .bytes()
            .any(|b| b.is_ascii_control() || b == b' ')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "proxy target host contains invalid characters",
        ));
    }

    // Build the HTTP CONNECT request
    let authority = if target_host.contains(':') {
        format!("[{target_host}]:{target_port}")
    } else {
        format!("{target_host}:{target_port}")
    };
    let mut request = format!(
        "CONNECT {authority} HTTP/1.1\r\n\
          Host: {authority}\r\n"
    );

    match auth {
        ProxyAuthorization::None => {}
        ProxyAuthorization::Basic { username, password } => {
            let credentials =
                openssl::base64::encode_block(format!("{username}:{password}").as_bytes());
            request.push_str("Proxy-Authorization: Basic ");
            request.push_str(&credentials);
            request.push_str("\r\n");
        }
    }

    request.push_str("\r\n");

    // Send the CONNECT request
    stream.write_all(request.as_bytes()).await?;

    // Read the HTTP response status line
    let mut buf_reader = BufReader::new(stream);
    let mut response_header_bytes = 0;
    let mut status_line = String::new();
    read_proxy_response_line(
        &mut buf_reader,
        &mut status_line,
        &mut response_header_bytes,
    )
    .await?;

    // Validate the response status
    let mut status_parts = status_line.split_whitespace();
    let version = status_parts.next();
    let status = status_parts
        .next()
        .filter(|status| status.len() == 3)
        .and_then(|status| status.parse::<u16>().ok());
    if !matches!(version, Some("HTTP/1.1" | "HTTP/1.0"))
        || !status.is_some_and(|status| (200..=299).contains(&status))
    {
        return Err(io::Error::other(format!(
            "proxy CONNECT failed: {}",
            status_line.trim()
        )));
    }

    // Consume remaining headers until the empty line
    let mut header_line = String::new();
    loop {
        header_line.clear();
        read_proxy_response_line(
            &mut buf_reader,
            &mut header_line,
            &mut response_header_bytes,
        )
        .await?;
        if header_line == "\r\n" || header_line == "\n" || header_line.is_empty() {
            break;
        }
    }

    // Unwrap the buffered reader to recover the raw stream — it is now a transparent tunnel.
    // LIMITATION: `BufReader::into_inner` discards any bytes it has already buffered past the
    // header terminator. This is safe for `CONNECT` because the client speaks first (TLS
    // ClientHello / MQTT CONNECT), so a well-behaved proxy sends nothing after the blank line.
    // However, a proxy that coalesces the `200` response with early target bytes into one segment
    // would cause those bytes to be silently lost here. Revisit if this ever proves a problem.
    Ok(buf_reader.into_inner())
}

async fn read_proxy_response_line<R>(
    reader: &mut R,
    line: &mut String,
    total_bytes_read: &mut usize,
) -> io::Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    let remaining = MAX_PROXY_RESPONSE_HEADER_BYTES.saturating_sub(*total_bytes_read);
    let bytes_read = reader.take(remaining as u64 + 1).read_line(line).await?;
    *total_bytes_read += bytes_read;

    if *total_bytes_read > MAX_PROXY_RESPONSE_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy response headers exceed size limit",
        ));
    }

    Ok(bytes_read)
}

/// Wrap an established stream in a client-side TLS session, returning the encrypted stream.
///
/// The hostname is used for SNI and to match against the server cert SAN.
pub(crate) async fn tls_handshake<S>(
    stream: S,
    config: TlsConfig,
    hostname: &str,
) -> io::Result<SslStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let TlsConfig(connector) = config;
    let connector = connector.build().configure()?;

    let ssl = connector.into_ssl(hostname)?;
    let mut ssl_stream = SslStream::new(ssl, stream)?;

    Pin::new(&mut ssl_stream)
        .connect()
        .await
        .map_err(openssl_err_to_io_err)?;

    Ok(ssl_stream)
}

fn openssl_err_to_io_err(err: impl Into<openssl::ssl::Error>) -> io::Error {
    match err.into().into_io_error() {
        Ok(err) => err,
        Err(err) => io::Error::other(err),
    }
}

impl AsyncRead for TransportStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            TransportStreamInner::Plain(s) => Pin::new(s).poll_read(cx, buf),
            TransportStreamInner::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().0 {
            TransportStreamInner::Plain(s) => Pin::new(s).poll_write(cx, buf),
            TransportStreamInner::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().0 {
            TransportStreamInner::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            TransportStreamInner::Tls(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match &self.0 {
            TransportStreamInner::Plain(s) => s.is_write_vectored(),
            TransportStreamInner::Tls(s) => s.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            TransportStreamInner::Plain(s) => Pin::new(s).poll_flush(cx),
            TransportStreamInner::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().0 {
            TransportStreamInner::Plain(s) => Pin::new(s).poll_shutdown(cx),
            TransportStreamInner::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};

    use super::{MAX_PROXY_RESPONSE_HEADER_BYTES, ProxyAuthorization, http_connect_exchange};

    // NOTE: These unit tests purely cover regressions from security items flagged by bots.
    // TODO: Add more comprehensive unit tests for the module

    async fn exchange_response(response: Vec<u8>) -> std::io::Result<DuplexStream> {
        let (client, mut proxy) = tokio::io::duplex(response.len().max(1024));
        proxy.write_all(&response).await.unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            http_connect_exchange(client, "example.com", 443, &ProxyAuthorization::None),
        )
        .await
        .expect("proxy exchange should finish before the peer closes");
        drop(proxy);
        result
    }

    #[tokio::test]
    async fn rejects_oversized_proxy_status_line() {
        let mut response = b"HTTP/1.1 200 ".to_vec();
        response.resize(MAX_PROXY_RESPONSE_HEADER_BYTES + 1, b'x');

        let err = exchange_response(response).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn rejects_proxy_headers_over_cumulative_limit() {
        let mut response = b"HTTP/1.1 200 OK\r\n".to_vec();
        let header = b"X-Test: value\r\n";
        while response.len() <= MAX_PROXY_RESPONSE_HEADER_BYTES {
            response.extend_from_slice(header);
        }

        let err = exchange_response(response).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn formats_ipv6_proxy_authority() {
        let response = b"HTTP/1.1 200 OK\r\n\r\n";
        let expected_request = b"CONNECT [2001:db8::1]:443 HTTP/1.1\r\n\
                                 Host: [2001:db8::1]:443\r\n\r\n";
        let (client, mut proxy) = tokio::io::duplex(expected_request.len());
        proxy.write_all(response).await.unwrap();

        let stream = http_connect_exchange(client, "2001:db8::1", 443, &ProxyAuthorization::None)
            .await
            .unwrap();
        let mut request = vec![0; expected_request.len()];
        proxy.read_exact(&mut request).await.unwrap();

        assert_eq!(request, expected_request);
        drop(stream);
    }

    #[tokio::test]
    async fn rejects_non_2xx_or_malformed_status_codes() {
        for status in ["199", "300", "2000"] {
            let response = format!("HTTP/1.1 {status} Not Successful\r\n\r\n").into_bytes();
            let err = exchange_response(response).await.unwrap_err();

            assert_eq!(err.kind(), std::io::ErrorKind::Other);
        }
    }

    #[tokio::test]
    async fn accepts_any_2xx_status_code() {
        for status in [201, 250, 299] {
            let response = format!("HTTP/1.1 {status} Success\r\n\r\n").into_bytes();

            exchange_response(response).await.unwrap();
        }
    }
}
