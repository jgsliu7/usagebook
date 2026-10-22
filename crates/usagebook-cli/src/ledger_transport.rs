//! Small HTTP/1 loopback transport: two callers own all blocking I/O. Each
//! connection carries one request and closes; no detached per-client threads.
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown, TcpStream},
    time::{Duration, Instant},
};
use tiny_http::{Header, Method, Response};
use usagebook_core::ledger::query_types::MAX_BODY;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(5);
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HEAD: usize = 32 * 1024;
// Bound chunk framing too: even an unterminated chunk-size line cannot allocate
// unbounded memory in the decoder. This is separate from the decoded body limit.
const MAX_WIRE_BODY: u64 = (MAX_BODY * 2 + MAX_HEAD) as u64;

pub(super) struct Connection {
    stream: TcpStream,
    deadline: Instant,
}
impl Connection {
    pub(super) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            deadline: Instant::now() + RECEIVE_TIMEOUT,
        }
    }
    pub(super) fn reject(mut self) {
        // The accept loop must never block while declining an overfull queue.
        if self.stream.set_nonblocking(true).is_err() {
            return;
        }
        let body = r#"{"error":{"code":"BUSY","message":"BUSY","retryable":true}}"#;
        let _ = write!(self.stream, "HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}

struct DeadlineStream {
    stream: TcpStream,
    deadline: Instant,
}
impl DeadlineStream {
    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "network deadline"))
    }
    fn timed_out(&self, error: io::Error, direction: Shutdown) -> io::Error {
        if matches!(
            error.kind(),
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
        ) {
            let _ = self.stream.shutdown(direction);
            io::Error::new(io::ErrorKind::TimedOut, "network deadline")
        } else {
            error
        }
    }
}
impl Read for DeadlineStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.set_read_timeout(Some(self.remaining()?))?;
        self.stream
            .read(buf)
            .map_err(|e| self.timed_out(e, Shutdown::Read))
    }
}
impl Write for DeadlineStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        self.stream
            .write(buf)
            .map_err(|e| self.timed_out(e, Shutdown::Write))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum Body {
    Fixed(io::Take<BufReader<DeadlineStream>>),
    Chunked(chunked_transfer::Decoder<io::Take<BufReader<DeadlineStream>>>),
}

pub(super) struct Request {
    method: Method,
    path: String,
    headers: Vec<Header>,
    body_length: Option<usize>,
    body: Body,
    writer: DeadlineStream,
    expect_continue: bool,
}
impl Request {
    pub(super) fn receive(connection: Connection) -> Option<Self> {
        let reply = connection.stream.try_clone().ok()?;
        match Self::read_head(connection) {
            Ok(request) => Some(request),
            Err(error) => {
                let (code, message) = match error.kind() {
                    io::ErrorKind::TimedOut => (408, "REQUEST_TIMEOUT"),
                    io::ErrorKind::OutOfMemory => (431, "HEADERS_TOO_LARGE"),
                    _ => (400, "INVALID_REQUEST"),
                };
                let mut writer = DeadlineStream {
                    stream: reply,
                    deadline: Instant::now() + SEND_TIMEOUT,
                };
                let body = format!("{{\"error\":{{\"code\":\"{message}\",\"message\":\"{message}\",\"retryable\":false}}}}");
                let _ = write!(writer, "HTTP/1.1 {code} Error\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                let _ = writer.stream.shutdown(Shutdown::Both);
                None
            }
        }
    }
    fn read_head(connection: Connection) -> io::Result<Self> {
        let writer = DeadlineStream {
            stream: connection.stream.try_clone()?,
            deadline: connection.deadline,
        };
        let mut reader = BufReader::new(DeadlineStream {
            stream: connection.stream,
            deadline: connection.deadline,
        });
        let mut bytes = Vec::new();
        loop {
            // read_until itself can allocate without bound: take caps each read
            // and the aggregate before any HTTP parsing happens.
            let read = reader
                .by_ref()
                .take((MAX_HEAD + 1 - bytes.len()) as u64)
                .read_until(b'\n', &mut bytes)?;
            if bytes.len() > MAX_HEAD {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "headers too large",
                ));
            }
            if read == 0 {
                return Err(invalid());
            }
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        let mut slots = [httparse::EMPTY_HEADER; 64];
        let mut parsed = httparse::Request::new(&mut slots);
        if !parsed.parse(&bytes).map_err(|_| invalid())?.is_complete() {
            return Err(invalid());
        }
        let path = parsed.path.ok_or_else(invalid)?.to_owned();
        if !path.starts_with('/') {
            return Err(invalid());
        }
        let method = parsed
            .method
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?;
        let headers = parsed
            .headers
            .iter()
            .map(|h| Header::from_bytes(h.name, h.value).map_err(|_| invalid()))
            .collect::<io::Result<Vec<_>>>()?;
        let values = |name: &'static str| {
            headers
                .iter()
                .filter(move |h| h.field.as_str().as_str().eq_ignore_ascii_case(name))
                .map(|h| h.value.as_str())
        };
        // Reject ambiguous framing rather than accepting a second interpretation.
        if values("Host").count() > 1
            || values("Content-Length").count() > 1
            || values("Transfer-Encoding").count() > 1
            || values("Expect").count() > 1
        {
            return Err(invalid());
        }
        let content_length = values("Content-Length")
            .next()
            .map(|s| {
                if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(invalid());
                }
                s.parse::<usize>().map_err(|_| invalid())
            })
            .transpose()?;
        let chunked = match values("Transfer-Encoding").next() {
            None => false,
            Some(value) if value.eq_ignore_ascii_case("chunked") && content_length.is_none() => {
                true
            }
            Some(_) => return Err(invalid()),
        };
        let expect_continue = match values("Expect").next() {
            None => false,
            Some(value) if value.eq_ignore_ascii_case("100-continue") => true,
            Some(_) => return Err(invalid()),
        };
        let body = if chunked {
            Body::Chunked(chunked_transfer::Decoder::new(reader.take(MAX_WIRE_BODY)))
        } else {
            Body::Fixed(reader.take(content_length.unwrap_or(0) as u64))
        };
        Ok(Self {
            method,
            path,
            headers,
            body_length: content_length,
            body,
            writer,
            expect_continue,
        })
    }
    pub(super) fn method(&self) -> &Method {
        &self.method
    }
    pub(super) fn url(&self) -> &str {
        &self.path
    }
    pub(super) fn headers(&self) -> &[Header] {
        &self.headers
    }
    pub(super) fn body_length(&self) -> Option<usize> {
        self.body_length
    }
    pub(super) fn read_body(&mut self) -> io::Result<Vec<u8>> {
        if self.expect_continue {
            // Still covered by the receive deadline, including 100-continue.
            self.writer.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
            self.expect_continue = false;
        }
        let mut bytes = Vec::new();
        let result = match &mut self.body {
            Body::Fixed(reader) => reader
                .by_ref()
                .take((MAX_BODY + 1) as u64)
                .read_to_end(&mut bytes),
            Body::Chunked(reader) => {
                let result = reader
                    .by_ref()
                    .take((MAX_BODY + 1) as u64)
                    .read_to_end(&mut bytes);
                if result.is_ok()
                    && bytes.len() <= MAX_BODY
                    && reader.remaining_chunks_size().is_some_and(|n| n > 0)
                {
                    return Err(invalid());
                }
                result
            }
        };
        if let Err(error) = result {
            return Err(if Instant::now() >= self.writer.deadline {
                io::Error::new(io::ErrorKind::TimedOut, "network deadline")
            } else {
                error
            });
        }
        if self.body_length.is_some_and(|n| bytes.len() < n) {
            return Err(invalid());
        }
        Ok(bytes)
    }
    pub(super) fn respond<R: Read>(mut self, response: Response<R>) -> io::Result<()> {
        self.writer.deadline = Instant::now() + SEND_TIMEOUT;
        let length = response.data_length().ok_or_else(invalid)?;
        let status = response.status_code();
        write!(
            self.writer,
            "HTTP/1.1 {} {}\r\nConnection: close\r\nContent-Length: {length}\r\n",
            status.0,
            status.default_reason_phrase()
        )?;
        for header in response.headers() {
            write!(self.writer, "{header}\r\n")?;
        }
        self.writer.write_all(b"\r\n")?;
        if self.method != Method::Head {
            io::copy(&mut response.into_reader(), &mut self.writer)?;
        }
        Ok(())
    }
}
impl Drop for Request {
    fn drop(&mut self) {
        // No body draining or deferred work: releasing the worker always closes
        // the socket, including timeout, validation failure and early responses.
        let _ = self.writer.stream.shutdown(Shutdown::Both);
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP request")
}
