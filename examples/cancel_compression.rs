//! Cancelling a flate2 compression with no changes to flate2.
//!
//! Same pattern as decompression: `io::copy` reads the uncompressed input from
//! a `CancelReader` into the encoder, so cancelling the read cancels the
//! compression. (Compression's work is bounded by the input you feed it, so if
//! you drive the encoder yourself with `write()` you can equally just check
//! your flag between writes — there's no adversarial case the way there is for
//! decompression.)
//!
//! As on the read side, signal with `ErrorKind::Other`, not `Interrupted`:
//! `io::copy` retries on `Interrupted`.
//!
//! Run with: `cargo run --example cancel_compression`

use flate2::write::ZlibEncoder;
use flate2::Compression;
use std::io::{self, Cursor, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A `Read` adapter that aborts by failing: it polls `should_cancel` at most
/// once per `interval` bytes, and once that returns `true`, every `read` fails
/// with `ErrorKind::Other`. The throttle keeps an expensive check off the hot
/// path; the sticky flag means it only has to trip once.
struct CancelReader<R, F> {
    inner: R,
    should_cancel: F,
    interval: usize,
    since_check: usize,
    cancelled: bool,
}

impl<R: Read, F: FnMut() -> bool> CancelReader<R, F> {
    fn new(inner: R, interval: usize, should_cancel: F) -> Self {
        CancelReader {
            inner,
            should_cancel,
            interval: interval.max(1),
            since_check: 0,
            cancelled: false,
        }
    }
}

impl<R: Read, F: FnMut() -> bool> Read for CancelReader<R, F> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.cancelled && self.since_check >= self.interval {
            self.since_check = 0;
            self.cancelled = (self.should_cancel)();
        }
        if self.cancelled {
            return Err(io::Error::new(io::ErrorKind::Other, "cancelled"));
        }
        let n = self.inner.read(buf)?;
        self.since_check += n;
        Ok(n)
    }
}

fn main() {
    let source = vec![0u8; 4 * 1024 * 1024]; // 4 MiB to compress

    let cancel = Arc::new(AtomicBool::new(false));
    // In real code another thread / a timer flips this.
    cancel.store(true, Ordering::Relaxed);

    let mut input = CancelReader::new(Cursor::new(source), 64 * 1024, {
        let cancel = Arc::clone(&cancel);
        move || cancel.load(Ordering::Relaxed)
    });
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    match io::copy(&mut input, &mut encoder) {
        Err(e) if e.kind() == io::ErrorKind::Other => {
            println!("compression cancelled — no flate2 changes needed");
        }
        Ok(n) => panic!("expected cancellation, compressed {n} bytes"),
        Err(e) => panic!("unexpected error: {e}"),
    }
}
