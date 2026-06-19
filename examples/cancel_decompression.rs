//! Cancelling a flate2 decompression with no changes to flate2.
//!
//! flate2's streaming decoders read from any `Read` and propagate that reader's
//! errors. So you can cancel a decode — even an adversarial one — by wrapping
//! the input in a `Read` that fails once a shared flag is set. Flip the flag
//! from a signal handler, a timer, another thread, wherever.
//!
//! Signal with `ErrorKind::Other` (or another non-`Interrupted` kind), NOT
//! `Interrupted`: `read_to_end`/`read_to_string`/`io::copy` retry on
//! `Interrupted` and would loop forever.
//!
//! Run with: `cargo run --example cancel_decompression`

use flate2::read::DeflateDecoder;
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
    // A "bomb": ~1 MiB of empty stored blocks. Zero output, large input — an
    // output-size limit would never trip on it, but the work is paid reading
    // the input we control.
    let mut bomb = Vec::new();
    for _ in 0..200_000 {
        bomb.extend_from_slice(&[0x00, 0x00, 0x00, 0xFF, 0xFF]);
    }
    bomb.extend_from_slice(&[0x01, 0x00, 0x00, 0xFF, 0xFF]);

    let cancel = Arc::new(AtomicBool::new(false));
    // In real code another thread / a timer / a Ctrl-C handler flips this.
    cancel.store(true, Ordering::Relaxed);

    // Poll the flag at most once per 64 KiB read.
    let input = CancelReader::new(Cursor::new(bomb), 64 * 1024, {
        let cancel = Arc::clone(&cancel);
        move || cancel.load(Ordering::Relaxed)
    });
    let mut out = Vec::new();
    match DeflateDecoder::new(input).read_to_end(&mut out) {
        Err(e) if e.kind() == io::ErrorKind::Other => {
            println!("decompression cancelled — no flate2 changes needed");
        }
        other => panic!("expected a cancellation error, got {other:?}"),
    }
}
