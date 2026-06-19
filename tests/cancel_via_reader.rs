//! A flate2-only cancellation, with no changes to flate2: wrap the input
//! `Read` so it fails once you decide to stop, and the decoder propagates that
//! error out of its next `read`.
//!
//! Two things matter:
//!   * Signal with `ErrorKind::Other` (or any non-`Interrupted` kind), NOT
//!     `Interrupted` — `Read::read_to_end`/`read_to_string`/`io::copy` silently
//!     retry on `Interrupted`, so a cancel signalled that way loops forever.
//!   * It works even on a decompression "bomb" of empty stored blocks (huge
//!     input, zero output) that an output-size limit would never catch, because
//!     the bomb's cost is paid reading input — and the input is what you wrapped.

use flate2::read::DeflateDecoder;
use std::io::{self, Cursor, Read};

/// A `Read` adapter that turns a cancellation signal into a clean abort: it
/// polls `should_cancel` at most once per `interval` bytes, and once that
/// returns `true`, every `read` fails with `ErrorKind::Other`. Any flate2
/// decoder/encoder reading through it propagates that error, so wrapping your
/// input cancels the operation with no flate2 changes — and the throttle keeps
/// an expensive check (a clock, a syscall) off the hot path.
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

/// `blocks` empty non-final stored blocks followed by a final empty block:
/// large input, zero output.
fn empty_block_bomb(blocks: usize) -> Vec<u8> {
    let mut bomb = Vec::with_capacity(blocks * 5 + 5);
    for _ in 0..blocks {
        bomb.extend_from_slice(&[0x00, 0x00, 0x00, 0xFF, 0xFF]);
    }
    bomb.extend_from_slice(&[0x01, 0x00, 0x00, 0xFF, 0xFF]);
    bomb
}

#[test]
fn bomb_is_cancelled_and_the_callback_is_throttled() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let bomb = empty_block_bomb(100_000); // ~500 KiB of input, 0 bytes of output
    let interval = 64 * 1024;
    let checks = Arc::new(AtomicUsize::new(0));

    let input = CancelReader::new(Cursor::new(&bomb), interval, {
        let checks = Arc::clone(&checks);
        move || {
            checks.fetch_add(1, Ordering::Relaxed);
            true // "cancel requested"
        }
    });
    let mut out = Vec::new();
    let err = DeflateDecoder::new(input)
        .read_to_end(&mut out)
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::Other);
    assert!(out.is_empty());
    // The interval throttles polling: the callback is consulted once (at the
    // first 64 KiB boundary), trips, and the sticky cancel handles the rest —
    // rather than being called on every one of the bomb's refills.
    assert_eq!(checks.load(Ordering::Relaxed), 1);
}

#[test]
fn the_same_bomb_otherwise_runs_to_completion() {
    // For contrast: without the wrapper the bomb is read in full (producing
    // nothing) — that wasted work is exactly what the wrapper lets you cut off.
    let bomb = empty_block_bomb(100_000);
    let mut out = Vec::new();
    DeflateDecoder::new(Cursor::new(&bomb))
        .read_to_end(&mut out)
        .unwrap();
    assert!(out.is_empty());
}
