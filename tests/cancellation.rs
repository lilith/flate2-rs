//! Cooperative cancellation of streaming decode/encode via `set_cancel`.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::{Compression, Decompress, NeverCancel};

/// A run of `n` empty stored DEFLATE blocks followed by a final empty block.
/// Decodes to zero bytes but forces the decoder to chew through `5 * (n + 1)`
/// input bytes — the classic "tiny output, huge input" shape on which an
/// output-size check would never fire.
fn empty_stored_block_bomb(n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(5 * (n + 1));
    for _ in 0..n {
        v.extend_from_slice(&[0x00, 0x00, 0x00, 0xFF, 0xFF]); // non-final stored, len 0
    }
    v.extend_from_slice(&[0x01, 0x00, 0x00, 0xFF, 0xFF]); // final stored, len 0
    v
}

/// A cancel that returns `true` once it has been polled more than `after` times,
/// together with the shared poll counter so the test can inspect it.
fn stop_after(after: usize) -> (impl Fn() -> bool + Send + Sync, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&polls);
    (
        move || polls.fetch_add(1, Ordering::Relaxed) >= after,
        counter,
    )
}

fn raw_deflate(payload: &[u8]) -> Vec<u8> {
    let mut enc = DeflateEncoder::new(Vec::new(), Compression::default());
    enc.write_all(payload).unwrap();
    enc.finish().unwrap()
}

#[test]
fn zero_output_bomb_is_cancellable() {
    // The key property: even though the bomb produces no output (so an
    // output-side poll would never fire), an always-on cancel aborts the very
    // first `read`.
    let bomb = empty_stored_block_bomb(1_000_000);
    let mut decomp = Decompress::new(false);
    decomp.set_cancel(|| true);
    let mut dec = DeflateDecoder::new_with_decompress(&bomb[..], decomp);

    let mut buf = [0u8; 16 * 1024];
    let err = dec.read(&mut buf).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
}

#[test]
fn bomb_is_cancelled_partway() {
    // A cancel that fires after a few polls cancels the bomb mid-stream, long
    // before its millions of input bytes are consumed.
    let bomb = empty_stored_block_bomb(1_000_000); // ~5 MB input, 0 output
    let (cancel, polls) = stop_after(3);
    let mut decomp = Decompress::new(false);
    decomp.set_cancel(cancel);
    let mut dec = DeflateDecoder::new_with_decompress(&bomb[..], decomp);

    let mut buf = [0u8; 16 * 1024];
    let err = dec.read(&mut buf).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
    // It was polled repeatedly inside one `read` (the cancellation reaches the
    // internal refill loop), and fired on the 4th poll.
    assert_eq!(polls.load(Ordering::Relaxed), 4);
}

#[test]
fn never_firing_stop_decodes_fully() {
    let payload = b"the quick brown fox jumps over the lazy dog".repeat(1000);
    let compressed = raw_deflate(&payload);

    let mut decomp = Decompress::new(false);
    decomp.set_cancel(|| false); // never stops
    let mut dec = DeflateDecoder::new_with_decompress(&compressed[..], decomp);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).unwrap();
    assert_eq!(out, payload);
}

#[test]
fn unstoppable_clears_a_previous_stop() {
    let payload = b"hello cancellation".to_vec();
    let compressed = raw_deflate(&payload);

    let mut decomp = Decompress::new(false);
    decomp.set_cancel(|| true); // would cancel everything ...
    decomp.set_cancel(NeverCancel); // ... but NeverCancel clears it
    let mut dec = DeflateDecoder::new_with_decompress(&compressed[..], decomp);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).unwrap();
    assert_eq!(out, payload);
}

#[test]
fn with_cancel_builder_round_trips() {
    let payload = b"builder form".to_vec();
    let compressed = raw_deflate(&payload);

    let decomp = Decompress::new(false).with_cancel(|| false);
    let mut dec = DeflateDecoder::new_with_decompress(&compressed[..], decomp);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).unwrap();
    assert_eq!(out, payload);
}
