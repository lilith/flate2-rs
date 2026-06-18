//! Cooperative cancellation for long-running compress/decompress operations.

/// A cooperative cancellation check, polled periodically while compressing or
/// decompressing.
///
/// Attach one to a [`Compress`](crate::Compress)/[`Decompress`](crate::Decompress) with
/// [`set_cancel`](crate::Decompress::set_cancel). The streaming `Read`/`Write`
/// adaptors poll it between internal steps and return an error of kind
/// [`io::ErrorKind::Interrupted`](std::io::ErrorKind::Interrupted) as soon as
/// it fires — which bounds the time spent on adversarial input (for
/// example a "zip bomb" whose every byte of output is cheap but whose input is
/// enormous).
///
/// Any `Fn() -> bool` that is `Send + Sync` implements `CancelCheck`, so the common
/// case is a closure over an `AtomicBool` or a deadline:
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use flate2::Decompress;
///
/// let cancel = Arc::new(AtomicBool::new(false));
/// let flag = Arc::clone(&cancel);
/// let mut decompress = Decompress::new(true);
/// decompress.set_cancel(move || flag.load(Ordering::Relaxed));
/// // ... hand `decompress` to a streaming adaptor; set `cancel` from another
/// // thread (or a signal handler, deadline, etc.) to abort decoding.
/// ```
///
/// This trait is intentionally minimal and dependency-free; its shape is
/// modelled on the `enough` crate's `Stop` trait.
pub trait CancelCheck: Send + Sync {
    /// Returns `true` to cancel the operation as soon as possible.
    ///
    /// Polled at coarse intervals (between internal backend calls), so it may
    /// be called many times during one operation — keep it cheap.
    fn is_cancelled(&self) -> bool;

    /// Returns `false` if this check can never fire, letting callers
    /// skip storing it. The default is `true`.
    #[inline]
    fn may_cancel(&self) -> bool {
        true
    }
}

/// A [`CancelCheck`] that never cancels: a zero-cost opt-out of cooperative cancellation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct NeverCancel;

impl CancelCheck for NeverCancel {
    #[inline(always)]
    fn is_cancelled(&self) -> bool {
        false
    }

    #[inline(always)]
    fn may_cancel(&self) -> bool {
        false
    }
}

impl<F: Fn() -> bool + Send + Sync> CancelCheck for F {
    #[inline]
    fn is_cancelled(&self) -> bool {
        self()
    }
}
