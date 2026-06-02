//! Cross-process wakeup primitive used to wake a SHM ring consumer in another
//! process when the producer publishes a frame.
//!
//! Two backends, selected at compile time:
//!
//! - **Linux**: a native `eventfd(2)`. A single fd is created on the sender,
//!   handed to the consumer over UDS via `SCM_RIGHTS`, and both sides end up
//!   holding distinct file descriptors backed by the same kernel counter.
//!   `notify()` writes an 8-byte counter increment; `drain()` reads and resets
//!   it, returning the accumulated count.
//!
//! - **macOS / other unixes**: a self-pipe. `eventfd(2)` does not exist, so the
//!   sender creates a `pipe(2)` and keeps the WRITE end for `notify()`, while
//!   the READ end is the fd handed to the consumer over `SCM_RIGHTS` (see
//!   [`EventFd::pass_fd`]). `notify()` writes one byte (coalescing on a full
//!   pipe, exactly like the eventfd counter saturating); `drain()` reads every
//!   pending byte and returns the count. Exact-count fidelity is not required —
//!   consumers only rely on "count > 0 means a wakeup is pending" and on no
//!   wakeup ever being lost.
//!
//! Both backends expose the same API, so the call sites in `transport`,
//! `sender`, and `consumer` are platform-agnostic. The one platform-aware
//! detail is the fd to pass to a peer: use [`EventFd::pass_fd`] (the read end on
//! macOS, the single eventfd on Linux) rather than [`AsRawFd::as_raw_fd`].

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use tokio::io::unix::AsyncFd;

// ---------------------------------------------------------------------------
// Linux backend: native eventfd
// ---------------------------------------------------------------------------

/// A non-blocking eventfd. Producer-side calls `notify()`; consumer-side
/// either drains synchronously via `drain()` or wraps the fd in
/// `AsyncEventFd` for tokio-driven waits.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct EventFd {
    fd: OwnedFd,
}

#[cfg(target_os = "linux")]
impl EventFd {
    pub fn new() -> io::Result<Self> {
        // SAFETY: eventfd(2) takes a plain initial value and flags and has no
        // memory effects; it returns a fresh fd or < 0 on error.
        let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            // SAFETY: `raw` is a fresh, valid, owned eventfd (checked >= 0
            // above) that nothing else holds, so transferring ownership to
            // OwnedFd is sound.
            fd: unsafe { OwnedFd::from_raw_fd(raw) },
        })
    }

    pub fn from_owned_fd(fd: OwnedFd) -> Self {
        Self { fd }
    }

    /// The fd to hand a peer over `SCM_RIGHTS`. On Linux this is the single
    /// eventfd (both `notify` and the consumer's wait use the same object).
    #[must_use = "the returned RawFd is unmanaged and only valid while this \
                  EventFd is alive; pass it over SCM_RIGHTS immediately or dup() it"]
    pub fn pass_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Increment the kernel counter; wakes any consumer epoll-waiting for
    /// readability. Returns Ok on transient saturation (counter at the cap)
    /// since the consumer is guaranteed already-woken in that case.
    pub fn notify(&self) -> io::Result<()> {
        let value = 1u64.to_ne_bytes();
        loop {
            // SAFETY: `self.fd` is a live owned eventfd; `value` is an 8-byte
            // stack buffer and we pass its real length, so the kernel reads
            // only valid initialized bytes.
            let n = unsafe {
                libc::write(
                    self.fd.as_raw_fd(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                // Counter saturated: the consumer is already woken.
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(());
                }
                // Interrupted by a signal before any write: retry.
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(err);
            }
            return Ok(());
        }
    }

    /// Read and reset the counter. Returns `Ok(0)` if no wakeup is pending.
    pub fn drain(&self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
        loop {
            // SAFETY: `self.fd` is a live owned eventfd; `buf` is an 8-byte
            // stack buffer we pass with its real length, so the kernel writes
            // only within bounds.
            let n = unsafe {
                libc::read(
                    self.fd.as_raw_fd(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    return Ok(0);
                }
                // Interrupted by a signal before any read: retry.
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(err);
            }
            return Ok(u64::from_ne_bytes(buf));
        }
    }
}

#[cfg(target_os = "linux")]
impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// Non-Linux backend (macOS/BSD): self-pipe
// ---------------------------------------------------------------------------

/// A non-blocking self-pipe standing in for an eventfd. The producer holds the
/// write end (`notify()`); the read end is handed to the consumer over
/// `SCM_RIGHTS` and drained there. A handle built via [`EventFd::from_owned_fd`]
/// wraps a received read end and has no write end, so `notify()` is unsupported
/// on it (the consumer never notifies).
#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub struct EventFd {
    read: OwnedFd,
    write: Option<OwnedFd>,
}

/// pipe(2) on macOS/BSD has no atomic CLOEXEC/NONBLOCK flags, so set both by
/// hand. Non-blocking is required for the `drain()`/`AsyncFd` readiness model.
#[cfg(not(target_os = "linux"))]
fn configure_pipe_fd(fd: RawFd) -> io::Result<()> {
    // SAFETY: `fd` is a live descriptor owned by the caller for the duration of
    // this call. F_SETFD/F_GETFL/F_SETFL with standard flag constants are
    // always sound on a valid fd and have no memory effects.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above — read back the current status flags.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: as above; `flags` was just returned by F_GETFL on this same fd.
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
impl EventFd {
    pub fn new() -> io::Result<Self> {
        let mut fds: [RawFd; 2] = [0; 2];
        // SAFETY: `fds` is a valid 2-element array; pipe(2) writes the read and
        // write fds into it on success and returns < 0 (leaving `fds` unused) on
        // error.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: pipe(2) succeeded, so fds[0]/fds[1] are fresh, distinct, valid
        // descriptors that nothing else owns. Wrap BOTH immediately, before any
        // fallible call: if either `configure_pipe_fd(..)?` below returns early,
        // both OwnedFds are dropped (closed) by Drop — no fd is leaked.
        let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        // SAFETY: as above; fds[1] is the write end.
        let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        configure_pipe_fd(read.as_raw_fd())?;
        configure_pipe_fd(write.as_raw_fd())?;
        Ok(Self {
            read,
            write: Some(write),
        })
    }

    /// Wrap a received read end (over `SCM_RIGHTS`). It shares the producer's
    /// pipe; this handle only drains and cannot `notify`.
    pub fn from_owned_fd(fd: OwnedFd) -> Self {
        // The producer already set O_NONBLOCK on the underlying open file
        // description, which is shared across SCM_RIGHTS, so this is
        // belt-and-suspenders. O_NONBLOCK is required for the AsyncFd readiness
        // model; a failure here is surprising but non-fatal (and changing the
        // signature to return Result would ripple to the infallible Linux
        // backend), so log rather than propagate.
        if let Err(err) = configure_pipe_fd(fd.as_raw_fd()) {
            tracing::warn!("eventfd: failed to set non-blocking on received fd: {err}");
        }
        Self {
            read: fd,
            write: None,
        }
    }

    /// The fd to hand a peer over `SCM_RIGHTS`: the pipe READ end. The producer
    /// keeps the write end for `notify()`.
    #[must_use = "the returned RawFd is unmanaged and only valid while this \
                  EventFd is alive; pass it over SCM_RIGHTS immediately or dup() it"]
    pub fn pass_fd(&self) -> RawFd {
        self.read.as_raw_fd()
    }

    /// Write one wakeup byte to the pipe. A full pipe (`EAGAIN`) means the
    /// consumer is behind but already woken, so we treat it as success —
    /// mirroring the Linux eventfd counter-saturation behavior.
    pub fn notify(&self) -> io::Result<()> {
        let Some(write) = self.write.as_ref() else {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "notify on a read-only (consumer-side) eventfd",
            ));
        };
        let byte = [1u8];
        loop {
            // SAFETY: `write` is a live owned pipe write end; `byte` is a 1-byte
            // stack buffer and we pass its real length.
            let n = unsafe {
                libc::write(
                    write.as_raw_fd(),
                    byte.as_ptr() as *const libc::c_void,
                    byte.len(),
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                // Pipe full: the consumer is behind but already woken.
                if err.raw_os_error() == Some(libc::EAGAIN) {
                    return Ok(());
                }
                // Interrupted by a signal before any write: retry.
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(err);
            }
            return Ok(());
        }
    }

    /// Drain every pending wakeup byte and return the count. Each byte is one
    /// (coalesced) notify; consumers rely only on `> 0`. Returns `Ok(0)` if no
    /// wakeup is pending.
    pub fn drain(&self) -> io::Result<u64> {
        let mut total = 0u64;
        let mut buf = [0u8; 64];
        loop {
            // SAFETY: `self.read` is a live owned pipe read end; `buf` is a
            // 64-byte stack buffer passed with its real length, so the kernel
            // writes only within bounds.
            let n = unsafe {
                libc::read(
                    self.read.as_raw_fd(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    break;
                }
                // Interrupted by a signal before any read: retry.
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(err);
            }
            if n == 0 {
                break; // write end fully closed
            }
            total += n as u64;
            if (n as usize) < buf.len() {
                break;
            }
        }
        Ok(total)
    }
}

#[cfg(not(target_os = "linux"))]
impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        // The read end is what AsyncFd must poll: the producer signals by
        // writing to the (separate) write end, which makes this end readable.
        // Producers must use `pass_fd()` (also the read end) to hand an fd to a
        // peer, never this for notify routing.
        self.read.as_raw_fd()
    }
}

// ---------------------------------------------------------------------------
// Shared async wrapper (both backends)
// ---------------------------------------------------------------------------

/// Tokio-aware wrapper. `wait()` resolves once the producer has called
/// `notify()` at least once since the last drain.
pub struct AsyncEventFd {
    inner: AsyncFd<EventFd>,
}

impl AsyncEventFd {
    pub fn new(efd: EventFd) -> io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(efd)?,
        })
    }

    /// Block until the eventfd is signaled, returning the accumulated
    /// notify count since the last call.
    pub async fn wait(&self) -> io::Result<u64> {
        loop {
            let mut guard = self.inner.readable().await?;
            match self.inner.get_ref().drain() {
                Ok(0) => {
                    guard.clear_ready();
                    continue;
                }
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    guard.clear_ready();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn notify_then_drain_returns_count() {
        let efd = EventFd::new().expect("eventfd");
        assert_eq!(efd.drain().expect("empty drain"), 0);
        efd.notify().expect("notify");
        efd.notify().expect("notify");
        efd.notify().expect("notify");
        // Linux: eventfd counter == 3. macOS: three coalesced bytes == 3.
        assert_eq!(efd.drain().expect("drain"), 3);
        assert_eq!(efd.drain().expect("drain again"), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_wait_resolves_after_notify() {
        let efd = EventFd::new().expect("eventfd");

        // Independent handle to the wakeup (read) end, simulating the consumer
        // that received the fd over SCM_RIGHTS. dup() the passable end so the
        // producer keeps its own; on macOS this is the pipe read end, on Linux
        // a second fd to the same eventfd.
        // SAFETY: `pass_fd()` returns a live fd borrowed from `efd`; dup(2)
        // creates an independent fd to the same object, valid once >= 0.
        let dup = unsafe { libc::dup(efd.pass_fd()) };
        assert!(dup >= 0, "dup failed");
        // SAFETY: `dup` is a fresh, valid, owned fd (checked >= 0 above).
        let consumer = unsafe { EventFd::from_owned_fd(OwnedFd::from_raw_fd(dup)) };
        let async_fd = AsyncEventFd::new(consumer).expect("async wrap");

        // Notify from the producer side after a short delay.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            efd.notify().expect("notify");
        });

        let count = tokio::time::timeout(Duration::from_secs(1), async_fd.wait())
            .await
            .expect("wait timed out")
            .expect("wait err");
        assert!(count >= 1);
    }

    // The consumer-side handle (built from a received read end) has no write
    // end, so notify() must refuse rather than silently no-op. Linux's eventfd
    // is bidirectional, so this contract is macOS/BSD-specific.
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn notify_on_consumer_side_handle_is_unsupported() {
        let efd = EventFd::new().expect("eventfd");
        // SAFETY: `pass_fd()` is a live fd; dup(2) yields an independent fd.
        let dup = unsafe { libc::dup(efd.pass_fd()) };
        assert!(dup >= 0, "dup failed");
        // SAFETY: `dup` is a fresh, valid, owned read-end fd (checked >= 0).
        let consumer = unsafe { EventFd::from_owned_fd(OwnedFd::from_raw_fd(dup)) };
        let err = consumer
            .notify()
            .expect_err("notify must fail on a read-only handle");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }
}
