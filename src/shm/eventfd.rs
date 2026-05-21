//! Linux `eventfd` wrapper. Used to wake a SHM ring consumer in another
//! process when the producer publishes a frame. The fd is created in the
//! sender and handed to the consumer over UDS via `SCM_RIGHTS`; both sides
//! end up holding distinct file descriptors backed by the same kernel object.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

use tokio::io::unix::AsyncFd;

/// A non-blocking eventfd. Producer-side calls `notify()`; consumer-side
/// either drains synchronously via `drain()` or wraps the fd in
/// `AsyncEventFd` for tokio-driven waits.
#[derive(Debug)]
pub struct EventFd {
    fd: OwnedFd,
}

impl EventFd {
    pub fn new() -> io::Result<Self> {
        let raw = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd: unsafe { OwnedFd::from_raw_fd(raw) },
        })
    }

    pub fn from_owned_fd(fd: OwnedFd) -> Self {
        Self { fd }
    }

    pub fn into_owned_fd(self) -> OwnedFd {
        self.fd
    }

    pub fn into_raw_fd(self) -> RawFd {
        self.fd.into_raw_fd()
    }

    /// Increment the kernel counter; wakes any consumer epoll-waiting for
    /// readability. Returns Ok on transient saturation (counter at the cap)
    /// since the consumer is guaranteed already-woken in that case.
    pub fn notify(&self) -> io::Result<()> {
        let value = 1u64.to_ne_bytes();
        let n = unsafe {
            libc::write(
                self.fd.as_raw_fd(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
            )
        };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }

    /// Read and reset the counter. Returns `Ok(0)` if no wakeup is pending.
    pub fn drain(&self) -> io::Result<u64> {
        let mut buf = [0u8; 8];
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
            return Err(err);
        }
        Ok(u64::from_ne_bytes(buf))
    }
}

impl AsRawFd for EventFd {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

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
        assert_eq!(efd.drain().expect("drain"), 3);
        assert_eq!(efd.drain().expect("drain again"), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_wait_resolves_after_notify() {
        let efd = EventFd::new().expect("eventfd");
        let raw = efd.as_raw_fd();
        let async_fd = AsyncEventFd::new(efd).expect("async wrap");

        // Schedule a notify after a short delay using a dup'd fd.
        let dup = unsafe { libc::dup(raw) };
        assert!(dup >= 0, "dup failed");
        let dup_efd = unsafe { EventFd::from_owned_fd(OwnedFd::from_raw_fd(dup)) };
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            dup_efd.notify().expect("notify");
        });

        let count = tokio::time::timeout(Duration::from_secs(1), async_fd.wait())
            .await
            .expect("wait timed out")
            .expect("wait err");
        assert!(count >= 1);
    }
}
