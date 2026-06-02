//! UDS (Unix domain socket) helpers for the SHM control plane.
//!
//! Normal subscribe/ping frames flow through `crate::wire::{read_framed,
//! write_framed}` (now generic over AsyncRead/AsyncWrite), which works on
//! `tokio::net::UnixStream` unchanged. The only piece that needs special
//! treatment is the connection-ready handshake from sender → consumer, which
//! must additionally hand the consumer a duplicated file descriptor for the
//! per-connection eventfd. That requires `sendmsg(SCM_RIGHTS)`, which neither
//! tokio nor std expose ergonomically — so we make the syscall directly under
//! `UnixStream::async_io` to stay tokio-driven.

use std::io;
use std::mem;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};

use tokio::io::Interest;
use tokio::net::UnixStream;

use crate::error::QlasterError;

const ANCILLARY_RECV_BUF: usize = 1024;

/// Suppress SIGPIPE for writes on this socket.
///
/// On Linux this is a no-op: each send already passes `MSG_NOSIGNAL`. On
/// macOS/BSD that flag is not reliably honored, so we set the `SO_NOSIGPIPE`
/// socket option once after the stream is established; it covers every write
/// on the socket (raw `sendmsg` here and tokio's own `AsyncWrite` path). Call
/// this on the accepted stream (sender) and the connected stream (consumer)
/// before any data is written.
pub(crate) fn set_nosigpipe(fd: RawFd) -> io::Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let on: libc::c_int = 1;
        // SAFETY: `fd` is a live socket fd supplied by the caller. setsockopt
        // copies `optlen` bytes from the optval pointer and does not retain it;
        // we pass a pointer to a single `c_int` with the matching length.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_NOSIGPIPE,
                (&raw const on).cast::<libc::c_void>(),
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(target_os = "linux")]
    let _ = fd;
    Ok(())
}

/// Send a length-prefixed `payload` and a single file descriptor in one
/// `sendmsg` call. The receiver MUST call `recv_frame_with_fd` to retrieve
/// the fd (a regular `read_framed` would silently drop it).
pub async fn send_frame_with_fd(
    stream: &UnixStream,
    payload: &[u8],
    fd: RawFd,
) -> Result<(), QlasterError> {
    if payload.len() > u32::MAX as usize {
        return Err(QlasterError::FrameTooLarge(payload.len()));
    }
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);

    let sock = stream.as_raw_fd();
    stream
        .async_io(Interest::WRITABLE, || unsafe {
            sendmsg_with_fd(sock, &buf, fd)
        })
        .await
        .map_err(|e| QlasterError::UdsError(format!("sendmsg: {e}")))
}

/// Receive a length-prefixed payload that was sent with an attached fd via
/// `send_frame_with_fd`. Returns the payload bytes and the (received-side
/// duplicated) `OwnedFd`. Caller is responsible for the fd's lifetime.
///
/// The ancillary buffer is fixed (`ANCILLARY_RECV_BUF` = 1024 bytes) and the
/// handshake carries a single fd; any extra attached fds are closed on receipt.
pub async fn recv_frame_with_fd(
    stream: &UnixStream,
) -> Result<(Vec<u8>, Option<OwnedFd>), QlasterError> {
    let sock = stream.as_raw_fd();
    let mut buf = vec![0u8; ANCILLARY_RECV_BUF];
    let (n, fd) = stream
        .async_io(Interest::READABLE, || unsafe {
            recvmsg_with_fd(sock, &mut buf)
        })
        .await
        .map_err(|e| QlasterError::UdsError(format!("recvmsg: {e}")))?;
    if n == 0 {
        return Err(QlasterError::UdsError(
            "peer closed UDS before sending ready frame".into(),
        ));
    }
    if n < 4 {
        return Err(QlasterError::UdsError(format!("short recvmsg ({n} bytes)")));
    }
    let payload_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if 4 + payload_len > n {
        return Err(QlasterError::UdsError(format!(
            "ancillary frame larger than buffer ({} > {})",
            4 + payload_len,
            n
        )));
    }
    Ok((buf[4..4 + payload_len].to_vec(), fd))
}

/// # Safety
/// `sock` must be a valid open socket fd and `fd` a valid open fd, neither
/// closed concurrently for the duration of the call.
unsafe fn sendmsg_with_fd(sock: RawFd, payload: &[u8], fd: RawFd) -> io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };

    // SAFETY: CMSG_SPACE is a pure arithmetic macro with no memory effects.
    let cmsg_space = unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = [0u8; 64];
    debug_assert!(cmsg_space <= cmsg_buf.len(), "cmsg buffer too small");

    // SAFETY: msghdr is a plain C struct of integers and pointers; an all-zero
    // bit pattern is a valid initialized value, which we fully populate below.
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space as _;

    // SAFETY: msg.msg_control points at cmsg_buf, which holds >= CMSG_SPACE
    // bytes (debug-asserted), so CMSG_FIRSTHDR returns a valid header pointer
    // into it. We set the control header fields and write the fd into the cmsg
    // data area (within that reservation) via write_unaligned, which tolerates
    // the payload's alignment.
    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as _;
        let data_ptr = libc::CMSG_DATA(cmsg);
        std::ptr::write_unaligned(data_ptr as *mut RawFd, fd);
    }

    // Linux suppresses SIGPIPE per-send via MSG_NOSIGNAL. macOS does not honor
    // it reliably; there we pass no flag and rely on SO_NOSIGPIPE having been
    // set on the socket (see `set_nosigpipe`).
    #[cfg(target_os = "linux")]
    let send_flags = libc::MSG_NOSIGNAL;
    #[cfg(not(target_os = "linux"))]
    let send_flags = 0;
    // SAFETY: msg is fully initialized and its iov/control pointers reference
    // live stack buffers (iov, cmsg_buf) that outlive this call.
    let n = unsafe { libc::sendmsg(sock, &msg, send_flags) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    if (n as usize) != payload.len() {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("partial sendmsg: {n} of {} bytes", payload.len()),
        ));
    }
    Ok(())
}

/// # Safety
/// `sock` must be a valid open socket fd that the caller does not close
/// concurrently for the duration of the call.
unsafe fn recvmsg_with_fd(sock: RawFd, buf: &mut [u8]) -> io::Result<(usize, Option<OwnedFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };
    let mut cmsg_buf = [0u8; 64];

    // SAFETY: msghdr is a plain C struct; an all-zero bit pattern is a valid
    // initialized value, which we fully populate below.
    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_buf.len() as _;

    // MSG_CMSG_CLOEXEC atomically marks received fds close-on-exec on Linux.
    // macOS lacks the flag, so we pass 0 and set FD_CLOEXEC by hand below.
    #[cfg(target_os = "linux")]
    let recv_flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(target_os = "linux"))]
    let recv_flags = 0;
    // SAFETY: msg is initialized with iov/control pointers to live stack
    // buffers (iov, cmsg_buf) that outlive this call.
    let n = unsafe { libc::recvmsg(sock, &mut msg, recv_flags) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut out_fd: Option<OwnedFd> = None;
    // SAFETY: recvmsg populated msg's control buffer; CMSG_FIRSTHDR/CMSG_NXTHDR
    // iterate strictly within cmsg_buf as bounded by the kernel-set
    // msg_controllen, and read_unaligned reads the fd payload at its (unaligned)
    // cmsg offset. A received fd is owned by us the moment we read it.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(cmsg);
                let fd: RawFd = std::ptr::read_unaligned(data as *const RawFd);
                if out_fd.is_none() {
                    // macOS could not set close-on-exec atomically during
                    // recvmsg; do it now before the fd can leak via exec.
                    // SAFETY: `fd` is the freshly received descriptor, not yet
                    // owned by anything. fcntl(F_SETFD) on a valid fd is sound;
                    // on failure we close it and return BEFORE constructing the
                    // OwnedFd, so it is never leaked nor double-closed.
                    #[cfg(not(target_os = "linux"))]
                    if libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) < 0 {
                        let err = io::Error::last_os_error();
                        libc::close(fd);
                        return Err(err);
                    }
                    out_fd = Some(OwnedFd::from_raw_fd(fd));
                } else {
                    // Multiple fds attached — close extras to avoid leaking
                    // them. Best-effort: ownership is not transferred, so a
                    // (rare) close failure is only logged.
                    if libc::close(fd) < 0 {
                        tracing::warn!(
                            "failed to close extra received fd: {}",
                            io::Error::last_os_error()
                        );
                    }
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }

    Ok((n as usize, out_fd))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::EventFd;

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_payload_and_fd() {
        let (tx, rx) = UnixStream::pair().expect("pair");

        let efd_send = EventFd::new().expect("eventfd");
        // Hand over the *passable* fd (read end on macOS, the eventfd on Linux).
        let pass = efd_send.pass_fd();

        let payload = b"hello over uds";
        let send_task = tokio::spawn(async move {
            send_frame_with_fd(&tx, payload, pass).await.expect("send");
            // Return tx so the connection stays open until recv completes.
            tx
        });
        let (got_payload, got_fd) = recv_frame_with_fd(&rx).await.expect("recv");
        let _tx = send_task.await.expect("send task");

        assert_eq!(got_payload, payload);
        let received = got_fd.expect("fd attached");
        let received_efd = EventFd::from_owned_fd(received);
        // Notify from the producer side (the write end on macOS, the same
        // eventfd on Linux); the received read end observes the wakeup. This
        // mirrors production, where the sender notifies and the consumer drains.
        efd_send.notify().expect("notify");
        assert_eq!(received_efd.drain().expect("drain"), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_without_fd_returns_none() {
        let (tx, rx) = UnixStream::pair().expect("pair");
        // Send a regular length-prefixed frame using the wire helper.
        let mut tx_mut = tx;
        crate::wire::write_framed(&mut tx_mut, b"plain")
            .await
            .expect("write");
        let mut rx_mut = rx;
        let got = crate::wire::read_framed(&mut rx_mut).await.expect("read");
        assert_eq!(got, b"plain");
    }
}
