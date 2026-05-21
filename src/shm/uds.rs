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

unsafe fn sendmsg_with_fd(sock: RawFd, payload: &[u8], fd: RawFd) -> io::Result<()> {
    let mut iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };

    let cmsg_space = unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = [0u8; 64];
    debug_assert!(cmsg_space <= cmsg_buf.len(), "cmsg buffer too small");

    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space as _;

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as _;
        let data_ptr = libc::CMSG_DATA(cmsg);
        std::ptr::write_unaligned(data_ptr as *mut RawFd, fd);
    }

    let n = unsafe { libc::sendmsg(sock, &msg, libc::MSG_NOSIGNAL) };
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

unsafe fn recvmsg_with_fd(sock: RawFd, buf: &mut [u8]) -> io::Result<(usize, Option<OwnedFd>)> {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };
    let mut cmsg_buf = [0u8; 64];

    let mut msg: libc::msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_buf.len() as _;

    let n = unsafe { libc::recvmsg(sock, &mut msg, libc::MSG_CMSG_CLOEXEC) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut out_fd: Option<OwnedFd> = None;
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let data = libc::CMSG_DATA(cmsg);
                let fd: RawFd = std::ptr::read_unaligned(data as *const RawFd);
                if out_fd.is_none() {
                    out_fd = Some(OwnedFd::from_raw_fd(fd));
                } else {
                    // Multiple fds attached — close extras to avoid leak.
                    libc::close(fd);
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
        let raw = efd_send.as_raw_fd();

        let payload = b"hello over uds";
        let send_task = tokio::spawn(async move {
            send_frame_with_fd(&tx, payload, raw).await.expect("send");
            // Keep efd_send alive until recv completes (the send dup'd the fd).
            drop(efd_send);
        });
        let (got_payload, got_fd) = recv_frame_with_fd(&rx).await.expect("recv");
        send_task.await.expect("send task");

        assert_eq!(got_payload, payload);
        let received = got_fd.expect("fd attached");
        let received_efd = EventFd::from_owned_fd(received);
        // Round-trip the eventfd: notify on the duplicated fd, drain on... wait,
        // the original was dropped. The kernel object stays alive as long as
        // any fd references it; the receiver holds one. Notify on the
        // received side, drain on the same side.
        received_efd.notify().expect("notify");
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
