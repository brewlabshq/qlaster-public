use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::QlasterError;

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

pub async fn read_framed<R>(stream: &mut R) -> Result<Vec<u8>, QlasterError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let size = u32::from_le_bytes(len) as usize;
    if size > MAX_FRAME_SIZE {
        return Err(QlasterError::FrameTooLarge(size));
    }

    let mut payload = vec![0u8; size];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

pub async fn write_framed<W>(stream: &mut W, payload: &[u8]) -> Result<(), QlasterError>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let size: u32 = payload
        .len()
        .try_into()
        .map_err(|_| QlasterError::FrameTooLarge(payload.len()))?;
    stream.write_all(&size.to_le_bytes()).await?;
    stream.write_all(payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;
    use tokio::io::{duplex, AsyncWriteExt};

    #[tokio::test]
    async fn framed_roundtrip_supports_multiple_and_empty_payloads() {
        let (mut send, mut recv) = duplex(1024);

        write_framed(&mut send, b"hello")
            .await
            .expect("write first frame");
        write_framed(&mut send, &[])
            .await
            .expect("write second empty frame");

        let first = read_framed(&mut recv).await.expect("read first frame");
        let second = read_framed(&mut recv).await.expect("read second frame");

        assert_eq!(first, b"hello".to_vec());
        assert!(second.is_empty());
    }

    #[tokio::test]
    async fn read_framed_rejects_oversized_frame_header() {
        let (mut send, mut recv) = duplex(1024);

        let oversized = (MAX_FRAME_SIZE as u32) + 1;
        send.write_all(&oversized.to_le_bytes())
            .await
            .expect("write oversized length");

        let err = read_framed(&mut recv)
            .await
            .expect_err("oversized frame must fail");
        match err {
            QlasterError::FrameTooLarge(size) => assert_eq!(size, MAX_FRAME_SIZE + 1),
            other => panic!("expected FrameTooLarge, got {other}"),
        }
    }

    #[tokio::test]
    async fn read_framed_rejects_truncated_payload() {
        let (mut send, mut recv) = duplex(1024);

        send.write_all(&8u32.to_le_bytes())
            .await
            .expect("write declared frame length");
        send.write_all(&[1, 2, 3])
            .await
            .expect("write partial frame payload");
        drop(send);

        let err = read_framed(&mut recv)
            .await
            .expect_err("truncated payload must fail");
        match err {
            QlasterError::Io(io_err) => assert_eq!(io_err.kind(), ErrorKind::UnexpectedEof),
            other => panic!("expected io::ErrorKind::UnexpectedEof, got {other}"),
        }
    }
}
