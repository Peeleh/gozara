//! Blob transfer over libp2p streams, reading from and writing to files on disk.
//!
//! Wire format (integers are big-endian):
//!
//! ```text
//! push: frame(hash) frame(payload)  ->  tag [frame(reason)]
//! pull: frame(hash)                 ->  tag [frame(payload)]
//!
//! frame(x) = u32 length || x
//! tag      = 1 (stored / found) | 0 (rejected / not found)
//! ```
//!
//! A `0` tag answering a push is followed by a frame with the reason; answering a pull it ends the
//! exchange. Payloads are never held in memory: they move between the stream and a file through a
//! fixed-size buffer. Incoming payloads land in a temp file and only reach their final path once
//! they hash to the expected value.

use std::{
    path::{Path, PathBuf},
};
use blake3::{Hash, OUT_LEN};
use eyre::{eyre, Result};
use futures::{
    io::{copy_buf, BufReader},
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, StreamExt,
};
use libp2p::{PeerId, StreamProtocol};
use libp2p_stream::{Control, IncomingStreams};
use tempfile::{NamedTempFile, TempPath};
use tokio::{
    fs::File,
    sync::{mpsc, oneshot, OwnedSemaphorePermit},
    time::{Duration, timeout}
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::debug;

pub const PUSH_PROTOCOL: StreamProtocol = StreamProtocol::new("/gozara/blob-push/1.0.0/");
pub const PULL_PROTOCOL: StreamProtocol = StreamProtocol::new("/gozara/blob-pull/1.0.0/");

const TIMEOUT_WINDOW: u64 = 60;
const TAG_NO: u8 = 0;
const TAG_YES: u8 = 1;
/// rejection reason length: 256 bytes ~20 unicode words
const MAX_REASON_LEN: usize = 1<<8;
/// 64kb buffer
const IO_BUF_LEN: usize = 1<<16;

const CHANNEL_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Push,
    Pull,
}

#[derive(Debug, Clone)]
pub struct TransferEvent {
    pub peer: PeerId,
    pub hash: Hash,
    /// Bytes moved; `0` unless the transfer succeeded.
    pub len: usize,
    pub direction: Direction,
    /// `true` only if a payload was transferred and verified, so a pull the peer couldn't serve
    /// is `false`.
    pub ok: bool,
}

async fn write_frame<W: AsyncWrite + Unpin>(io: &mut W, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len())?;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(bytes).await?;
    Ok(())
}

/// Reads a frame's length prefix and checks it against `max_len`.
async fn read_len<R: AsyncRead + Unpin>(io: &mut R, max_len: usize) -> Result<usize> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_len {
        return Err(eyre!("frame of {len} bytes exceeds {max_len}-byte limit"));
    }
    Ok(len)
}

async fn read_frame<R: AsyncRead + Unpin>(io: &mut R, max_len: usize) -> Result<Vec<u8>> {
    let len = read_len(io, max_len).await?;
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_hash<R: AsyncRead + Unpin>(io: &mut R) -> Result<Hash> {
    let bytes = read_frame(io, OUT_LEN).await?;
    Hash::from_slice(&bytes)
        .map_err(|_| eyre!("expected a {OUT_LEN}-byte hash, got {} bytes", bytes.len()))
}

async fn read_tag<R: AsyncRead + Unpin>(io: &mut R) -> Result<bool> {
    let mut tag = [0u8; 1];
    io.read_exact(&mut tag).await?;
    match tag[0] {
        TAG_YES => Ok(true),
        TAG_NO => Ok(false),
        other => Err(eyre!("unexpected tag byte `{other}`")),
    }
}

async fn write_rejection<W: AsyncWrite + Unpin>(io: &mut W, reason: &str) -> Result<()> {
    io.write_all(&[TAG_NO]).await?;
    write_frame(io, reason.as_bytes()).await?;
    io.close().await?;
    Ok(())
}

async fn finish<W: AsyncWrite + Unpin>(
    io: &mut W,
    peer: PeerId,
    direction: Direction
) {
    if let Err(e) = io.close().await {
        debug!(%peer, error = %e, direction = ?direction, "Unclean close.");
    }
}

async fn open_for_send(path: &Path) -> Result<(File, u32)> {
    let file = File::open(path)
        .await
        .map_err(|e| eyre!("failed to open `{}`: {e}", path.display()))?;
    let size = file.metadata().await?.len();
    let len = u32::try_from(size).map_err(|_| {
        eyre!(
            "`{}` is {size} bytes, too large for one frame",
            path.display()
        )
    })?;
    Ok((file, len))
}

async fn send_file<W: AsyncWrite + Unpin>(
    io: &mut W,
    file: File,
    len: u32
) -> Result<()> {
    io.write_all(&len.to_be_bytes()).await?;
    let limit = u64::from(len);
    let reader = BufReader::with_capacity(IO_BUF_LEN, file.compat().take(limit));
    let sent = copy_buf(reader, io).await?;
    if sent != limit {
        return Err(eyre!("file shrank while sending: {sent} of {len} bytes"));
    }
    Ok(())
}

async fn push_inner(
    control: &mut Control,
    peer: PeerId,
    hash: Hash,
    path: &Path
) -> Result<usize> {
    let (file, len) = open_for_send(path).await?;
    let mut stream = control
        .open_stream(peer, PUSH_PROTOCOL)
        .await
        .map_err(|e| eyre!("failed to open stream to {peer}: {e}"))?;

    write_frame(&mut stream, hash.as_slice()).await?;
    send_file(&mut stream, file, len).await?;

    let result = if read_tag(&mut stream).await? {
        Ok(len as usize)
    } else {
        let reason = read_frame(&mut stream, MAX_REASON_LEN).await?;
        Err(eyre!("push rejected: {}", String::from_utf8_lossy(&reason)))
    };
    finish(&mut stream, peer, Direction::Push).await;
    result
}

pub async fn push(
    mut control: Control,
    peer: PeerId,
    hash: Hash,
    path: &Path,
    events_tx: mpsc::UnboundedSender<TransferEvent>,
) -> Result<()> {
    let result = push_inner(&mut control, peer, hash, path).await;
    let len = match &result {
        Ok(len) => *len,
        Err(_) => 0,
    };
    let _ = events_tx.send(TransferEvent {
        peer,
        hash,
        len,
        direction: Direction::Push,
        ok: result.is_ok(),
    });
    result.map(|_| ())
}

async fn receive_to_temp<R: AsyncRead + Unpin>(
    io: &mut R,
    len: usize,
    dir: &Path,
) -> Result<(Hash, TempPath)> {
    let (file, temp_path) = NamedTempFile::new_in(dir)?.into_parts();
    let mut file = File::from_std(file).compat_write();
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; IO_BUF_LEN];
    let mut remaining = len;
    while remaining > 0 {
        let want = buf.len().min(remaining);
        let n = io.read(&mut buf[..want]).await?;
        if n == 0 {
            return Err(eyre!("stream ended {remaining} bytes early"));
        }
        hasher.update(&buf[..n]);
        file.write_all(&buf[..n]).await?;
        remaining -= n;
    }
    file.flush().await?;
    Ok((hasher.finalize(), temp_path))
}

/// Moves a verified temp file into place, replacing whatever is at `dest`.
fn persist(temp_path: TempPath, dest: &Path) -> Result<()> {
    temp_path.persist(dest).map_err(|e| {
        eyre!(
            "failed to move payload to `{}`: {}",
            dest.display(),
            e.error
        )
    })
}

async fn pull_inner(
    control: &mut Control,
    peer: PeerId,
    hash: Hash,
    dest: &Path,
    max_payload_len: usize,
) -> Result<Option<usize>> {
    let Some(dir) = dest.parent() else {
        return Err(eyre!(format!("no parent for {}", dest.display())));
    };
    let mut stream = control
        .open_stream(peer, PULL_PROTOCOL)
        .await
        .map_err(|e| eyre!("failed to open stream to {peer}: {e}"))?;

    write_frame(&mut stream, hash.as_slice()).await?;
    let stored = if read_tag(&mut stream).await? {
        let len = read_len(&mut stream, max_payload_len).await?;
        let (actual, temp_path) = receive_to_temp(&mut stream, len, dir).await?;
        if actual != hash {
            return Err(eyre!("pull hash mismatch: expected {hash}, got {actual}"));
        }
        persist(temp_path, dest)?;
        Some(len)
    } else {
        None
    };
    finish(&mut stream, peer, Direction::Pull).await;
    Ok(stored)
}

pub async fn pull(
    mut control: Control,
    peer: PeerId,
    hash: Hash,
    dest: &Path,
    max_payload_len: usize,
    events_tx: mpsc::UnboundedSender<TransferEvent>,
) -> Result<Option<usize>> {
    let result = pull_inner(&mut control, peer, hash, dest, max_payload_len).await;
    let len = match &result {
        Ok(Some(len)) => *len,
        _ => 0,
    };
    let _ = events_tx.send(TransferEvent {
        peer,
        hash,
        len,
        direction: Direction::Pull,
        ok: matches!(result, Ok(Some(_))),
    });
    result
}

#[derive(Debug)]
#[allow(unused)]
pub struct IOPermit {
    pub path: PathBuf,
    pub permit: OwnedSemaphorePermit
}

#[derive(Debug)]
pub enum IncomingPush {
    Request {
        peer: PeerId,
        hash: Hash,
        len: usize,
        reply_tx: oneshot::Sender<Option<IOPermit>>
    },
    Finished {
        hash: Hash,
        succeeded: bool
    }
}


#[derive(Debug)]
pub struct IncomingPull {
    pub peer: PeerId,
    pub hash: Hash,
    reply_tx: oneshot::Sender<Option<IOPermit>>,
}

impl IncomingPull {
    pub fn reply(self, pull_permit: Option<IOPermit>) {
        let _ = self.reply_tx.send(pull_permit);
    }
}

async fn handle_incoming_push(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    peer: PeerId,
    max_payload_len: usize,
    tx: mpsc::Sender<IncomingPush>
) -> Result<()> {
    let expected = read_hash(&mut stream).await?;
    let len = read_len(&mut stream, max_payload_len).await?;
    let (reply_tx, rx) = oneshot::channel();
    if tx.send(IncomingPush::Request { peer, hash: expected, len, reply_tx }).await.is_err() {
        let _ = write_rejection(&mut stream, "receiver is not available").await;
        return Err(eyre!("failed to notify the pipeline about an incoming push"));
    }
    let Some(push_permit) = rx.await.ok().flatten() else {
        let _ = write_rejection(&mut stream, "push refused").await;
        return Err(eyre!("push refused"));
    };
    let Some(dir) = push_permit.path.parent() else {
        let reason = format!("interal error");
        let _ = write_rejection(&mut stream, &reason).await;
        let _ = tx.send(IncomingPush::Finished { hash: expected, succeeded: false }).await;
        return Err(eyre!("no parent for {}", push_permit.path.display()));
    };
    let (actual, temp_path) = receive_to_temp(&mut stream, len, dir).await?;
    if actual != expected {
        let reason = format!("integrity check failed (got {actual})");
        let _ = write_rejection(&mut stream, &reason).await;
        let _ = tx.send(IncomingPush::Finished { hash: expected, succeeded: false }).await;
        return Err(eyre!(
            "hash mismatch for the incoming push: expected {expected}, got {actual}"
        ));
    }

    persist(temp_path, &push_permit.path)?;

    stream.write_all(&[TAG_YES]).await?;
    finish(&mut stream, peer, Direction::Push).await;
    let _ = tx.send(IncomingPush::Finished { hash: actual, succeeded: true }).await;
    Ok(())
}

pub fn accept_pushes(
    mut incoming: IncomingStreams,
    max_payload_len: usize,
) -> mpsc::Receiver<IncomingPush> {
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming.next().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                match timeout(
                    Duration::from_secs(TIMEOUT_WINDOW),
                    handle_incoming_push(stream, peer, max_payload_len, tx)
                ).await {
                    Ok(Ok(_)) => (),
                    Ok(Err(e)) => {
                        debug!(from = %peer, error = %e, "Incoming push failed.");
                    }
                    Err(_) => {
                        debug!(from = %peer, "Incoming push failed: timeout.");
                    }
                }
            });
        }
    });
    rx
}

async fn handle_incoming_pull(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    peer: PeerId,
    tx: mpsc::Sender<IncomingPull>
) -> Result<()> {
    let hash = read_hash(&mut stream).await?;
    let (reply_tx, rx) = oneshot::channel();
    let _ = tx.send(IncomingPull { peer, hash, reply_tx }).await;
    // No consumer, or one that drops the request unanswered, counts as "don't have it".
    let Some(pull_permit) = rx.await.ok().flatten() else {
        stream.write_all(&[TAG_NO]).await?;
        finish(&mut stream, peer, Direction::Pull).await;
        return Ok(());
    };
    let (file, len) = open_for_send(&pull_permit.path).await?;
    stream.write_all(&[TAG_YES]).await?;
    send_file(&mut stream, file, len).await?;
    finish(&mut stream, peer, Direction::Pull).await;
    Ok(())
}

pub fn accept_pulls(
    mut incoming: IncomingStreams
) -> mpsc::Receiver<IncomingPull> {
    let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming.next().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                match timeout(
                    Duration::from_secs(TIMEOUT_WINDOW),
                    handle_incoming_pull(stream, peer, tx)
                ).await {
                    Ok(Ok(_)) => (),
                    Ok(Err(e)) => {
                        debug!(from = %peer, error = %e, "Incoming pull failed.");
                    }
                    Err(_) => {
                        debug!(from = %peer, "Incoming pull failed: timeout.");
                    }
                }
            });
        }
    });
    rx
}
