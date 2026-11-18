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
    time::{Instant, Duration, timeout}
};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::debug;

pub const PUSH_PROTOCOL: StreamProtocol = StreamProtocol::new("/gozara/blob-push/1.0.0/");
pub const PULL_PROTOCOL: StreamProtocol = StreamProtocol::new("/gozara/blob-pull/1.0.0/");

// 60 seconds
pub const TIMEOUT_WINDOW: u64 = 60;

const TAG_NO: u8 = 0;
const TAG_YES: u8 = 1;

/// rejection reason length: 256 bytes ~20 unicode words
const MAX_REASON_LEN: usize = 1<<8;

/// 64kb buffer
const IO_BUF_LEN: usize = 1<<16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Push,
    Pull,
}

#[derive(Debug)]
pub struct TransferPermit {
    pub path: PathBuf,
    pub permit: OwnedSemaphorePermit
}

#[derive(Debug, Clone)]
pub struct Outcome {
    pub peer: PeerId,
    pub hash: Hash,
    pub direction: Direction,
    pub succeeded: bool
}

#[derive(Debug, Clone)]
pub enum TransferOutcome {
    Incoming(Outcome),
    Outgoing(Outcome)
}

async fn write_frame<W: AsyncWrite + Unpin>(io: &mut W, bytes: &[u8]) -> Result<()> {
    let len = u32::try_from(bytes.len())?;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(bytes).await?;
    Ok(())
}

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
) -> Result<()> {
    let (file, len) = open_for_send(path).await?;
    let mut stream = control
        .open_stream(peer, PUSH_PROTOCOL)
        .await
        .map_err(|e| eyre!("failed to open stream to {peer}: {e}"))?;

    write_frame(&mut stream, hash.as_slice()).await?;
    send_file(&mut stream, file, len).await?;

    let result = if read_tag(&mut stream).await? {
        Ok(())
    } else {
        let reason = read_frame(&mut stream, MAX_REASON_LEN).await?;
        Err(eyre!("push rejected: {}", String::from_utf8_lossy(&reason)))
    };
    finish(&mut stream, peer, Direction::Push).await;
    result
}

pub fn push(
    mut control: Control,
    peer: PeerId,
    hash: Hash,
    path: PathBuf,
    permit: OwnedSemaphorePermit,
    tx_outcome: mpsc::Sender<TransferOutcome>,
) -> Result<()> {
    let onset = Instant::now();
    tokio::spawn(async move {
        let result = timeout(
            Duration::from_secs(TIMEOUT_WINDOW),
            push_inner(&mut control, peer, hash, &path)
        ).await;
        drop(permit);
        match result {
            Ok(Ok(_)) => {
                let _ = tx_outcome.send(TransferOutcome::Outgoing(Outcome {
                    peer,
                    hash,
                    direction: Direction::Push,
                    succeeded: true
                })).await;
                debug!(hash = %hash.to_hex(), to = %peer,
                    duration = Instant::now().duration_since(onset).as_secs(),
                    "Push finished with success."
                );
            }
            Ok(Err(e)) => {
                let _ = tx_outcome.send(TransferOutcome::Outgoing(Outcome {
                    peer,
                    hash,
                    direction: Direction::Push,
                    succeeded: false
                })).await;
                debug!(hash = %hash.to_hex(), to = %peer, error = %e,
                    duration = Instant::now().duration_since(onset).as_secs(),
                    "Push failed.");
            }
            Err(_) => {
                let _ = tx_outcome.send(TransferOutcome::Outgoing(Outcome {
                    peer,
                    hash,
                    direction: Direction::Push,
                    succeeded: false
                })).await;
                debug!(chunk = %hash.to_hex(), to = %peer, "Push failed: timeout.");
            }
        }
    });
    Ok(())
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
) -> Result<()> {
    let Some(dir) = dest.parent() else {
        return Err(eyre!(format!("no parent for {}", dest.display())));
    };
    let mut stream = control
        .open_stream(peer, PULL_PROTOCOL)
        .await
        .map_err(|e| eyre!("failed to open stream to {peer}: {e}"))?;

    write_frame(&mut stream, hash.as_slice()).await?;
    if read_tag(&mut stream).await? {
        let len = read_len(&mut stream, max_payload_len).await?;
        let (actual, temp_path) = receive_to_temp(&mut stream, len, dir).await?;
        if actual != hash {
            return Err(eyre!("pull hash mismatch: expected {hash}, got {actual}"));
        }
        persist(temp_path, dest)?;
    } else {
        return Err(eyre!("pull failed: read tag is 0"));
    }
    finish(&mut stream, peer, Direction::Pull).await;
    Ok(())
}

pub fn pull(
    mut control: Control,
    peer: PeerId,
    hash: Hash,
    dest: PathBuf,
    max_payload_len: usize,
    permit: OwnedSemaphorePermit,
    tx_outcome: mpsc::Sender<TransferOutcome>
) -> Result<()> {
    let onset = Instant::now();
    tokio::spawn(async move {
        let result = timeout(
            Duration::from_secs(TIMEOUT_WINDOW),
            pull_inner(&mut control, peer, hash, &dest, max_payload_len)
        ).await;
        drop(permit);
        match result {
            Ok(Ok(_)) => {
                let _ = tx_outcome.send(TransferOutcome::Outgoing(Outcome {
                    peer,
                    hash,
                    direction: Direction::Pull,
                    succeeded: true
                })).await;
                debug!(from = %peer, hash = %hash.to_hex(),
                    duration = Instant::now().duration_since(onset).as_secs(),
                    "Pull finished with success."
                );
            }
            Ok(Err(e)) => {
                let _ = tx_outcome.send(TransferOutcome::Outgoing(Outcome {
                    peer,
                    hash,
                    direction: Direction::Pull,
                    succeeded: false
                })).await;
                debug!(from = %peer, hash = %hash.to_hex(), error = %e,
                    duration = Instant::now().duration_since(onset).as_secs(),
                    "Pull failed."
                );
            }
            Err(_) => {
                let _ = tx_outcome.send(TransferOutcome::Outgoing(Outcome {
                    peer,
                    hash,
                    direction: Direction::Pull,
                    succeeded: false
                })).await;
                debug!(chunk = %hash.to_hex(), from = %peer, "Pull failed: timeout.");
            }
        }
    });
    Ok(())
}

#[derive(Debug)]
pub struct IncomingTransfer {
    pub from: PeerId,
    pub hash: Hash,
    pub direction: Direction,
    pub reply_tx: oneshot::Sender<Option<TransferPermit>>
}

async fn handle_incoming_push(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    peer: PeerId,
    max_payload_len: usize,
    tx: mpsc::Sender<IncomingTransfer>
) -> Result<Option<Hash>, Hash> {
    let expected = match read_hash(&mut stream).await {
        Ok(h) => h,
        Err(e) => {
            debug!(peer = %peer, error = %e, "Incoming push failed: read_hash.");
            return Ok(None)
        }
    };
    let len = match read_len(&mut stream, max_payload_len).await {
        Ok(l) => l,
        Err(e) => {
            debug!(peer = %peer, error = %e, "Incoming push failed: read_len.");
            return Ok(None)
        }
    };
    let (reply_tx, rx) = oneshot::channel();
    if tx.send(IncomingTransfer {
        from: peer,
        hash: expected,
        direction: Direction::Push,
        reply_tx
    }).await.is_err() {
        let _ = write_rejection(&mut stream, "receiver is not available").await;
        return Ok(None)
    }
    let Some(push_permit) = rx.await.ok().flatten() else {
        let _ = write_rejection(&mut stream, "push refused").await;
        return Ok(None)
    };
    let Some(dir) = push_permit.path.parent() else {
        let reason = "internal error";
        let _ = write_rejection(&mut stream, reason).await;
        debug!(peer = %peer, chunk = %expected.to_hex(), path = %push_permit.path.display(),
            "Incoming push failed: no parent directory."
        );
        return Err(expected)
    };
    let Ok((actual, temp_path)) = receive_to_temp(&mut stream, len, dir).await else {
        return Err(expected)
    };
    if actual != expected {
        let reason = format!("integrity check failed (got {actual})");
        let _ = write_rejection(&mut stream, &reason).await;
        debug!(expected = %expected.to_hex(), given = %actual.to_hex(),
            "Incoming push failed: hash mismatch."
        );
        return Err(expected)
    }
    if let Err(e) = persist(temp_path, &push_permit.path){
        debug!(peer = %peer, error = %e, "Incoming push failed: persist.");
        return Err(expected)
    };
    if let Err(e) = stream.write_all(&[TAG_YES]).await {
        debug!(peer = %peer, error = %e, "Incoming push failed: write tag(yes).");
        return Err(expected)
    }
    finish(&mut stream, peer, Direction::Push).await;
    Ok(Some(expected))
}

pub fn accept_pushes(
    mut incoming: IncomingStreams,
    max_payload_len: usize,
    tx_incoming_transfer: mpsc::Sender<IncomingTransfer>,
    tx_outcome: mpsc::Sender<TransferOutcome>
) {
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming.next().await {
            let tx_incoming = tx_incoming_transfer.clone();
            let tx_outcome = tx_outcome.clone();
            tokio::spawn(async move {
                match timeout(
                    Duration::from_secs(TIMEOUT_WINDOW),
                    handle_incoming_push(stream, peer, max_payload_len, tx_incoming)
                ).await {
                    Ok(Ok(maybe_hash)) => {
                        if let Some(hash) = maybe_hash {
                            let _ = tx_outcome.send(TransferOutcome::Incoming(Outcome {
                                peer,
                                hash,
                                direction: Direction::Push,
                                succeeded: true
                            })).await;
                        }
                    },
                    Ok(Err(hash)) => {
                        let _ = tx_outcome.send(TransferOutcome::Incoming(Outcome {
                            peer,
                            hash,
                            direction: Direction::Push,
                            succeeded: false
                        })).await;
                    }
                    Err(_) => {
                        debug!(from = %peer, "Incoming push failed: timeout.");
                    }
                }
            });
        }
    });
}

async fn handle_incoming_pull(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    peer: PeerId,
    tx: mpsc::Sender<IncomingTransfer>
) -> Result<Option<Hash>, Hash> {
    let hash = match read_hash(&mut stream).await {
        Ok(h) => h,
        Err(e) => {
            debug!(from = %peer, error = %e, "Incoming pull failed: read_hash.");
            return Ok(None)
        }
    };
    let (reply_tx, rx) = oneshot::channel();
    let _ = tx.send(IncomingTransfer {
        from: peer,
        direction: Direction::Pull,
        hash,
        reply_tx
    }).await;
    // No consumer, or one that drops the request unanswered, counts as "don't have it".
    let Some(pull_permit) = rx.await.ok().flatten() else {
        if stream.write_all(&[TAG_NO]).await.is_err() {
            return Ok(None)
        }
        finish(&mut stream, peer, Direction::Pull).await;
        return Ok(None)
    };
    let (file, len) = match open_for_send(&pull_permit.path).await {
        Ok((f, l)) => (f, l),
        Err(e) => {
            debug!(from = %peer, error = %e, "Incoming pull failed: open_for_send.");
            return Err(hash)
        }
    };
    if let Err(e) = stream.write_all(&[TAG_YES]).await {
        debug!(from = %peer, error = %e, "Incoming pull failed: write tag(yes).");
        return Err(hash)
    }
    if send_file(&mut stream, file, len).await.is_err() {
        return Err(hash)
    }
    finish(&mut stream, peer, Direction::Pull).await;
    Ok(Some(hash))
}

pub fn accept_pulls(
    mut incoming: IncomingStreams,
    tx_incoming_transfer: mpsc::Sender<IncomingTransfer>,
    tx_outcome: mpsc::Sender<TransferOutcome>
) {
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming.next().await {
            let tx_incoming = tx_incoming_transfer.clone();
            let tx_outcome = tx_outcome.clone();
            tokio::spawn(async move {
                match timeout(
                    Duration::from_secs(TIMEOUT_WINDOW),
                    handle_incoming_pull(stream, peer, tx_incoming)
                ).await {
                    Ok(Ok(maybe_hash)) => {
                        if let Some(hash) = maybe_hash {
                            let _ = tx_outcome.send(TransferOutcome::Incoming(Outcome {
                                peer,
                                hash,
                                direction: Direction::Pull,
                                succeeded: true
                            })).await;
                        }
                    },
                    Ok(Err(hash)) => {
                        let _ = tx_outcome.send(TransferOutcome::Incoming(Outcome {
                            peer,
                            hash,
                            direction: Direction::Pull,
                            succeeded: false
                        })).await;
                    }
                    Err(_) => {
                        debug!(from = %peer, "Incoming pull failed: timeout.");
                    }
                }
            });
        }
    });
}
