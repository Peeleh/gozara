// Claude generated code
use eyre::{eyre, Result};
use tracing::{info, warn};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, StreamExt};
use libp2p::{PeerId, StreamProtocol};
use libp2p_stream::{Control, IncomingStreams};
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;

pub const PUSH_PROTOCOL: StreamProtocol = StreamProtocol::new("/gozara/blob-push/1.0.0");
pub const PULL_PROTOCOL: StreamProtocol = StreamProtocol::new("/gozara/blob-pull/1.0.0");

/// Generous ceiling on the hash frame 
const HASH_MAX_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Push,
    Pull,
}

#[derive(Debug, Clone)]
pub struct TransferEvent {
    pub peer: PeerId,
    pub hash: String,
    pub len: usize,
    pub direction: Direction,
    pub ok: bool,
}

async fn write_frame<W: AsyncWrite + Unpin>(
    io: &mut W,
    bytes: &[u8]
) -> Result<()> {
    let len: u32 = bytes
        .len()
        .try_into()?;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(bytes).await?;
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(
    io: &mut R,
    max_len: usize
) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_len {
        return Err(eyre!(format!("frame of {len} bytes exceeds {max_len}-byte limit")))
    }
    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

fn hash_from_bytes(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes)
        .map_err(|_| eyre!("hash was not valid utf-8"))
}

pub async fn push(
    mut control: Control,
    peer: PeerId,
    hash: String,
    data: Bytes,
    events_tx: mpsc::UnboundedSender<TransferEvent>,
) -> Result<()> {
    let mut stream = control
        .open_stream(peer, PUSH_PROTOCOL)
        .await
        .map_err(|e| eyre!(format!("Open stream error: {}", e.to_string())))?;

    write_frame(&mut stream, hash.as_bytes()).await?;
    write_frame(&mut stream, data.as_ref()).await?;

    let mut ack_tag = [0u8; 1];
    stream.read_exact(&mut ack_tag).await?;
    let result = if ack_tag[0] == 1 {
        Ok(())
    } else {
        // reason is no more than 4096 bytes
        let reason = read_frame(&mut stream, 4096).await?;
        Err(eyre!(String::from_utf8_lossy(&reason).into_owned()))
    };
    stream.close().await?;

    let len = data.len();
    let _ = events_tx.send(TransferEvent {
        peer,
        hash,
        len,
        direction: Direction::Push,
        ok: result.is_ok(),
    });
    result
}

/// Ask `peer` for the bytes behind `hash`. `Ok(None)` means they don't
/// have it — a normal outcome, not an error.
pub async fn pull(
    mut control: Control,
    peer: PeerId,
    hash: String,
    max_payload_len: usize,
    events_tx: mpsc::UnboundedSender<TransferEvent>,
) -> Result<Option<Vec<u8>>> {
    let mut stream = control
        .open_stream(peer, PULL_PROTOCOL)
        .await
        .map_err(|e| eyre!(format!("Open stream error: {}", e.to_string())))?;

    write_frame(&mut stream, hash.as_bytes()).await?;

    let mut found_tag = [0u8; 1];
    stream.read_exact(&mut found_tag).await?;
    let result = if found_tag[0] == 0 {
        Ok(None)
    } else {
        let data = read_frame(&mut stream, max_payload_len).await?;
        let received_hash = blake3::hash(&data).to_hex().to_string();
        if  received_hash == hash {
            Ok(Some(data))
        } else {
            Err(eyre!(format!(
                "Pull hash mismatch, expected `{}` but got `{}`", hash, received_hash
            )))
        }
    };
    stream.close().await?;

    let len = match &result {
        Ok(Some(data)) => data.len(),
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
pub struct IncomingPush {
    pub peer: PeerId,
    pub hash: String,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct IncomingPull {
    pub peer: PeerId,
    pub hash: String,
    reply_tx: oneshot::Sender<Option<Vec<u8>>>,
}

impl IncomingPull {
    pub fn respond(self, data: Option<Vec<u8>>) {
        let _ = self.reply_tx.send(data);
    }
}

pub fn accept_pushes(
    mut incoming: IncomingStreams,
    max_payload_len: usize,
) -> mpsc::UnboundedReceiver<IncomingPush> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming.next().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_incoming_push(stream, peer, max_payload_len, tx).await {
                    warn!(%peer, error = %e, "Incoming push failed");
                }
            });
        }
    });
    rx
}

async fn handle_incoming_push(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    peer: PeerId,
    max_payload_len: usize,
    tx: mpsc::UnboundedSender<IncomingPush>,
) -> Result<()> {
    let expected_hash = hash_from_bytes(read_frame(&mut stream, HASH_MAX_LEN).await?)?;
    let data = read_frame(&mut stream, max_payload_len).await?;

    let actual_hash = blake3::hash(&data).to_hex().to_string();
    if actual_hash != expected_hash {
        stream.write_all(&[0u8]).await?;
        write_frame(
            &mut stream,
            format!("integrity check failed (got {actual_hash})").as_bytes(),
        )
        .await?;
        stream.close().await?;
        return Err(eyre!(format!(
            "Hash mismatch for incoming push, expected `{}` got `{}`",
            expected_hash,
            actual_hash
        )))
    }

    stream.write_all(&[1u8]).await?;
    stream.close().await?;
    let _ = tx.send(IncomingPush {
        peer,
        hash: actual_hash,
        data
    });
    Ok(())
}

pub fn accept_pulls(
    mut incoming: IncomingStreams
) -> mpsc::UnboundedReceiver<IncomingPull> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming.next().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_incoming_get(stream, peer, tx).await {
                    warn!(%peer, error = %e, "blob pull failed");
                }
            });
        }
    });
    rx
}

async fn handle_incoming_get(
    mut stream: impl AsyncRead + AsyncWrite + Unpin,
    peer: PeerId,
    tx: mpsc::UnboundedSender<IncomingPull>,
) -> Result<()> {
    let hash = hash_from_bytes(read_frame(&mut stream, HASH_MAX_LEN).await?)?;
    let (reply_tx, reply_rx) = oneshot::channel();
    let _ = tx.send(IncomingPull {
        peer,
        hash: hash.clone(),
        reply_tx,
    });
    // If the caller drops the IncomingGet without responding, treat it
    // the same as "don't have it" rather than hanging the requester.
    let data = reply_rx.await.unwrap_or(None);

    match data {
        Some(bytes) => {
            stream.write_all(&[1u8]).await?;
            write_frame(&mut stream, &bytes).await?;
        }
        None => stream.write_all(&[0u8]).await?,
    }
    stream.close().await?;
    Ok(())
}
