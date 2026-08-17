use std::{
    io,
    sync::OnceLock
};
use futures::{
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt
};
use async_trait::async_trait;
use libp2p::{
    StreamProtocol,
    request_response::Codec
};

use indicatif::{
    MultiProgress,
    ProgressBar,
    ProgressStyle
};

static PROGRESS_MANAGER: OnceLock<MultiProgress> = OnceLock::new();

pub fn new_progress_bar(size: u64) -> ProgressBar {
    let pb = PROGRESS_MANAGER
        .get_or_init(MultiProgress::new)
        .add(ProgressBar::new(size));   
    
    pb.set_style(ProgressStyle::default_bar()
        .template("{msg}\n{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta}")
        .unwrap()
        .progress_chars("#>-")
    );    
    pb.set_prefix(String::from("Pulling"));
    pb
}

#[derive(Debug, Clone)]
pub struct BlobCodec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request(
    // param: blob hash
    pub String
); 

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response(
    // param: blob data
    pub Vec<u8>
); 

#[async_trait]
impl Codec for BlobCodec {
    type Protocol = StreamProtocol;
    type Request = Request;
    type Response = Response;

    async fn read_request<T>(
        &mut self, _: &Self::Protocol,
        io: &mut T
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        // Keep it simple: Read request size (u64) then string bytes
        // For this demo, we assume a small fixed buffer for simplicity
        let mut buf = vec![0u8; 1024];
        let n = io.read(&mut buf).await?;

        Ok(
            Request(
                String::from_utf8_lossy(&buf[..n]).to_string()
            )
        )
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        // 1: read the length header
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let total_size = u32::from_be_bytes(len_buf);                
        
        let mut buffer = Vec::with_capacity(total_size as usize); 
        
        // setup the progress bar
        let pb = new_progress_bar(total_size as u64);
        
        // A temporary buffer for "chunks" off the wire
        let mut fragment = [0u8; 1<<16]; // 64KB buffer
        loop {
            // read directly from the Yamux stream
            let n = io.read(&mut fragment).await?;            
            if n == 0 {
                break; // EOF - Stream finished
            }            
            buffer.extend_from_slice(&fragment[..n]);
            pb.inc(n as u64);
        }
        pb.finish_and_clear();
        
        Ok(Response(buffer))
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // 1: send blob's length(4 bytes)
        let len = res.0.len() as u32;
        io.write_all(&len.to_be_bytes()).await?;

        // 2: write the entire buffer
        // Yamux handles breaking this into frames internally
        io.write_all(&res.0).await?;
        io.close().await?; // Close is essential to trigger EOF on receiver
        Ok(())
    }

    // write_request implementation omitted for brevity (similar to write_response)
    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request
    ) -> io::Result<()>
    where T: AsyncWrite + Unpin + Send {
        io.write_all(req.0.as_bytes()).await?;
        io.close().await?;
        Ok(())
    }
}