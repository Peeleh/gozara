// let (tx_incoming_transfer, mut rx_incoming_transfer) = mpsc::channel(128);

// blob_transfer::accept_pushes(
//         blob_transfer_control.accept(PUSH_PROTOCOL)?,
//         CHUNK_SIZE,
//         tx_incoming_transfer.clone(),
//         tx_transfer_outcome.clone()
//     );
//     blob_transfer::accept_pulls(
//         blob_transfer_control.accept(PULL_PROTOCOL)?,
//         tx_incoming_transfer,
//         tx_transfer_outcome.clone()
//     );

//         fn handle_incoming_push_request(
//         &mut self,
//         from: PeerId,
//         hash: Hash,
//         reply_tx: oneshot::Sender<Option<TransferPermit>>
//     ) {
//         debug!(hash = %hash.to_hex(), from = %from, "A new incoming push request.");
//         let Some(RemoteBlobContext {
//             blob: RemoteBlob::Gathering { chunks, .. },
//             ..
//         }) = self.remote_blob.as_mut()
//         else {
//             debug!("Remote blob is not gathering.");
//             return
//         };
//         let Some(RemoteChunk::Pulling { path, owners, inflight, failed }) =
//             chunks.get_mut(&hash)
//         else {
//             let _ = reply_tx.send(None);
//             debug!("Chunk is not pulling or is missing.");
//             return
//         };
//         let _ = owners.insert(from);
//         if inflight.is_some() {
//             // save the peer
//             let _ = reply_tx.send(None);
//             debug!("Another pull already underway.");
//             return
//         }
//         if let Some(attempts) = failed.get(&from) {
//             if *attempts >= MAX_RETRIES {
//                 debug!("Max pull retries already attempted for this peer.");
//                 let _ = reply_tx.send(None);
//                 return
//             }
//         }
//         let Ok(permit) = self
//             .download_permit
//             .clone()
//             .try_acquire_owned()
//         else {
//             debug!(hash = %hash.to_hex(), from = %from, "Could not obtain a pull permit.");
//             let _ = reply_tx.send(None);
//             return
//         };
//         *inflight = Some(InflightPull {
//             from,
//             created_at: Instant::now()
//         });
//         let _ = reply_tx.send(Some(
//             TransferPermit {
//                 path: path.clone(),
//                 permit
//             }
//         ));
//     }

//     fn handle_incoming_pull_request(
//         &mut self,
//         from: PeerId,
//         hash: Hash,
//         reply_tx: oneshot::Sender<Option<TransferPermit>>
//     ) {
//         debug!(hash = %hash.to_hex(), from = %from, "A new incoming pull request.");
//         let Some(LocalBlobContext {
//             blob: LocalBlob::Distributing { chunks, .. },
//             ..
//         }) = self.local_blob.as_mut()
//         else {
//             let _ = reply_tx.send(None);
//             debug!("Local blob is not distributing.");
//             return
//         };
//         let Some(LocalChunk::Assigned { path, inflight, failed, .. }) = chunks.get_mut(&hash) else {
//             let _ = reply_tx.send(None);
//             debug!("Chunk is not assigned or is missing.");
//             return
//         };
//         if inflight.is_some() {
//             let _ = reply_tx.send(None);
//             debug!("Another pull is already underway.");
//             return
//         }
//         if failed.get(&from).copied().unwrap_or_default() >= MAX_RETRIES {
//             debug!("Max push retries already attempted for this peer.");
//             let _ = reply_tx.send(None);
//             return
//         }
//         let Ok(permit) = self
//             .upload_permit
//             .clone()
//             .try_acquire_owned()
//         else {
//             debug!("Could not obtain a push permit.");
//             let _ = reply_tx.send(None);
//             return
//         };
//         *inflight = Some(InflightPush {
//             to: from,
//             created_at: Instant::now()
//         });
//         let _ = reply_tx.send(Some(
//             TransferPermit {
//                 path: path.clone(),
//                 permit
//             }
//         ));
//     }